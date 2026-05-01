use std::collections::{HashMap, HashSet};

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{
  Api,
  runtime::{
    WatchStreamExt,
    watcher::{self, Event},
  },
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

pub const RUN_ID_LABEL: &str = "the-conn.com/run-id";
pub const NODE_NAME_ANNOTATION: &str = "the-conn.com/node-name";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InfraFailureReason {
  ImagePullFailed { image: String, message: String },
  OOMKilled,
  InitContainerFailed { name: String, message: String },
  ContainerCreateError(String),
  PodStartTimeout,
  PodDeletedUnexpectedly,
  NodeLost,
}

impl InfraFailureReason {
  pub fn stable_code(&self) -> &'static str {
    match self {
      Self::ImagePullFailed { .. } => "ImagePullFailed",
      Self::OOMKilled => "OOMKilled",
      Self::InitContainerFailed { .. } => "InitContainerFailed",
      Self::ContainerCreateError(_) => "ContainerCreateError",
      Self::PodStartTimeout => "PodStartTimeout",
      Self::PodDeletedUnexpectedly => "PodDeletedUnexpectedly",
      Self::NodeLost => "NodeLost",
    }
  }

  pub fn full_message(&self) -> String {
    match self {
      Self::ImagePullFailed { image, message } => {
        format!("Image '{image}' could not be pulled: {message}")
      }
      Self::OOMKilled => {
        "Container was killed for exceeding its memory limit (OOMKilled)".to_string()
      }
      Self::InitContainerFailed { name, message } => {
        format!("Init container '{name}' failed: {message}")
      }
      Self::ContainerCreateError(msg) => format!("Container could not be created: {msg}"),
      Self::PodStartTimeout => {
        "Pod did not enter the Running phase within the configured startup timeout".to_string()
      }
      Self::PodDeletedUnexpectedly => "Pod was deleted while running".to_string(),
      Self::NodeLost => "Cluster node was lost while the pod was running".to_string(),
    }
  }

  pub fn user_message(&self) -> Option<String> {
    match self {
      Self::ImagePullFailed { image, .. } => Some(format!(
        "Failed to pull image '{image}'. Verify the image name, tag, and registry permissions."
      )),
      Self::OOMKilled | Self::ContainerCreateError(_) | Self::PodStartTimeout => {
        Self::user_message_from_code(self.stable_code())
      }
      Self::InitContainerFailed { .. } | Self::PodDeletedUnexpectedly | Self::NodeLost => None,
    }
  }

  pub fn user_message_from_code(code: &str) -> Option<String> {
    match code {
      "ImagePullFailed" => Some(
        "Failed to pull the container image. Verify the image name, tag, and registry permissions."
          .to_string(),
      ),
      "OOMKilled" => Some(
        "The container ran out of memory. Increase its memory limit or reduce its memory usage."
          .to_string(),
      ),
      "ContainerCreateError" => Some(
        "The container could not be created. Check the pod configuration for invalid mounts, environment variables, or security settings."
          .to_string(),
      ),
      "PodStartTimeout" => Some(
        "The pod did not start in time. The cluster may be out of capacity, or the image may be very large."
          .to_string(),
      ),
      _ => None,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSignal {
  PodRunning {
    node_name: String,
    pod_uid: String,
  },
  InfraFailure {
    node_name: String,
    pod_uid: String,
    reason: InfraFailureReason,
  },
}

impl PodSignal {
  pub fn node_name(&self) -> &str {
    match self {
      Self::PodRunning { node_name, .. } | Self::InfraFailure { node_name, .. } => node_name,
    }
  }

  pub fn pod_uid(&self) -> &str {
    match self {
      Self::PodRunning { pod_uid, .. } | Self::InfraFailure { pod_uid, .. } => pod_uid,
    }
  }
}

#[derive(Debug)]
pub enum WatcherCommand {
  ExpectDeletion { node_name: String },
  Shutdown,
}

pub fn classify_pod(pod: &Pod) -> Option<PodSignal> {
  let annotations = pod.metadata.annotations.as_ref()?;
  let node_name = annotations.get(NODE_NAME_ANNOTATION)?.clone();
  let pod_uid = pod.metadata.uid.clone()?;
  let status = pod.status.as_ref()?;

  if let Some(reason) = classify_init_container_statuses(status) {
    return Some(PodSignal::InfraFailure {
      node_name,
      pod_uid,
      reason,
    });
  }

  classify_main_container_statuses(status, &node_name, &pod_uid)
}

fn classify_init_container_statuses(
  status: &k8s_openapi::api::core::v1::PodStatus,
) -> Option<InfraFailureReason> {
  let init_statuses = status.init_container_statuses.as_ref()?;
  for cs in init_statuses {
    let state = cs.state.as_ref()?;
    if let Some(waiting) = state.waiting.as_ref() {
      let reason = waiting.reason.as_deref().unwrap_or("");
      if matches!(reason, "ImagePullBackOff" | "ErrImagePull") {
        let message = waiting.message.as_deref().unwrap_or("").to_string();
        return Some(InfraFailureReason::InitContainerFailed {
          name: cs.name.clone(),
          message: format!("{reason}: {message}"),
        });
      }
    }
    if let Some(terminated) = state.terminated.as_ref()
      && terminated.exit_code != 0
    {
      let message = terminated.message.as_deref().unwrap_or("").to_string();
      let exit = terminated.exit_code;
      return Some(InfraFailureReason::InitContainerFailed {
        name: cs.name.clone(),
        message: format!("exit code {exit}: {message}"),
      });
    }
  }
  None
}

fn classify_main_container_statuses(
  status: &k8s_openapi::api::core::v1::PodStatus,
  node_name: &str,
  pod_uid: &str,
) -> Option<PodSignal> {
  let container_statuses = status.container_statuses.as_ref()?;
  for cs in container_statuses {
    let state = cs.state.as_ref()?;
    if let Some(waiting) = state.waiting.as_ref() {
      let reason = waiting.reason.as_deref().unwrap_or("");
      let message = waiting.message.as_deref().unwrap_or("").to_string();
      if matches!(reason, "ImagePullBackOff" | "ErrImagePull") {
        return Some(PodSignal::InfraFailure {
          node_name: node_name.to_string(),
          pod_uid: pod_uid.to_string(),
          reason: InfraFailureReason::ImagePullFailed {
            image: cs.image.clone(),
            message,
          },
        });
      }
      if matches!(
        reason,
        "CreateContainerConfigError" | "CreateContainerError"
      ) {
        return Some(PodSignal::InfraFailure {
          node_name: node_name.to_string(),
          pod_uid: pod_uid.to_string(),
          reason: InfraFailureReason::ContainerCreateError(format!("{reason}: {message}")),
        });
      }
    }
    if let Some(terminated) = state.terminated.as_ref()
      && terminated.reason.as_deref() == Some("OOMKilled")
    {
      return Some(PodSignal::InfraFailure {
        node_name: node_name.to_string(),
        pod_uid: pod_uid.to_string(),
        reason: InfraFailureReason::OOMKilled,
      });
    }
    if state.running.is_some() {
      return Some(PodSignal::PodRunning {
        node_name: node_name.to_string(),
        pod_uid: pod_uid.to_string(),
      });
    }
  }
  None
}

#[derive(Default)]
struct WatcherState {
  reported_failure_uids: HashMap<String, HashSet<String>>,
  reported_running_uids: HashMap<String, HashSet<String>>,
  expected_deletions: HashSet<String>,
}

impl WatcherState {
  fn should_emit(&mut self, signal: &PodSignal) -> bool {
    match signal {
      PodSignal::PodRunning { node_name, pod_uid } => self
        .reported_running_uids
        .entry(node_name.clone())
        .or_default()
        .insert(pod_uid.clone()),
      PodSignal::InfraFailure {
        node_name, pod_uid, ..
      } => self
        .reported_failure_uids
        .entry(node_name.clone())
        .or_default()
        .insert(pod_uid.clone()),
    }
  }

  fn was_running(&self, node_name: &str) -> bool {
    self
      .reported_running_uids
      .get(node_name)
      .is_some_and(|s| !s.is_empty())
  }

  fn expect_deletion(&mut self, node_name: String) {
    self.expected_deletions.insert(node_name);
  }

  fn deletion_was_expected(&self, node_name: &str) -> bool {
    self.expected_deletions.contains(node_name)
  }
}

pub struct PodWatcher {
  client: kube::Client,
  namespaces: Vec<String>,
  run_id: String,
  signal_tx: mpsc::Sender<PodSignal>,
  cmd_rx: mpsc::Receiver<WatcherCommand>,
}

impl PodWatcher {
  pub fn new(
    client: kube::Client,
    namespaces: Vec<String>,
    run_id: String,
    signal_tx: mpsc::Sender<PodSignal>,
    cmd_rx: mpsc::Receiver<WatcherCommand>,
  ) -> Self {
    Self {
      client,
      namespaces,
      run_id,
      signal_tx,
      cmd_rx,
    }
  }

  pub async fn run(mut self) {
    let label_selector = format!("{RUN_ID_LABEL}={}", self.run_id);
    let mut streams = Vec::new();
    for ns in &self.namespaces {
      let api: Api<Pod> = Api::namespaced(self.client.clone(), ns);
      let cfg = watcher::Config::default().labels(&label_selector);
      let stream = watcher::watcher(api, cfg).default_backoff().boxed();
      streams.push(stream);
    }

    let mut combined = futures_util::stream::select_all(streams);
    let mut state = WatcherState::default();

    info!(
      run_id = %self.run_id,
      namespaces = ?self.namespaces,
      "PodWatcher started"
    );

    loop {
      tokio::select! {
        biased;
        cmd = self.cmd_rx.recv() => {
          match cmd {
            Some(WatcherCommand::ExpectDeletion { node_name }) => {
              debug!(run_id = %self.run_id, node_name, "Marking node deletion as expected");
              state.expect_deletion(node_name);
            }
            Some(WatcherCommand::Shutdown) | None => {
              info!(run_id = %self.run_id, "PodWatcher shutting down");
              return;
            }
          }
        }
        ev = combined.next() => {
          match ev {
            Some(Ok(event)) => self.handle_event(&mut state, event).await,
            Some(Err(e)) => {
              warn!(
                run_id = %self.run_id,
                error = %e,
                "Pod watcher stream error; default_backoff will reconnect"
              );
            }
            None => {
              warn!(run_id = %self.run_id, "Pod watcher stream ended");
              return;
            }
          }
        }
      }
    }
  }

  async fn handle_event(&self, state: &mut WatcherState, event: Event<Pod>) {
    match event {
      Event::Apply(pod) | Event::InitApply(pod) => {
        if let Some(signal) = classify_pod(&pod)
          && state.should_emit(&signal)
        {
          self.send_signal(signal).await;
        }
      }
      Event::Delete(pod) => self.handle_delete(state, &pod).await,
      Event::Init | Event::InitDone => {}
    }
  }

  async fn handle_delete(&self, state: &mut WatcherState, pod: &Pod) {
    let Some(node_name) = pod
      .metadata
      .annotations
      .as_ref()
      .and_then(|a| a.get(NODE_NAME_ANNOTATION))
      .cloned()
    else {
      return;
    };
    if state.deletion_was_expected(&node_name) {
      debug!(
        run_id = %self.run_id,
        node_name,
        "Pod deletion was expected; not emitting infra failure"
      );
      return;
    }
    if !state.was_running(&node_name) {
      return;
    }
    let pod_uid = pod.metadata.uid.clone().unwrap_or_default();
    let signal = PodSignal::InfraFailure {
      node_name,
      pod_uid,
      reason: InfraFailureReason::PodDeletedUnexpectedly,
    };
    if state.should_emit(&signal) {
      self.send_signal(signal).await;
    }
  }

  async fn send_signal(&self, signal: PodSignal) {
    if let Err(e) = self.signal_tx.send(signal).await {
      error!(
        run_id = %self.run_id,
        error = %e,
        "Failed to forward pod signal to coordinator"
      );
    }
  }
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use k8s_openapi::{
    api::core::v1::{
      ContainerState, ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting,
      ContainerStatus, Pod, PodStatus,
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
  };

  use super::*;

  fn pod_with(status: PodStatus) -> Pod {
    let mut annotations = BTreeMap::new();
    annotations.insert(NODE_NAME_ANNOTATION.to_string(), "build".to_string());
    Pod {
      metadata: ObjectMeta {
        annotations: Some(annotations),
        uid: Some("uid-123".to_string()),
        ..Default::default()
      },
      status: Some(status),
      ..Default::default()
    }
  }

  fn waiting_main_container(reason: &str, message: &str, image: &str) -> ContainerStatus {
    ContainerStatus {
      name: "tube".to_string(),
      image: image.to_string(),
      image_id: String::new(),
      ready: false,
      restart_count: 0,
      state: Some(ContainerState {
        waiting: Some(ContainerStateWaiting {
          reason: Some(reason.to_string()),
          message: Some(message.to_string()),
        }),
        running: None,
        terminated: None,
      }),
      ..Default::default()
    }
  }

  fn running_main_container(image: &str) -> ContainerStatus {
    ContainerStatus {
      name: "tube".to_string(),
      image: image.to_string(),
      image_id: String::new(),
      ready: true,
      restart_count: 0,
      state: Some(ContainerState {
        waiting: None,
        running: Some(ContainerStateRunning { started_at: None }),
        terminated: None,
      }),
      ..Default::default()
    }
  }

  fn terminated_main_container(reason: &str, exit_code: i32) -> ContainerStatus {
    ContainerStatus {
      name: "tube".to_string(),
      image: "image:tag".to_string(),
      image_id: String::new(),
      ready: false,
      restart_count: 0,
      state: Some(ContainerState {
        waiting: None,
        running: None,
        terminated: Some(ContainerStateTerminated {
          exit_code,
          reason: Some(reason.to_string()),
          message: None,
          finished_at: None,
          started_at: None,
          signal: None,
          container_id: None,
        }),
      }),
      ..Default::default()
    }
  }

  fn failed_init_container(reason: &str) -> ContainerStatus {
    ContainerStatus {
      name: "tube-init".to_string(),
      image: "init-image".to_string(),
      image_id: String::new(),
      ready: false,
      restart_count: 0,
      state: Some(ContainerState {
        waiting: Some(ContainerStateWaiting {
          reason: Some(reason.to_string()),
          message: Some("init bad".to_string()),
        }),
        running: None,
        terminated: None,
      }),
      ..Default::default()
    }
  }

  #[test]
  fn classifies_image_pull_back_off() {
    let pod = pod_with(PodStatus {
      container_statuses: Some(vec![waiting_main_container(
        "ImagePullBackOff",
        "Back-off pulling image \"foo:bar\"",
        "foo:bar",
      )]),
      ..Default::default()
    });
    let signal = classify_pod(&pod).expect("should classify");
    match signal {
      PodSignal::InfraFailure {
        node_name,
        reason: InfraFailureReason::ImagePullFailed { image, .. },
        ..
      } => {
        assert_eq!(node_name, "build");
        assert_eq!(image, "foo:bar");
      }
      other => panic!("expected ImagePullFailed, got {other:?}"),
    }
  }

  #[test]
  fn classifies_err_image_pull() {
    let pod = pod_with(PodStatus {
      container_statuses: Some(vec![waiting_main_container(
        "ErrImagePull",
        "rpc error",
        "foo:bar",
      )]),
      ..Default::default()
    });
    let signal = classify_pod(&pod).expect("should classify");
    assert!(matches!(
      signal,
      PodSignal::InfraFailure {
        reason: InfraFailureReason::ImagePullFailed { .. },
        ..
      }
    ));
  }

  #[test]
  fn classifies_oom_killed() {
    let pod = pod_with(PodStatus {
      container_statuses: Some(vec![terminated_main_container("OOMKilled", 137)]),
      ..Default::default()
    });
    let signal = classify_pod(&pod).expect("should classify");
    assert!(matches!(
      signal,
      PodSignal::InfraFailure {
        reason: InfraFailureReason::OOMKilled,
        ..
      }
    ));
  }

  #[test]
  fn classifies_create_container_config_error() {
    let pod = pod_with(PodStatus {
      container_statuses: Some(vec![waiting_main_container(
        "CreateContainerConfigError",
        "bad mount",
        "img",
      )]),
      ..Default::default()
    });
    let signal = classify_pod(&pod).expect("should classify");
    assert!(matches!(
      signal,
      PodSignal::InfraFailure {
        reason: InfraFailureReason::ContainerCreateError(_),
        ..
      }
    ));
  }

  #[test]
  fn classifies_init_container_image_pull_back_off() {
    let pod = pod_with(PodStatus {
      init_container_statuses: Some(vec![failed_init_container("ImagePullBackOff")]),
      ..Default::default()
    });
    let signal = classify_pod(&pod).expect("should classify");
    assert!(matches!(
      signal,
      PodSignal::InfraFailure {
        reason: InfraFailureReason::InitContainerFailed { .. },
        ..
      }
    ));
  }

  #[test]
  fn classifies_running_main_container() {
    let pod = pod_with(PodStatus {
      container_statuses: Some(vec![running_main_container("img")]),
      ..Default::default()
    });
    let signal = classify_pod(&pod).expect("should classify");
    assert!(matches!(signal, PodSignal::PodRunning { .. }));
  }

  #[test]
  fn pending_pod_with_no_status_is_ignored() {
    let pod = pod_with(PodStatus::default());
    assert!(classify_pod(&pod).is_none());
  }

  #[test]
  fn pod_without_node_name_annotation_is_ignored() {
    let pod = Pod {
      metadata: ObjectMeta {
        uid: Some("uid".into()),
        ..Default::default()
      },
      status: Some(PodStatus {
        container_statuses: Some(vec![running_main_container("img")]),
        ..Default::default()
      }),
      ..Default::default()
    };
    assert!(classify_pod(&pod).is_none());
  }

  #[test]
  fn dedup_prevents_duplicate_emit_for_same_uid() {
    let mut state = WatcherState::default();
    let signal = PodSignal::InfraFailure {
      node_name: "build".into(),
      pod_uid: "uid-1".into(),
      reason: InfraFailureReason::OOMKilled,
    };
    assert!(state.should_emit(&signal));
    assert!(!state.should_emit(&signal));
  }

  #[test]
  fn dedup_allows_new_uid_for_same_node() {
    let mut state = WatcherState::default();
    let s1 = PodSignal::PodRunning {
      node_name: "build".into(),
      pod_uid: "uid-1".into(),
    };
    let s2 = PodSignal::PodRunning {
      node_name: "build".into(),
      pod_uid: "uid-2".into(),
    };
    assert!(state.should_emit(&s1));
    assert!(state.should_emit(&s2));
  }

  #[test]
  fn user_message_is_none_for_non_actionable_reasons() {
    assert!(
      InfraFailureReason::InitContainerFailed {
        name: "init".into(),
        message: "x".into()
      }
      .user_message()
      .is_none()
    );
    assert!(
      InfraFailureReason::PodDeletedUnexpectedly
        .user_message()
        .is_none()
    );
    assert!(InfraFailureReason::NodeLost.user_message().is_none());
  }

  #[test]
  fn user_message_is_some_for_actionable_reasons() {
    assert!(
      InfraFailureReason::ImagePullFailed {
        image: "x".into(),
        message: "y".into()
      }
      .user_message()
      .is_some()
    );
    assert!(InfraFailureReason::OOMKilled.user_message().is_some());
    assert!(InfraFailureReason::PodStartTimeout.user_message().is_some());
  }
}
