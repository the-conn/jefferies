use std::{collections::HashMap, sync::Arc, time::Duration};

use app_config::AppConfig;
use async_trait::async_trait;
use backplane::{Backplane, BackplaneEvent};
use chrono::{DateTime, Utc};
use pipelines::{NodeInfo, Pipeline};
use run_history::{
  NodeDispatchRecord, NodeRunRecord, PipelineRunRecord, PipelineStartRecord, RunHistory, RunStatus,
};
use state_store::{NodeStatus, StateStore, StateStoreError};
use tokio::{sync::mpsc, task::JoinHandle, time::interval};
use tracing::{error, info, warn};

use crate::{
  dispatcher::Dispatcher,
  message::CoordinatorMessage,
  pod_watcher::{InfraFailureReason, PodSignal, WatcherCommand},
  run::PipelineRun,
};

pub struct RunContext {
  pub owner: String,
  pub repo: String,
  pub sha: String,
  pub branch: Option<String>,
  pub target_branch: Option<String>,
  pub tag: Option<String>,
  pub pr_number: Option<i64>,
  pub trigger: String,
  pub pipeline_yaml: String,
  pub created_at: DateTime<Utc>,
  pub retry_of: Option<String>,
  pub tenant_slug: Option<String>,
}

pub struct CoordinatorServices {
  pub config: Arc<AppConfig>,
  pub dispatcher: Arc<dyn Dispatcher>,
  pub state_store: Arc<dyn StateStore>,
  pub backplane: Arc<dyn Backplane>,
  pub run_history: Arc<dyn RunHistory>,
  pub status_reporter: Option<Arc<dyn RunStatusReporter>>,
}

#[async_trait]
pub trait RunStatusReporter: Send + Sync {
  async fn report_completed(&self, status: RunStatus);
}

pub struct RunSummary {
  pub run_id: String,
  pub status: RunStatus,
  pub node_statuses: HashMap<String, NodeStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerPhase {
  Startup,
  Runtime,
}

struct PhaseTimer {
  phase: TimerPhase,
  handle: JoinHandle<()>,
}

struct Coordinator {
  run_id: String,
  run: PipelineRun,
  pipeline: Arc<Pipeline>,
  config: Arc<AppConfig>,
  node_info_cache: HashMap<String, NodeInfo>,
  node_phase_handles: HashMap<String, PhaseTimer>,
  internal_tx: mpsc::Sender<CoordinatorMessage>,
  internal_rx: mpsc::Receiver<CoordinatorMessage>,
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  backplane: Arc<dyn Backplane>,
  state_version: u64,
  lease_version: u64,
  server_id: String,
  run_context: RunContext,
  run_history: Arc<dyn RunHistory>,
  status_reporter: Option<Arc<dyn RunStatusReporter>>,
  pod_watcher_handle: Option<JoinHandle<()>>,
  signal_relay_handle: Option<JoinHandle<()>>,
  watcher_cmd_tx: Option<mpsc::Sender<WatcherCommand>>,
}

pub async fn start_coordinator(
  run_id: String,
  pipeline: Arc<Pipeline>,
  services: CoordinatorServices,
  run_context: RunContext,
) -> Option<JoinHandle<RunSummary>> {
  let server_id = uuid::Uuid::new_v4().to_string();

  let lease_version = match services
    .state_store
    .try_acquire_lease(&run_id, &server_id, 30)
    .await
  {
    Ok(Some(v)) => v,
    Ok(None) => {
      info!(
        run_id,
        "Could not acquire lease; another server is handling this run"
      );
      return None;
    }
    Err(e) => {
      error!(run_id, error = %e, "Failed to acquire lease");
      return None;
    }
  };

  let node_infos = pipeline.node_info();
  let node_info_cache = node_infos
    .iter()
    .map(|n| (n.name.clone(), n.clone()))
    .collect();

  let (run, state_version) = initialize_run_state(
    &run_id,
    &node_infos,
    &pipeline,
    services.state_store.as_ref(),
  )
  .await;

  let (internal_tx, internal_rx) = mpsc::channel(128);

  let (pod_watcher_handle, signal_relay_handle, watcher_cmd_tx) =
    spawn_pod_watcher(&run_id, &services.dispatcher, internal_tx.clone()).await;

  let coordinator = Coordinator {
    run_id,
    run,
    pipeline,
    config: services.config,
    node_info_cache,
    node_phase_handles: HashMap::new(),
    internal_tx,
    internal_rx,
    dispatcher: services.dispatcher,
    state_store: services.state_store,
    backplane: services.backplane,
    state_version,
    lease_version,
    server_id,
    run_context,
    run_history: services.run_history,
    status_reporter: services.status_reporter,
    pod_watcher_handle,
    signal_relay_handle,
    watcher_cmd_tx,
  };

  Some(tokio::spawn(coordinator.run()))
}

async fn spawn_pod_watcher(
  run_id: &str,
  dispatcher: &Arc<dyn Dispatcher>,
  internal_tx: mpsc::Sender<CoordinatorMessage>,
) -> (
  Option<JoinHandle<()>>,
  Option<JoinHandle<()>>,
  Option<mpsc::Sender<WatcherCommand>>,
) {
  let (signal_tx, mut signal_rx) = mpsc::channel::<PodSignal>(128);
  let (cmd_tx, cmd_rx) = mpsc::channel::<WatcherCommand>(32);

  let Some(watcher_handle) = dispatcher
    .start_pod_watcher(run_id, signal_tx, cmd_rx)
    .await
  else {
    return (None, None, None);
  };

  let run_id_owned = run_id.to_string();
  let relay_handle = tokio::spawn(async move {
    while let Some(signal) = signal_rx.recv().await {
      let msg = match signal {
        PodSignal::PodRunning { node_name, .. } => CoordinatorMessage::NodePodRunning { node_name },
        PodSignal::InfraFailure {
          node_name, reason, ..
        } => CoordinatorMessage::NodeInfraFailed { node_name, reason },
      };
      if let Err(e) = internal_tx.send(msg).await {
        warn!(run_id = %run_id_owned, error = %e, "Pod signal relay channel closed");
        return;
      }
    }
  });

  (Some(watcher_handle), Some(relay_handle), Some(cmd_tx))
}

async fn initialize_run_state(
  run_id: &str,
  node_infos: &[NodeInfo],
  pipeline: &Arc<Pipeline>,
  state_store: &dyn StateStore,
) -> (PipelineRun, u64) {
  match state_store.load_run(run_id).await {
    Ok(Some(existing)) => {
      info!(
        run_id,
        version = existing.version,
        "Resuming existing run state"
      );
      let run = PipelineRun::from_run_state(&existing);
      (run, existing.version)
    }
    Ok(None) => {
      let run = PipelineRun::new(node_infos);
      let state = run.to_run_state(0, pipeline.clone());
      if let Err(e) = state_store.save_run(run_id, &state, 0).await {
        error!(run_id, error = %e, "Failed to save initial run state");
      }
      (run, 0)
    }
    Err(e) => {
      error!(run_id, error = %e, "Failed to load run state; starting fresh");
      let run = PipelineRun::new(node_infos);
      (run, 0)
    }
  }
}

fn ms_to_datetime(ms: u128) -> Option<DateTime<Utc>> {
  DateTime::from_timestamp((ms / 1000) as i64, ((ms % 1000) * 1_000_000) as u32)
}

impl Coordinator {
  async fn run(mut self) -> RunSummary {
    info!(run_id = %self.run_id, server_id = %self.server_id, "Coordinator starting");
    self.record_pipeline_started().await;

    let pipeline_timeout_secs = self
      .pipeline
      .pipeline_timeout_secs()
      .unwrap_or_else(|| self.config.default_pipeline_timeout_secs());
    let pipeline_deadline = tokio::time::sleep(Duration::from_secs(pipeline_timeout_secs));
    tokio::pin!(pipeline_deadline);

    let mut subscription = match self.backplane.subscribe_run(&self.run_id).await {
      Ok(sub) => sub,
      Err(e) => {
        error!(run_id = %self.run_id, error = %e, "Failed to subscribe to backplane");
        return self.build_summary(RunStatus::Failure).await;
      }
    };

    let mut heartbeat = interval(Duration::from_secs(15));
    heartbeat.tick().await;

    self.dispatch_ready_nodes().await;

    if self.run.is_complete() {
      let status = self.outcome_status();
      self.finalize_run(status).await;
      self.cleanup().await;
      return self.build_summary(status).await;
    }

    loop {
      tokio::select! {
        _ = &mut pipeline_deadline => {
          warn!(
            run_id = %self.run_id,
            timeout_secs = pipeline_timeout_secs,
            "Pipeline timeout exceeded"
          );
          self.finalize_run(RunStatus::Failure).await;
          self.cleanup().await;
          return self.terminate_running_nodes(RunStatus::Failure).await;
        }
        _ = heartbeat.tick() => {
          let renewed = self.state_store
            .renew_lease(&self.run_id, &self.server_id, self.lease_version, 30)
            .await
            .unwrap_or(false);
          if !renewed {
            warn!(run_id = %self.run_id, "Lease renewal failed; another server took over");
            return self.build_summary(RunStatus::Failure).await;
          }
        }
        msg = self.internal_rx.recv() => {
          match msg {
            Some(CoordinatorMessage::NodeTimedOut { node_name }) => {
              if self.handle_node_timed_out(&node_name).await {
                self.finalize_run(RunStatus::Failure).await;
                self.cleanup().await;
                return self.terminate_running_nodes(RunStatus::Failure).await;
              }
              if self.run.is_complete() {
                break;
              }
            }
            Some(CoordinatorMessage::NodePodRunning { node_name }) => {
              self.handle_pod_running(&node_name).await;
            }
            Some(CoordinatorMessage::NodeInfraFailed { node_name, reason }) => {
              if self.handle_node_infra_failed(&node_name, &reason).await {
                self.finalize_run(RunStatus::Failure).await;
                self.cleanup().await;
                return self.terminate_running_nodes(RunStatus::Failure).await;
              }
              if self.run.is_complete() {
                break;
              }
            }
            Some(CoordinatorMessage::NodePodStartTimedOut { node_name }) => {
              if self.handle_pod_start_timed_out(&node_name).await {
                self.finalize_run(RunStatus::Failure).await;
                self.cleanup().await;
                return self.terminate_running_nodes(RunStatus::Failure).await;
              }
              if self.run.is_complete() {
                break;
              }
            }
            None => {}
          }
        }
        event = subscription.next_event() => {
          match event {
            Some(BackplaneEvent::NodeCompleted { node_name, success }) => {
              self.cancel_node_timeout(&node_name);
              if self.handle_node_completed(&node_name, success).await {
                self.finalize_run(RunStatus::Failure).await;
                self.cleanup().await;
                return self.terminate_running_nodes(RunStatus::Failure).await;
              }
              if self.run.is_complete() {
                break;
              }
            }
            Some(BackplaneEvent::Cancel) => {
              info!(run_id = %self.run_id, "Received Cancel event from backplane");
              self.finalize_run(RunStatus::Cancelled).await;
              self.cleanup().await;
              return self.terminate_running_nodes(RunStatus::Cancelled).await;
            }
            None => {
              warn!(run_id = %self.run_id, "Backplane subscription closed unexpectedly");
              self.finalize_run(RunStatus::Failure).await;
              self.cleanup().await;
              return self.terminate_running_nodes(RunStatus::Failure).await;
            }
          }
        }
      }
    }

    let status = self.outcome_status();
    self.finalize_run(status).await;
    self.cleanup().await;
    self.build_summary(status).await
  }

  fn outcome_status(&self) -> RunStatus {
    if self
      .run
      .statuses()
      .values()
      .all(|s| *s == NodeStatus::Success)
    {
      RunStatus::Success
    } else {
      RunStatus::Failure
    }
  }

  async fn persist_state(&mut self) -> Result<(), StateStoreError> {
    let new_version = self.state_version + 1;
    let state = self.run.to_run_state(new_version, self.pipeline.clone());
    let accepted = self
      .state_store
      .save_run(&self.run_id, &state, self.state_version)
      .await?;
    if accepted {
      self.state_version = new_version;
      Ok(())
    } else {
      Err(StateStoreError::Store(format!(
        "Version fencing rejected save for run {}",
        self.run_id
      )))
    }
  }

  fn cancel_node_timeout(&mut self, node_name: &str) {
    if let Some(timer) = self.node_phase_handles.remove(node_name) {
      timer.handle.abort();
    }
  }

  async fn notify_watcher_of_deletion(&self, node_name: &str) {
    let Some(tx) = self.watcher_cmd_tx.as_ref() else {
      return;
    };
    if let Err(e) = tx
      .send(WatcherCommand::ExpectDeletion {
        node_name: node_name.to_string(),
      })
      .await
    {
      warn!(
        run_id = %self.run_id,
        node_name,
        error = %e,
        "Failed to notify pod watcher of expected deletion"
      );
    }
  }

  async fn handle_pod_running(&mut self, node_name: &str) {
    if let Some(existing) = self.node_phase_handles.get(node_name)
      && existing.phase == TimerPhase::Runtime
    {
      return;
    }
    let runtime_secs = self
      .node_info_cache
      .get(node_name)
      .and_then(|n| n.timeout_secs);
    if let Some(prev) = self.node_phase_handles.remove(node_name) {
      prev.handle.abort();
    }
    let handle = self.spawn_runtime_timeout(node_name, runtime_secs);
    self.node_phase_handles.insert(
      node_name.to_string(),
      PhaseTimer {
        phase: TimerPhase::Runtime,
        handle,
      },
    );
    info!(
      run_id = %self.run_id,
      node_name,
      "Pod entered Running phase; switching to runtime timeout"
    );
  }

  async fn handle_node_infra_failed(
    &mut self,
    node_name: &str,
    reason: &InfraFailureReason,
  ) -> bool {
    let already_terminal = self
      .run
      .statuses()
      .get(node_name)
      .is_some_and(|s| matches!(s, NodeStatus::Success | NodeStatus::Failed));
    if already_terminal {
      tracing::debug!(
        run_id = %self.run_id,
        node_name,
        stable_code = reason.stable_code(),
        "Ignoring infra failure for already-terminal node"
      );
      return false;
    }

    error!(
      run_id = %self.run_id,
      node_name,
      stable_code = reason.stable_code(),
      message = %reason.full_message(),
      user_actionable = reason.user_message().is_some(),
      "Infrastructure failure detected"
    );

    if !self.run.mark_failed(node_name) {
      return false;
    }
    self.cancel_node_timeout(node_name);
    if let Err(e) = self.persist_state().await {
      error!(run_id = %self.run_id, error = %e, "Failed to persist state after infra failure; stopping");
      return true;
    }
    self.notify_watcher_of_deletion(node_name).await;
    if let Err(e) = self
      .dispatcher
      .cancel_node(&self.run_id, node_name, &self.config)
      .await
    {
      warn!(run_id = %self.run_id, node_name, error = %e, "Failed to cancel infra-failed node");
    }
    self.record_node_completed(node_name, Some(reason)).await;
    if self.fail_fast_enabled() {
      warn!(run_id = %self.run_id, node_name, "Fail-fast enabled; cancelling pipeline");
      true
    } else {
      self.dispatch_ready_nodes().await;
      false
    }
  }

  async fn handle_pod_start_timed_out(&mut self, node_name: &str) -> bool {
    self
      .handle_node_infra_failed(node_name, &InfraFailureReason::PodStartTimeout)
      .await
  }

  async fn handle_node_completed(&mut self, node_name: &str, success: bool) -> bool {
    if success {
      if !self.run.mark_success(node_name) {
        warn!(run_id = %self.run_id, node_name, "Unexpected state transition: node was not Running");
      } else {
        info!(run_id = %self.run_id, node_name, "Node completed successfully");
      }
      if let Err(e) = self.persist_state().await {
        error!(run_id = %self.run_id, error = %e, "Failed to persist state after node success; stopping");
        return true;
      }
      self.record_node_completed(node_name, None).await;
      self.dispatch_ready_nodes().await;
      false
    } else {
      if !self.run.mark_failed(node_name) {
        warn!(run_id = %self.run_id, node_name, "Unexpected state transition: node was not Running");
      } else {
        warn!(run_id = %self.run_id, node_name, "Node failed");
      }
      if let Err(e) = self.persist_state().await {
        error!(run_id = %self.run_id, error = %e, "Failed to persist state after node failure; stopping");
        return true;
      }
      self.record_node_completed(node_name, None).await;
      if self.fail_fast_enabled() {
        warn!(run_id = %self.run_id, node_name, "Fail-fast enabled; cancelling pipeline");
        true
      } else {
        self.dispatch_ready_nodes().await;
        false
      }
    }
  }

  async fn handle_node_timed_out(&mut self, node_name: &str) -> bool {
    if !self.run.mark_failed(node_name) {
      return false;
    }
    warn!(run_id = %self.run_id, node_name, "Node timed out");
    if let Err(e) = self.persist_state().await {
      error!(run_id = %self.run_id, error = %e, "Failed to persist state after node timeout; stopping");
      return true;
    }
    self.notify_watcher_of_deletion(node_name).await;
    if let Err(e) = self
      .dispatcher
      .cancel_node(&self.run_id, node_name, &self.config)
      .await
    {
      warn!(run_id = %self.run_id, node_name, error = %e, "Failed to cancel timed-out node");
    }
    self.record_node_completed(node_name, None).await;
    if self.fail_fast_enabled() {
      warn!(run_id = %self.run_id, node_name, "Fail-fast enabled; cancelling pipeline");
      true
    } else {
      self.dispatch_ready_nodes().await;
      false
    }
  }

  fn fail_fast_enabled(&self) -> bool {
    self
      .pipeline
      .fail_fast_override()
      .unwrap_or_else(|| self.config.default_fail_fast())
  }

  async fn dispatch_ready_nodes(&mut self) {
    for node_name in self.run.ready_nodes() {
      let Some(node) = self.node_info_cache.get(&node_name).cloned() else {
        error!(run_id = %self.run_id, node_name = %node_name, "Node info not found in pipeline");
        self.run.mark_dispatch_failed(&node_name);
        self
          .record_dispatch_failed(&node_name, "Node info not found in pipeline")
          .await;
        continue;
      };

      let startup_secs = node
        .startup_timeout_secs
        .unwrap_or_else(|| self.config.default_node_startup_timeout_secs());
      match self
        .dispatcher
        .dispatch(
          &self.run_id,
          &self.run_context.owner,
          &self.run_context.repo,
          &node,
          &self.pipeline,
          &self.config,
        )
        .await
      {
        Ok(()) => {
          if !self.run.mark_running(&node_name) {
            warn!(run_id = %self.run_id, node_name = %node_name, "Unexpected state transition: node was not Pending");
          } else {
            info!(run_id = %self.run_id, node_name = %node_name, "Node dispatched");
            self.record_node_dispatched(&node_name, &node).await;
            let handle = self.spawn_startup_timeout(&node_name, startup_secs);
            self.node_phase_handles.insert(
              node_name,
              PhaseTimer {
                phase: TimerPhase::Startup,
                handle,
              },
            );
          }
        }
        Err(e) => {
          let err_message = e.to_string();
          error!(run_id = %self.run_id, node_name = %node_name, error = %err_message, "Failed to dispatch node");
          self.run.mark_dispatch_failed(&node_name);
          self.record_dispatch_failed(&node_name, &err_message).await;
        }
      }
    }
  }

  async fn record_dispatch_failed(&self, node_name: &str, error: &str) {
    let node_definition = self
      .node_info_cache
      .get(node_name)
      .and_then(|info| serde_json::to_string(info).ok())
      .unwrap_or_default();
    let now = Utc::now();
    let node_record = NodeRunRecord {
      run_id: self.run_id.clone(),
      node_name: node_name.to_string(),
      node_definition,
      success: false,
      created_at: now,
      started_at: None,
      completed_at: Some(now),
      output_log: Some(format!("Dispatch failed: {error}")),
      failure_reason: Some(
        InfraFailureReason::DispatchFailed(error.to_string())
          .stable_code()
          .to_string(),
      ),
    };
    if let Err(e) = self.run_history.record_node_run(node_record).await {
      warn!(run_id = %self.run_id, node_name, error = %e, "Failed to record dispatch-failed node run history");
    }
  }

  fn spawn_startup_timeout(&self, node_name: &str, timeout_secs: u64) -> JoinHandle<()> {
    let tx = self.internal_tx.clone();
    let name = node_name.to_string();
    tokio::spawn(async move {
      tokio::time::sleep(Duration::from_secs(timeout_secs)).await;
      let _ = tx
        .send(CoordinatorMessage::NodePodStartTimedOut { node_name: name })
        .await;
    })
  }

  fn spawn_runtime_timeout(&self, node_name: &str, override_secs: Option<u64>) -> JoinHandle<()> {
    let timeout_secs = override_secs.unwrap_or_else(|| self.config.default_node_timeout_secs());
    let tx = self.internal_tx.clone();
    let name = node_name.to_string();
    tokio::spawn(async move {
      tokio::time::sleep(Duration::from_secs(timeout_secs)).await;
      let _ = tx
        .send(CoordinatorMessage::NodeTimedOut { node_name: name })
        .await;
    })
  }

  async fn terminate_running_nodes(mut self, status: RunStatus) -> RunSummary {
    info!(run_id = %self.run_id, status = status.as_str(), "Coordinator terminating running nodes");

    for (_, timer) in self.node_phase_handles.drain() {
      timer.handle.abort();
    }

    let running_nodes: Vec<String> = self
      .run
      .statuses()
      .iter()
      .filter(|(_, s)| **s == NodeStatus::Running)
      .map(|(name, _)| name.clone())
      .collect();
    for node_name in &running_nodes {
      self.notify_watcher_of_deletion(node_name).await;
      if let Err(e) = self
        .dispatcher
        .cancel_node(&self.run_id, node_name, &self.config)
        .await
      {
        warn!(run_id = %self.run_id, node_name, error = %e, "Failed to cancel node");
      }
    }

    self.shutdown_pod_watcher().await;

    let node_statuses = self.run.statuses().clone();
    RunSummary {
      run_id: self.run_id,
      status,
      node_statuses,
    }
  }

  async fn shutdown_pod_watcher(&mut self) {
    if let Some(tx) = self.watcher_cmd_tx.take() {
      let _ = tx.send(WatcherCommand::Shutdown).await;
    }
    if let Some(handle) = self.signal_relay_handle.take() {
      handle.abort();
    }
    if let Some(handle) = self.pod_watcher_handle.take() {
      handle.abort();
    }
  }

  async fn record_pipeline_started(&self) {
    let record = PipelineStartRecord {
      run_id: self.run_id.clone(),
      pipeline_name: self.pipeline.name().to_string(),
      owner: self.run_context.owner.clone(),
      repo: self.run_context.repo.clone(),
      sha: self.run_context.sha.clone(),
      branch: self.run_context.branch.clone(),
      target_branch: self.run_context.target_branch.clone(),
      tag: self.run_context.tag.clone(),
      pr_number: self.run_context.pr_number,
      trigger: self.run_context.trigger.clone(),
      pipeline_definition: self.run_context.pipeline_yaml.clone(),
      created_at: self.run_context.created_at,
      retry_of: self.run_context.retry_of.clone(),
      tenant_slug: self.run_context.tenant_slug.clone(),
    };
    if let Err(e) = self.run_history.record_pipeline_started(record).await {
      warn!(run_id = %self.run_id, error = %e, "Failed to record pipeline start");
    }
  }

  async fn record_node_dispatched(&self, node_name: &str, node: &NodeInfo) {
    let node_definition = serde_json::to_string(node).unwrap_or_default();
    let record = NodeDispatchRecord {
      run_id: self.run_id.clone(),
      node_name: node_name.to_string(),
      node_definition,
      created_at: Utc::now(),
    };
    if let Err(e) = self.run_history.record_node_dispatched(record).await {
      warn!(run_id = %self.run_id, node_name, error = %e, "Failed to record node dispatch");
    }
  }

  async fn record_node_completed(
    &self,
    node_name: &str,
    failure_reason: Option<&InfraFailureReason>,
  ) {
    let node_success = self
      .run
      .statuses()
      .get(node_name)
      .map(|s| *s == NodeStatus::Success)
      .unwrap_or(false);

    let node_definition = self
      .node_info_cache
      .get(node_name)
      .and_then(|info| serde_json::to_string(info).ok())
      .unwrap_or_default();

    let outcome = match self
      .dispatcher
      .get_node_outcome(&self.run_id, node_name)
      .await
    {
      Ok(o) => o,
      Err(e) => {
        warn!(run_id = %self.run_id, node_name, error = %e, "Failed to fetch node outcome for history");
        None
      }
    };

    let started_at = outcome
      .as_ref()
      .and_then(|o| o.started_at)
      .and_then(ms_to_datetime);
    let node_completed_at = outcome
      .as_ref()
      .and_then(|o| o.finished_at)
      .and_then(ms_to_datetime)
      .or_else(|| failure_reason.is_some().then(Utc::now));

    let output_log = match self.dispatcher.get_node_log(&self.run_id, node_name).await {
      Ok(log) => log,
      Err(e) => {
        warn!(run_id = %self.run_id, node_name, error = %e, "Failed to fetch node log for history");
        None
      }
    };

    let node_record = NodeRunRecord {
      run_id: self.run_id.clone(),
      node_name: node_name.to_string(),
      node_definition,
      success: node_success,
      created_at: Utc::now(),
      started_at,
      completed_at: node_completed_at,
      output_log,
      failure_reason: failure_reason.map(|r| r.stable_code().to_string()),
    };
    if let Err(e) = self.run_history.record_node_run(node_record).await {
      warn!(run_id = %self.run_id, node_name, error = %e, "Failed to record node run history");
    }
  }

  async fn record_history(&mut self, status: RunStatus) {
    let running_nodes: Vec<String> = self
      .run
      .statuses()
      .iter()
      .filter(|(_, s)| **s == NodeStatus::Running)
      .map(|(name, _)| name.clone())
      .collect();
    for node_name in &running_nodes {
      self.record_running_node_terminated(node_name).await;
      self.run.mark_failed(node_name);
    }

    let completed_at = Utc::now();
    let pipeline_record = PipelineRunRecord {
      run_id: self.run_id.clone(),
      pipeline_name: self.pipeline.name().to_string(),
      owner: self.run_context.owner.clone(),
      repo: self.run_context.repo.clone(),
      sha: self.run_context.sha.clone(),
      branch: self.run_context.branch.clone(),
      target_branch: self.run_context.target_branch.clone(),
      tag: self.run_context.tag.clone(),
      pr_number: self.run_context.pr_number,
      trigger: self.run_context.trigger.clone(),
      pipeline_definition: self.run_context.pipeline_yaml.clone(),
      status,
      created_at: self.run_context.created_at,
      completed_at: Some(completed_at),
      retry_of: self.run_context.retry_of.clone(),
      tenant_slug: self.run_context.tenant_slug.clone(),
    };
    if let Err(e) = self.run_history.record_pipeline_run(pipeline_record).await {
      warn!(run_id = %self.run_id, error = %e, "Failed to record pipeline run history");
    }
  }

  async fn finalize_run(&mut self, status: RunStatus) {
    self.record_history(status).await;
    if let Some(reporter) = self.status_reporter.as_ref() {
      reporter.report_completed(status).await;
    }
  }

  async fn record_running_node_terminated(&self, node_name: &str) {
    let node_definition = self
      .node_info_cache
      .get(node_name)
      .and_then(|info| serde_json::to_string(info).ok())
      .unwrap_or_default();

    let outcome = match self
      .dispatcher
      .get_node_outcome(&self.run_id, node_name)
      .await
    {
      Ok(o) => o,
      Err(e) => {
        warn!(run_id = %self.run_id, node_name, error = %e, "Failed to fetch node outcome for terminated node");
        None
      }
    };

    let started_at = outcome
      .as_ref()
      .and_then(|o| o.started_at)
      .and_then(ms_to_datetime);
    let completed_at = outcome
      .as_ref()
      .and_then(|o| o.finished_at)
      .and_then(ms_to_datetime)
      .or_else(|| Some(Utc::now()));
    let success = outcome.as_ref().and_then(|o| o.success).unwrap_or(false);

    let output_log = match self.dispatcher.get_node_log(&self.run_id, node_name).await {
      Ok(log) => log,
      Err(e) => {
        warn!(run_id = %self.run_id, node_name, error = %e, "Failed to fetch node log for terminated node");
        None
      }
    };

    let node_record = NodeRunRecord {
      run_id: self.run_id.clone(),
      node_name: node_name.to_string(),
      node_definition,
      success,
      created_at: Utc::now(),
      started_at,
      completed_at,
      output_log,
      failure_reason: None,
    };
    if let Err(e) = self.run_history.record_node_run(node_record).await {
      warn!(run_id = %self.run_id, node_name, error = %e, "Failed to record terminated node run history");
    }
  }

  async fn cleanup(&self) {
    if let Err(e) = self.dispatcher.cleanup_run(&self.run_id).await {
      warn!(run_id = %self.run_id, error = %e, "Failed to clean up S3 objects for run");
    }
    if let Err(e) = self.state_store.release_lease(&self.run_id).await {
      warn!(run_id = %self.run_id, error = %e, "Failed to release lease");
    }
    if let Err(e) = self.state_store.delete_run(&self.run_id).await {
      warn!(run_id = %self.run_id, error = %e, "Failed to delete run state");
    }
  }

  async fn build_summary(&mut self, status: RunStatus) -> RunSummary {
    info!(
      run_id = %self.run_id,
      status = %status,
      "Coordinator completed"
    );
    self.shutdown_pod_watcher().await;
    RunSummary {
      run_id: self.run_id.clone(),
      status,
      node_statuses: self.run.statuses().clone(),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{Arc, Mutex};

  use backplane::InMemoryBackplane;
  use chrono::Utc;
  use run_history::{
    ListRunsQuery, NoOpRunHistory, NodeDispatchRecord, NodeRunRecord, NodeRunRow,
    PipelineRunRecord, PipelineRunRow, PipelineStartRecord, RunFilters, RunHistory,
    RunHistoryError,
  };
  use state_store::InMemoryStateStore;

  use super::*;
  use crate::dispatcher::{DispatchError, Dispatcher};

  struct AlwaysSuccessDispatcher {
    backplane: Arc<dyn Backplane>,
  }

  #[async_trait::async_trait]
  impl Dispatcher for AlwaysSuccessDispatcher {
    async fn dispatch(
      &self,
      run_id: &str,
      _owner: &str,
      _repo: &str,
      node: &NodeInfo,
      _pipeline: &Pipeline,
      _config: &AppConfig,
    ) -> Result<(), DispatchError> {
      let backplane = self.backplane.clone();
      let run_id = run_id.to_string();
      let node_name = node.name.clone();
      tokio::spawn(async move {
        let _ = backplane
          .publish_node_completed(&run_id, &node_name, true)
          .await;
      });
      Ok(())
    }

    async fn cancel_node(
      &self,
      _run_id: &str,
      _node_name: &str,
      _config: &AppConfig,
    ) -> Result<(), DispatchError> {
      Ok(())
    }

    async fn cleanup_run(&self, _run_id: &str) -> Result<(), DispatchError> {
      Ok(())
    }
  }

  type SignalSender = mpsc::Sender<PodSignal>;

  struct InjectableDispatcher {
    cancel_calls: Arc<Mutex<Vec<String>>>,
    signal_tx_slot: Arc<Mutex<Option<SignalSender>>>,
    watcher_started: Arc<tokio::sync::Notify>,
    dispatch_called: Arc<tokio::sync::Notify>,
    dispatch_done: Arc<std::sync::atomic::AtomicBool>,
  }

  impl InjectableDispatcher {
    fn new() -> Self {
      Self {
        cancel_calls: Arc::new(Mutex::new(Vec::new())),
        signal_tx_slot: Arc::new(Mutex::new(None)),
        watcher_started: Arc::new(tokio::sync::Notify::new()),
        dispatch_called: Arc::new(tokio::sync::Notify::new()),
        dispatch_done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
      }
    }

    fn cancel_calls(&self) -> Arc<Mutex<Vec<String>>> {
      self.cancel_calls.clone()
    }

    fn signal_tx_slot(&self) -> Arc<Mutex<Option<SignalSender>>> {
      self.signal_tx_slot.clone()
    }

    fn watcher_started_notify(&self) -> Arc<tokio::sync::Notify> {
      self.watcher_started.clone()
    }

    fn dispatch_called_notify(&self) -> Arc<tokio::sync::Notify> {
      self.dispatch_called.clone()
    }

    fn dispatch_done_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
      self.dispatch_done.clone()
    }
  }

  #[async_trait::async_trait]
  impl Dispatcher for InjectableDispatcher {
    async fn dispatch(
      &self,
      _run_id: &str,
      _owner: &str,
      _repo: &str,
      _node: &NodeInfo,
      _pipeline: &Pipeline,
      _config: &AppConfig,
    ) -> Result<(), DispatchError> {
      self
        .dispatch_done
        .store(true, std::sync::atomic::Ordering::SeqCst);
      self.dispatch_called.notify_waiters();
      Ok(())
    }

    async fn cancel_node(
      &self,
      _run_id: &str,
      node_name: &str,
      _config: &AppConfig,
    ) -> Result<(), DispatchError> {
      self
        .cancel_calls
        .lock()
        .unwrap()
        .push(node_name.to_string());
      Ok(())
    }

    async fn cleanup_run(&self, _run_id: &str) -> Result<(), DispatchError> {
      Ok(())
    }

    async fn start_pod_watcher(
      &self,
      _run_id: &str,
      signal_tx: mpsc::Sender<PodSignal>,
      mut cmd_rx: mpsc::Receiver<WatcherCommand>,
    ) -> Option<JoinHandle<()>> {
      *self.signal_tx_slot.lock().unwrap() = Some(signal_tx);
      self.watcher_started.notify_waiters();
      Some(tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
          if matches!(cmd, WatcherCommand::Shutdown) {
            return;
          }
        }
      }))
    }
  }

  #[derive(Default)]
  struct CapturingRunHistory {
    node_runs: Arc<Mutex<Vec<NodeRunRecord>>>,
  }

  impl CapturingRunHistory {
    fn new() -> Arc<Self> {
      Arc::new(Self::default())
    }

    fn node_runs(&self) -> Vec<NodeRunRecord> {
      self
        .node_runs
        .lock()
        .unwrap()
        .iter()
        .map(|r| NodeRunRecord {
          run_id: r.run_id.clone(),
          node_name: r.node_name.clone(),
          node_definition: r.node_definition.clone(),
          success: r.success,
          created_at: r.created_at,
          started_at: r.started_at,
          completed_at: r.completed_at,
          output_log: r.output_log.clone(),
          failure_reason: r.failure_reason.clone(),
        })
        .collect()
    }
  }

  #[async_trait::async_trait]
  impl RunHistory for CapturingRunHistory {
    async fn record_pipeline_started(&self, _: PipelineStartRecord) -> Result<(), RunHistoryError> {
      Ok(())
    }
    async fn record_pipeline_run(&self, _: PipelineRunRecord) -> Result<(), RunHistoryError> {
      Ok(())
    }
    async fn record_node_dispatched(&self, _: NodeDispatchRecord) -> Result<(), RunHistoryError> {
      Ok(())
    }
    async fn record_node_run(&self, r: NodeRunRecord) -> Result<(), RunHistoryError> {
      self.node_runs.lock().unwrap().push(r);
      Ok(())
    }
    async fn list_pipeline_runs(
      &self,
      _: ListRunsQuery,
    ) -> Result<Vec<PipelineRunRow>, RunHistoryError> {
      Ok(vec![])
    }
    async fn count_pipeline_runs(&self, _: &RunFilters) -> Result<i64, RunHistoryError> {
      Ok(0)
    }
    async fn get_pipeline_run(&self, _: &str) -> Result<Option<PipelineRunRow>, RunHistoryError> {
      Ok(None)
    }
    async fn list_node_runs(&self, _: &str) -> Result<Vec<NodeRunRow>, RunHistoryError> {
      Ok(vec![])
    }
    async fn get_node_run(&self, _: &str, _: &str) -> Result<Option<NodeRunRow>, RunHistoryError> {
      Ok(None)
    }
    async fn get_node_log(&self, _: &str, _: &str) -> Result<Option<String>, RunHistoryError> {
      Ok(None)
    }
    async fn ping(&self) -> Result<(), RunHistoryError> {
      Ok(())
    }
  }

  fn make_run_context() -> RunContext {
    RunContext {
      owner: String::new(),
      repo: String::new(),
      sha: String::new(),
      branch: None,
      target_branch: None,
      tag: None,
      pr_number: None,
      trigger: "push".into(),
      pipeline_yaml: String::new(),
      created_at: Utc::now(),
      retry_of: None,
      tenant_slug: None,
    }
  }

  async fn wait_for_signal_tx(
    notify: Arc<tokio::sync::Notify>,
    slot: Arc<Mutex<Option<SignalSender>>>,
  ) -> SignalSender {
    if slot.lock().unwrap().is_none() {
      notify.notified().await;
    }
    slot.lock().unwrap().clone().expect("signal_tx populated")
  }

  async fn wait_for_dispatch(
    notify: Arc<tokio::sync::Notify>,
    flag: Arc<std::sync::atomic::AtomicBool>,
  ) {
    if !flag.load(std::sync::atomic::Ordering::SeqCst) {
      notify.notified().await;
    }
  }

  fn make_config() -> Arc<AppConfig> {
    Arc::new(AppConfig::load().expect("test config"))
  }

  fn make_pipeline() -> Arc<Pipeline> {
    Arc::new(
      Pipeline::from_yaml(
        r#"
name: test-pipeline
on:
  push:
    branches: [main]
nodes:
  - name: build
    image: rust:latest
    steps:
      - cargo build
"#,
      )
      .unwrap(),
    )
  }

  #[tokio::test]
  async fn test_full_pipeline_run_completes() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_pipeline();

    let dispatcher = Arc::new(AlwaysSuccessDispatcher {
      backplane: backplane.clone(),
    });

    let handle = start_coordinator(
      "test-run-1".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: Arc::new(NoOpRunHistory),
        status_reporter: None,
      },
      RunContext {
        owner: String::new(),
        repo: String::new(),
        sha: String::new(),
        branch: None,
        target_branch: None,
        tag: None,
        pr_number: None,
        trigger: "push".into(),
        pipeline_yaml: String::new(),
        created_at: Utc::now(),
        retry_of: None,
        tenant_slug: None,
      },
    )
    .await
    .expect("Should acquire lease");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Success);
  }

  #[tokio::test]
  async fn infra_failure_marks_node_failed_with_reason() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_pipeline();
    let dispatcher = Arc::new(InjectableDispatcher::new());
    let cancel_calls = dispatcher.cancel_calls();
    let signal_slot = dispatcher.signal_tx_slot();
    let watcher_started = dispatcher.watcher_started_notify();
    let run_history = CapturingRunHistory::new();

    let handle = start_coordinator(
      "test-run-infra".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: run_history.clone(),
        status_reporter: None,
      },
      make_run_context(),
    )
    .await
    .expect("Should acquire lease");

    let signal_tx = wait_for_signal_tx(watcher_started, signal_slot).await;
    signal_tx
      .send(PodSignal::InfraFailure {
        node_name: "build".into(),
        pod_uid: "uid-1".into(),
        reason: InfraFailureReason::ImagePullFailed {
          image: "foo:bar".into(),
          message: "back-off".into(),
        },
      })
      .await
      .expect("send signal");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Failure);
    assert_eq!(summary.node_statuses["build"], NodeStatus::Failed);
    assert!(
      cancel_calls.lock().unwrap().contains(&"build".to_string()),
      "cancel_node should have been called for build"
    );
    let recorded = run_history.node_runs();
    let build_record = recorded
      .iter()
      .find(|r| r.node_name == "build")
      .expect("build node was recorded");
    assert_eq!(build_record.success, false);
    assert_eq!(
      build_record.failure_reason.as_deref(),
      Some("ImagePullFailed")
    );
  }

  fn make_two_node_pipeline() -> Arc<Pipeline> {
    Arc::new(
      Pipeline::from_yaml(
        r#"
name: test-pipeline
on:
  push:
    branches: [main]
fail_fast: false
nodes:
  - name: build
    image: rust:latest
    steps:
      - cargo build
  - name: linger
    image: rust:latest
    steps:
      - sleep 1
"#,
      )
      .unwrap(),
    )
  }

  #[tokio::test]
  async fn late_oom_for_already_completed_node_is_dropped() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_two_node_pipeline();
    let dispatcher = Arc::new(InjectableDispatcher::new());
    let signal_slot = dispatcher.signal_tx_slot();
    let watcher_started = dispatcher.watcher_started_notify();
    let dispatch_called = dispatcher.dispatch_called_notify();
    let dispatch_done = dispatcher.dispatch_done_flag();
    let run_history = CapturingRunHistory::new();
    let backplane_for_test = backplane.clone();

    let handle = start_coordinator(
      "test-run-race".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: run_history.clone(),
        status_reporter: None,
      },
      make_run_context(),
    )
    .await
    .expect("Should acquire lease");

    let signal_tx = wait_for_signal_tx(watcher_started, signal_slot).await;
    wait_for_dispatch(dispatch_called, dispatch_done).await;

    backplane_for_test
      .publish_node_completed("test-run-race", "build", true)
      .await
      .expect("publish completed for build");

    tokio::time::sleep(Duration::from_millis(50)).await;

    signal_tx
      .send(PodSignal::InfraFailure {
        node_name: "build".into(),
        pod_uid: "uid-1".into(),
        reason: InfraFailureReason::OOMKilled,
      })
      .await
      .expect("send late OOM signal");

    tokio::time::sleep(Duration::from_millis(50)).await;

    backplane_for_test
      .publish_node_completed("test-run-race", "linger", true)
      .await
      .expect("publish completed for linger");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Success);
    assert_eq!(summary.node_statuses["build"], NodeStatus::Success);
    assert_eq!(summary.node_statuses["linger"], NodeStatus::Success);
    let recorded = run_history.node_runs();
    let build_record = recorded
      .iter()
      .find(|r| r.node_name == "build")
      .expect("build node was recorded");
    assert!(build_record.success);
    assert!(
      build_record.failure_reason.is_none(),
      "the OOM signal should have been dropped because build was already Success"
    );
  }

  fn make_pipeline_with_timeouts(startup_secs: u64, runtime_secs: u64) -> Arc<Pipeline> {
    Arc::new(
      Pipeline::from_yaml(&format!(
        r#"
name: test-pipeline
on:
  push:
    branches: [main]
nodes:
  - name: build
    image: rust:latest
    timeout_secs: {runtime_secs}
    startup_timeout_secs: {startup_secs}
    steps:
      - cargo build
"#,
      ))
      .unwrap(),
    )
  }

  #[tokio::test]
  async fn startup_timeout_fires_when_pod_never_runs() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_pipeline_with_timeouts(1, 600);
    let dispatcher = Arc::new(InjectableDispatcher::new());
    let cancel_calls = dispatcher.cancel_calls();
    let run_history = CapturingRunHistory::new();

    let handle = start_coordinator(
      "test-run-startup".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: run_history.clone(),
        status_reporter: None,
      },
      make_run_context(),
    )
    .await
    .expect("Should acquire lease");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Failure);
    assert_eq!(summary.node_statuses["build"], NodeStatus::Failed);
    assert!(
      cancel_calls.lock().unwrap().contains(&"build".to_string()),
      "cancel_node should have been called for build"
    );
    let recorded = run_history.node_runs();
    let build_record = recorded
      .iter()
      .find(|r| r.node_name == "build")
      .expect("build node was recorded");
    assert_eq!(
      build_record.failure_reason.as_deref(),
      Some("PodStartTimeout"),
      "expected PodStartTimeout, got {:?}",
      build_record.failure_reason
    );
  }

  #[tokio::test]
  async fn runtime_timeout_starts_fresh_when_pod_enters_running() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_pipeline_with_timeouts(60, 1);
    let dispatcher = Arc::new(InjectableDispatcher::new());
    let signal_slot = dispatcher.signal_tx_slot();
    let watcher_started = dispatcher.watcher_started_notify();
    let dispatch_called = dispatcher.dispatch_called_notify();
    let dispatch_done = dispatcher.dispatch_done_flag();
    let run_history = CapturingRunHistory::new();

    let handle = start_coordinator(
      "test-run-runtime".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: run_history.clone(),
        status_reporter: None,
      },
      make_run_context(),
    )
    .await
    .expect("Should acquire lease");

    let signal_tx = wait_for_signal_tx(watcher_started, signal_slot).await;
    wait_for_dispatch(dispatch_called, dispatch_done).await;

    signal_tx
      .send(PodSignal::PodRunning {
        node_name: "build".into(),
        pod_uid: "uid-1".into(),
      })
      .await
      .expect("send running");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Failure);
    assert_eq!(summary.node_statuses["build"], NodeStatus::Failed);
    let recorded = run_history.node_runs();
    let build_record = recorded
      .iter()
      .find(|r| r.node_name == "build")
      .expect("build node was recorded");
    assert!(
      build_record.failure_reason.is_none(),
      "runtime timeout should not be recorded as an infra-failure reason; got {:?}",
      build_record.failure_reason
    );
    assert_eq!(build_record.success, false);
  }

  #[tokio::test]
  async fn pod_running_after_runtime_already_active_is_idempotent() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_pipeline_with_timeouts(60, 60);
    let dispatcher = Arc::new(InjectableDispatcher::new());
    let signal_slot = dispatcher.signal_tx_slot();
    let watcher_started = dispatcher.watcher_started_notify();
    let dispatch_called = dispatcher.dispatch_called_notify();
    let dispatch_done = dispatcher.dispatch_done_flag();
    let run_history = CapturingRunHistory::new();
    let backplane_for_test = backplane.clone();

    let handle = start_coordinator(
      "test-run-idempotent".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: run_history.clone(),
        status_reporter: None,
      },
      make_run_context(),
    )
    .await
    .expect("Should acquire lease");

    let signal_tx = wait_for_signal_tx(watcher_started, signal_slot).await;
    wait_for_dispatch(dispatch_called, dispatch_done).await;

    signal_tx
      .send(PodSignal::PodRunning {
        node_name: "build".into(),
        pod_uid: "uid-1".into(),
      })
      .await
      .expect("send running 1");
    signal_tx
      .send(PodSignal::PodRunning {
        node_name: "build".into(),
        pod_uid: "uid-1".into(),
      })
      .await
      .expect("send running 2");

    backplane_for_test
      .publish_node_completed("test-run-idempotent", "build", true)
      .await
      .expect("publish completed");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Success);
  }

  struct AlwaysFailDispatcher {
    error_message: String,
  }

  #[async_trait::async_trait]
  impl Dispatcher for AlwaysFailDispatcher {
    async fn dispatch(
      &self,
      _run_id: &str,
      _owner: &str,
      _repo: &str,
      _node: &NodeInfo,
      _pipeline: &Pipeline,
      _config: &AppConfig,
    ) -> Result<(), DispatchError> {
      Err(DispatchError::Kube(self.error_message.clone()))
    }

    async fn cancel_node(
      &self,
      _run_id: &str,
      _node_name: &str,
      _config: &AppConfig,
    ) -> Result<(), DispatchError> {
      Ok(())
    }

    async fn cleanup_run(&self, _run_id: &str) -> Result<(), DispatchError> {
      Ok(())
    }
  }

  #[tokio::test]
  async fn dispatch_failure_records_node_run_with_failure_reason() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_pipeline();
    let dispatcher = Arc::new(AlwaysFailDispatcher {
      error_message:
        "Pod \"build\" is invalid: spec.containers[0].resources.limits[memory]: Invalid value"
          .into(),
    });
    let run_history = CapturingRunHistory::new();

    let handle = start_coordinator(
      "test-run-dispatch-fail".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: run_history.clone(),
        status_reporter: None,
      },
      make_run_context(),
    )
    .await
    .expect("Should acquire lease");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Failure);
    assert_eq!(summary.node_statuses["build"], NodeStatus::Failed);

    let recorded = run_history.node_runs();
    let build_record = recorded
      .iter()
      .find(|r| r.node_name == "build")
      .expect("dispatch failure must persist a node_runs row");
    assert!(!build_record.success);
    assert_eq!(
      build_record.failure_reason.as_deref(),
      Some("DispatchFailed")
    );
    assert!(build_record.completed_at.is_some());
    assert!(
      build_record
        .output_log
        .as_deref()
        .unwrap_or("")
        .contains("Pod \"build\" is invalid"),
      "output_log should embed the underlying dispatch error so the user can see why"
    );
  }

  fn make_pipeline_with_pipeline_timeout(pipeline_timeout_secs: u64) -> Arc<Pipeline> {
    Arc::new(
      Pipeline::from_yaml(&format!(
        r#"
name: test-pipeline
timeout_secs: {pipeline_timeout_secs}
on:
  push:
    branches: [main]
nodes:
  - name: build
    image: rust:latest
    timeout_secs: 600
    startup_timeout_secs: 600
    steps:
      - cargo build
"#,
      ))
      .unwrap(),
    )
  }

  #[tokio::test]
  async fn pipeline_timeout_records_completed_at_for_running_node() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_pipeline_with_pipeline_timeout(1);
    let dispatcher = Arc::new(InjectableDispatcher::new());
    let signal_slot = dispatcher.signal_tx_slot();
    let watcher_started = dispatcher.watcher_started_notify();
    let dispatch_called = dispatcher.dispatch_called_notify();
    let dispatch_done = dispatcher.dispatch_done_flag();
    let run_history = CapturingRunHistory::new();

    let handle = start_coordinator(
      "test-run-pipeline-timeout".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: run_history.clone(),
        status_reporter: None,
      },
      make_run_context(),
    )
    .await
    .expect("Should acquire lease");

    let signal_tx = wait_for_signal_tx(watcher_started, signal_slot).await;
    wait_for_dispatch(dispatch_called, dispatch_done).await;

    signal_tx
      .send(PodSignal::PodRunning {
        node_name: "build".into(),
        pod_uid: "uid-1".into(),
      })
      .await
      .expect("send running");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Failure);
    assert_eq!(summary.node_statuses["build"], NodeStatus::Failed);
    let recorded = run_history.node_runs();
    let build_record = recorded
      .iter()
      .find(|r| r.node_name == "build")
      .expect("build node was recorded");
    assert!(
      build_record.completed_at.is_some(),
      "completed_at should be set for the running node when the pipeline times out"
    );
    assert!(!build_record.success);
  }

  #[tokio::test]
  async fn test_lease_already_held_returns_none() {
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let config = make_config();
    let pipeline = make_pipeline();

    state_store
      .try_acquire_lease("test-run-2", "other-server", 30)
      .await
      .unwrap();

    let dispatcher = Arc::new(AlwaysSuccessDispatcher {
      backplane: backplane.clone(),
    });

    let result = start_coordinator(
      "test-run-2".to_string(),
      pipeline,
      CoordinatorServices {
        config,
        dispatcher,
        state_store,
        backplane,
        run_history: Arc::new(NoOpRunHistory),
        status_reporter: None,
      },
      RunContext {
        owner: String::new(),
        repo: String::new(),
        sha: String::new(),
        branch: None,
        target_branch: None,
        tag: None,
        pr_number: None,
        trigger: "push".into(),
        pipeline_yaml: String::new(),
        created_at: Utc::now(),
        retry_of: None,
        tenant_slug: None,
      },
    )
    .await;

    assert!(result.is_none(), "Should not start when lease already held");
  }
}
