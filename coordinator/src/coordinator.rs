use std::{collections::HashMap, sync::Arc, time::Duration};

use app_config::AppConfig;
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
}

pub struct CoordinatorServices {
  pub config: Arc<AppConfig>,
  pub dispatcher: Arc<dyn Dispatcher>,
  pub state_store: Arc<dyn StateStore>,
  pub backplane: Arc<dyn Backplane>,
  pub run_history: Arc<dyn RunHistory>,
}

pub struct RunSummary {
  pub run_id: String,
  pub status: RunStatus,
  pub node_statuses: HashMap<String, NodeStatus>,
}

struct Coordinator {
  run_id: String,
  run: PipelineRun,
  pipeline: Arc<Pipeline>,
  config: Arc<AppConfig>,
  node_info_cache: HashMap<String, NodeInfo>,
  node_timeout_handles: HashMap<String, JoinHandle<()>>,
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
    node_timeout_handles: HashMap::new(),
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
      self.record_history(status).await;
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
          self.record_history(RunStatus::Failure).await;
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
                self.record_history(RunStatus::Failure).await;
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
              self.handle_node_infra_failed(&node_name, &reason).await;
            }
            Some(CoordinatorMessage::NodePodStartTimedOut { node_name }) => {
              self.handle_pod_start_timed_out(&node_name).await;
            }
            None => {}
          }
        }
        event = subscription.next_event() => {
          match event {
            Some(BackplaneEvent::NodeCompleted { node_name, success }) => {
              self.cancel_node_timeout(&node_name);
              if self.handle_node_completed(&node_name, success).await {
                self.record_history(RunStatus::Failure).await;
                self.cleanup().await;
                return self.terminate_running_nodes(RunStatus::Failure).await;
              }
              if self.run.is_complete() {
                break;
              }
            }
            Some(BackplaneEvent::Cancel) => {
              info!(run_id = %self.run_id, "Received Cancel event from backplane");
              self.record_history(RunStatus::Cancelled).await;
              self.cleanup().await;
              return self.terminate_running_nodes(RunStatus::Cancelled).await;
            }
            None => {
              warn!(run_id = %self.run_id, "Backplane subscription closed unexpectedly");
              self.record_history(RunStatus::Failure).await;
              self.cleanup().await;
              return self.terminate_running_nodes(RunStatus::Failure).await;
            }
          }
        }
      }
    }

    let status = self.outcome_status();
    self.record_history(status).await;
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
    if let Some(handle) = self.node_timeout_handles.remove(node_name) {
      handle.abort();
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
    info!(
      run_id = %self.run_id,
      node_name,
      "Pod main container is running"
    );
  }

  async fn handle_node_infra_failed(&mut self, node_name: &str, reason: &InfraFailureReason) {
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
      return;
    }
    error!(
      run_id = %self.run_id,
      node_name,
      stable_code = reason.stable_code(),
      message = %reason.full_message(),
      user_actionable = reason.user_message().is_some(),
      "Infrastructure failure detected"
    );
  }

  async fn handle_pod_start_timed_out(&mut self, node_name: &str) {
    warn!(
      run_id = %self.run_id,
      node_name,
      "Pod did not enter Running phase within startup timeout"
    );
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
      self.record_node_completed(node_name).await;
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
      self.record_node_completed(node_name).await;
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
    self.record_node_completed(node_name).await;
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
      let Some(node) = self.node_info_cache.get(&node_name) else {
        error!(run_id = %self.run_id, node_name = %node_name, "Node info not found in pipeline");
        self.run.mark_dispatch_failed(&node_name);
        continue;
      };

      match self
        .dispatcher
        .dispatch(&self.run_id, node, &self.pipeline, &self.config)
        .await
      {
        Ok(()) => {
          if !self.run.mark_running(&node_name) {
            warn!(run_id = %self.run_id, node_name = %node_name, "Unexpected state transition: node was not Pending");
          } else {
            info!(run_id = %self.run_id, node_name = %node_name, "Node dispatched");
            self.record_node_dispatched(&node_name, node).await;
            let timeout_handle = self.spawn_node_timeout(&node_name, node.timeout_secs);
            self.node_timeout_handles.insert(node_name, timeout_handle);
          }
        }
        Err(e) => {
          error!(run_id = %self.run_id, node_name = %node_name, error = %e, "Failed to dispatch node");
          self.run.mark_dispatch_failed(&node_name);
        }
      }
    }
  }

  fn spawn_node_timeout(&self, node_name: &str, override_secs: Option<u64>) -> JoinHandle<()> {
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

    for (_, handle) in self.node_timeout_handles.drain() {
      handle.abort();
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

  async fn record_node_completed(&self, node_name: &str) {
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
      .and_then(ms_to_datetime);

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
    };
    if let Err(e) = self.run_history.record_node_run(node_record).await {
      warn!(run_id = %self.run_id, node_name, error = %e, "Failed to record node run history");
    }
  }

  async fn record_history(&self, status: RunStatus) {
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
    };
    if let Err(e) = self.run_history.record_pipeline_run(pipeline_record).await {
      warn!(run_id = %self.run_id, error = %e, "Failed to record pipeline run history");
    }

    let running_nodes: Vec<String> = self
      .run
      .statuses()
      .iter()
      .filter(|(_, s)| **s == NodeStatus::Running)
      .map(|(name, _)| name.clone())
      .collect();
    for node_name in running_nodes {
      self.record_running_node_terminated(&node_name).await;
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
  use std::sync::Arc;

  use backplane::InMemoryBackplane;
  use chrono::Utc;
  use run_history::NoOpRunHistory;
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
      },
    )
    .await
    .expect("Should acquire lease");

    let summary = handle.await.expect("Coordinator should complete");
    assert_eq!(summary.status, RunStatus::Success);
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
      },
    )
    .await;

    assert!(result.is_none(), "Should not start when lease already held");
  }
}
