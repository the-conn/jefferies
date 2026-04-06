use std::{collections::HashMap, sync::Arc, time::Duration};

use app_config::AppConfig;
use backplane::{Backplane, BackplaneEvent};
use pipelines::{NodeInfo, Pipeline};
use state_store::{NodeStatus, StateStore, StateStoreError};
use tokio::{sync::mpsc, task::JoinHandle, time::interval};
use tracing::{error, info, warn};

use crate::{dispatcher::Dispatcher, message::CoordinatorMessage, run::PipelineRun};

pub struct RunSummary {
  pub run_id: String,
  pub success: bool,
  pub cancelled: bool,
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
}

pub async fn start_coordinator(
  run_id: String,
  pipeline: Arc<Pipeline>,
  config: Arc<AppConfig>,
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  backplane: Arc<dyn Backplane>,
) -> Option<JoinHandle<RunSummary>> {
  let server_id = uuid::Uuid::new_v4().to_string();

  let lease_version = match state_store.try_acquire_lease(&run_id, &server_id, 30).await {
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

  let (run, state_version) =
    initialize_run_state(&run_id, &node_infos, &pipeline, state_store.as_ref()).await;

  let (internal_tx, internal_rx) = mpsc::channel(128);

  let coordinator = Coordinator {
    run_id,
    run,
    pipeline,
    config,
    node_info_cache,
    node_timeout_handles: HashMap::new(),
    internal_tx,
    internal_rx,
    dispatcher,
    state_store,
    backplane,
    state_version,
    lease_version,
    server_id,
  };

  Some(tokio::spawn(coordinator.run()))
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

impl Coordinator {
  async fn run(mut self) -> RunSummary {
    info!(run_id = %self.run_id, server_id = %self.server_id, "Coordinator starting");

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
        return self.build_summary(true);
      }
    };

    let mut heartbeat = interval(Duration::from_secs(15));
    heartbeat.tick().await;

    self.dispatch_ready_nodes().await;

    if self.run.is_complete() {
      self.cleanup().await;
      return self.build_summary(false);
    }

    loop {
      tokio::select! {
        _ = &mut pipeline_deadline => {
          warn!(
            run_id = %self.run_id,
            timeout_secs = pipeline_timeout_secs,
            "Pipeline timeout exceeded"
          );
          self.cleanup().await;
          return self.handle_cancellation().await;
        }
        _ = heartbeat.tick() => {
          let renewed = self.state_store
            .renew_lease(&self.run_id, &self.server_id, self.lease_version, 30)
            .await
            .unwrap_or(false);
          if !renewed {
            warn!(run_id = %self.run_id, "Lease renewal failed; another server took over");
            return self.build_summary(true);
          }
        }
        msg = self.internal_rx.recv() => {
          match msg {
            Some(CoordinatorMessage::NodeTimedOut { node_name }) => {
              if self.handle_node_timed_out(&node_name).await {
                self.cleanup().await;
                return self.handle_cancellation().await;
              }
              if self.run.is_complete() {
                break;
              }
            }
            _ => {}
          }
        }
        event = subscription.next_event() => {
          match event {
            Some(BackplaneEvent::NodeCompleted { node_name, success }) => {
              self.cancel_node_timeout(&node_name);
              if self.handle_node_completed(&node_name, success).await {
                self.cleanup().await;
                return self.handle_cancellation().await;
              }
              if self.run.is_complete() {
                break;
              }
            }
            Some(BackplaneEvent::Cancel) => {
              info!(run_id = %self.run_id, "Received Cancel event from backplane");
              self.cleanup().await;
              return self.handle_cancellation().await;
            }
            None => {
              warn!(run_id = %self.run_id, "Backplane subscription closed unexpectedly");
              self.cleanup().await;
              return self.handle_cancellation().await;
            }
          }
        }
      }
    }

    self.cleanup().await;
    self.build_summary(false)
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
    if let Err(e) = self
      .dispatcher
      .cancel_node(&self.run_id, node_name, &self.config)
      .await
    {
      warn!(run_id = %self.run_id, node_name, error = %e, "Failed to cancel timed-out node");
    }
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

  async fn handle_cancellation(mut self) -> RunSummary {
    info!(run_id = %self.run_id, "Coordinator handling cancellation");

    for (_, handle) in self.node_timeout_handles.drain() {
      handle.abort();
    }

    for (node_name, status) in self.run.statuses() {
      if *status == NodeStatus::Running {
        if let Err(e) = self
          .dispatcher
          .cancel_node(&self.run_id, node_name, &self.config)
          .await
        {
          warn!(run_id = %self.run_id, node_name, error = %e, "Failed to cancel node");
        }
      }
    }

    let node_statuses = self.run.statuses().clone();
    RunSummary {
      run_id: self.run_id,
      success: false,
      cancelled: true,
      node_statuses,
    }
  }

  async fn cleanup(&self) {
    if let Err(e) = self.state_store.release_lease(&self.run_id).await {
      warn!(run_id = %self.run_id, error = %e, "Failed to release lease");
    }
    if let Err(e) = self.state_store.delete_run(&self.run_id).await {
      warn!(run_id = %self.run_id, error = %e, "Failed to delete run state");
    }
  }

  fn build_summary(&self, cancelled: bool) -> RunSummary {
    let success = !cancelled
      && self
        .run
        .statuses()
        .values()
        .all(|s| *s == NodeStatus::Success);
    info!(
      run_id = %self.run_id,
      success,
      cancelled,
      "Coordinator completed"
    );
    RunSummary {
      run_id: self.run_id.clone(),
      success,
      cancelled,
      node_statuses: self.run.statuses().clone(),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use backplane::InMemoryBackplane;
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
  }

  fn make_config() -> Arc<AppConfig> {
    unsafe {
      std::env::set_var("JEFFERIES__SERVER__HOST", "127.0.0.1");
      std::env::set_var("JEFFERIES__SERVER__PORT", "3000");
      std::env::set_var("JEFFERIES__LOG__LEVEL", "info");
      std::env::set_var("JEFFERIES__GITHUB__APP_ID", "test");
      std::env::set_var("JEFFERIES__GITHUB__WEBHOOK_SECRET", "test");
      std::env::set_var("JEFFERIES__GITHUB__CLIENT_ID", "test");
      std::env::set_var("JEFFERIES__GITHUB__CLIENT_SECRET", "test");
      std::env::set_var("JEFFERIES__GITHUB__PRIVATE_KEY", "test");
      std::env::set_var("JEFFERIES__PIPELINE__DEFAULT_PIPELINE_TIMEOUT_SECS", "300");
      std::env::set_var("JEFFERIES__PIPELINE__DEFAULT_NODE_TIMEOUT_SECS", "60");
      std::env::set_var("JEFFERIES__PIPELINE__FAIL_FAST", "false");
      std::env::set_var("JEFFERIES__REDIS__URL", "redis://localhost:6379");
      std::env::set_var("JEFFERIES__REDIS__PASSWORD", "");
      std::env::set_var(
        "JEFFERIES__RABBITMQ__URL",
        "amqp://guest:guest@localhost:5672",
      );
      std::env::set_var("JEFFERIES__RABBITMQ__POOL__MAX_SIZE", "16");
    }
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
      config,
      dispatcher,
      state_store,
      backplane,
    )
    .await
    .expect("Should acquire lease");

    let summary = handle.await.expect("Coordinator should complete");
    assert!(summary.success);
    assert!(!summary.cancelled);
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
      config,
      dispatcher,
      state_store,
      backplane,
    )
    .await;

    assert!(result.is_none(), "Should not start when lease already held");
  }
}
