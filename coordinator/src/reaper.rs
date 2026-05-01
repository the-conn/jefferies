use std::{sync::Arc, time::Duration};

use app_config::AppConfig;
use backplane::Backplane;
use chrono::Utc;
use run_history::NoOpRunHistory;
use state_store::StateStore;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::{CoordinatorServices, Dispatcher, RunContext, start_coordinator};

const LEASE_RECLAIM_INTERVAL_SECS: u64 = 60;
const RESOURCE_SWEEP_INTERVAL_SECS: u64 = 300;

pub fn start_reaper(
  config: Arc<AppConfig>,
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  backplane: Arc<dyn Backplane>,
) -> JoinHandle<()> {
  tokio::spawn(async move {
    let mut lease_tick = tokio::time::interval(Duration::from_secs(LEASE_RECLAIM_INTERVAL_SECS));
    let mut sweep_tick = tokio::time::interval(Duration::from_secs(RESOURCE_SWEEP_INTERVAL_SECS));
    lease_tick.tick().await;
    sweep_tick.tick().await;
    loop {
      tokio::select! {
        _ = lease_tick.tick() => {
          reclaim_orphaned_runs(
            config.clone(),
            dispatcher.clone(),
            state_store.clone(),
            backplane.clone(),
          )
          .await;
        }
        _ = sweep_tick.tick() => {
          sweep_stranded_resources(dispatcher.clone(), state_store.clone()).await;
        }
      }
    }
  })
}

async fn reclaim_orphaned_runs(
  config: Arc<AppConfig>,
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  backplane: Arc<dyn Backplane>,
) {
  let orphaned = match state_store.get_orphaned_runs().await {
    Ok(runs) => runs,
    Err(e) => {
      warn!(error = %e, "Failed to get orphaned runs");
      return;
    }
  };

  for run_id in orphaned {
    info!(run_id, "Reclaiming orphaned run");

    let run_state = match state_store.load_run(&run_id).await {
      Ok(Some(state)) => state,
      Ok(None) => {
        warn!(run_id, "Orphaned run state disappeared before reclaim");
        continue;
      }
      Err(e) => {
        error!(run_id, error = %e, "Failed to load orphaned run state");
        continue;
      }
    };

    let pipeline = std::sync::Arc::new(run_state.pipeline.clone());

    match start_coordinator(
      run_id.clone(),
      pipeline,
      CoordinatorServices {
        config: config.clone(),
        dispatcher: dispatcher.clone(),
        state_store: state_store.clone(),
        backplane: backplane.clone(),
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
        trigger: String::new(),
        pipeline_yaml: String::new(),
        created_at: Utc::now(),
        retry_of: None,
      },
    )
    .await
    {
      Some(handle) => {
        let monitor_run_id = run_id.clone();
        tokio::spawn(async move {
          match handle.await {
            Ok(summary) => {
              info!(
                run_id = %monitor_run_id,
                status = %summary.status,
                "Reclaimed run completed"
              );
            }
            Err(e) => {
              warn!(run_id = %monitor_run_id, error = %e, "Reclaimed coordinator task panicked");
            }
          }
        });
      }
      None => {
        info!(run_id, "Another server acquired lease for orphaned run");
      }
    }
  }
}

async fn sweep_stranded_resources(
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
) {
  sweep_terminal_redis_runs(dispatcher.clone(), state_store.clone()).await;
  sweep_orphan_kube_resources(dispatcher, state_store).await;
}

async fn sweep_terminal_redis_runs(
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
) {
  let terminal = match state_store.list_terminal_unleased_runs().await {
    Ok(runs) => runs,
    Err(e) => {
      warn!(error = %e, "Failed to list terminal unleased runs");
      return;
    }
  };

  for run_id in terminal {
    info!(run_id, "Reaping terminal unleased run");
    if let Err(e) = dispatcher.cleanup_run(&run_id).await {
      warn!(run_id, error = %e, "Failed to cleanup dispatcher resources for terminal run");
    }
    if let Err(e) = state_store.release_lease(&run_id).await {
      warn!(run_id, error = %e, "Failed to release lease for terminal run");
    }
    if let Err(e) = state_store.delete_run(&run_id).await {
      warn!(run_id, error = %e, "Failed to delete state for terminal run");
    }
  }
}

async fn sweep_orphan_kube_resources(
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
) {
  let managed = match dispatcher.list_managed_run_ids().await {
    Ok(ids) => ids,
    Err(e) => {
      warn!(error = %e, "Failed to list managed run ids");
      return;
    }
  };

  for run_id in managed {
    let load_result = match state_store.load_run(&run_id).await {
      Ok(state) => state,
      Err(e) => {
        warn!(run_id, error = %e, "Failed to load state while sweeping kube resources");
        continue;
      }
    };
    if load_result.is_some() {
      continue;
    }
    info!(
      run_id,
      "Reaping orphan Kubernetes resources for completed run"
    );
    if let Err(e) = dispatcher.cleanup_run(&run_id).await {
      warn!(run_id, error = %e, "Failed to cleanup orphan Kubernetes resources");
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{Arc, Mutex};

  use app_config::AppConfig;
  use async_trait::async_trait;
  use pipelines::{NodeInfo, Pipeline};
  use state_store::{InMemoryStateStore, NodeStatus, RunState};

  use super::*;
  use crate::dispatcher::{DispatchError, Dispatcher};

  struct TrackingDispatcher {
    managed_run_ids: Mutex<Vec<String>>,
    cleanup_calls: Mutex<Vec<String>>,
  }

  impl TrackingDispatcher {
    fn new(managed: Vec<String>) -> Self {
      Self {
        managed_run_ids: Mutex::new(managed),
        cleanup_calls: Mutex::new(Vec::new()),
      }
    }
  }

  #[async_trait]
  impl Dispatcher for TrackingDispatcher {
    async fn dispatch(
      &self,
      _: &str,
      _: &NodeInfo,
      _: &Pipeline,
      _: &AppConfig,
    ) -> Result<(), DispatchError> {
      Ok(())
    }
    async fn cancel_node(&self, _: &str, _: &str, _: &AppConfig) -> Result<(), DispatchError> {
      Ok(())
    }
    async fn cleanup_run(&self, run_id: &str) -> Result<(), DispatchError> {
      self.cleanup_calls.lock().unwrap().push(run_id.to_string());
      Ok(())
    }
    async fn list_managed_run_ids(&self) -> Result<Vec<String>, DispatchError> {
      Ok(self.managed_run_ids.lock().unwrap().clone())
    }
  }

  fn make_pipeline() -> Pipeline {
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
    .unwrap()
  }

  fn make_state(status: NodeStatus) -> RunState {
    let mut statuses = std::collections::HashMap::new();
    statuses.insert("build".to_string(), status);
    let mut deps = std::collections::HashMap::new();
    deps.insert("build".to_string(), vec![]);
    RunState {
      version: 0,
      statuses,
      dependencies: deps,
      pipeline: make_pipeline(),
    }
  }

  #[tokio::test]
  async fn sweep_terminal_redis_runs_cleans_up_completed_runs() {
    let state_store = InMemoryStateStore::new();
    state_store
      .save_run("done-run", &make_state(NodeStatus::Success), 0)
      .await
      .unwrap();
    state_store
      .save_run("active-run", &make_state(NodeStatus::Running), 0)
      .await
      .unwrap();

    let dispatcher = Arc::new(TrackingDispatcher::new(vec![]));
    sweep_terminal_redis_runs(dispatcher.clone(), state_store.clone()).await;

    let calls = dispatcher.cleanup_calls.lock().unwrap().clone();
    assert_eq!(calls, vec!["done-run".to_string()]);
    assert!(state_store.load_run("done-run").await.unwrap().is_none());
    assert!(state_store.load_run("active-run").await.unwrap().is_some());
  }

  #[tokio::test]
  async fn sweep_orphan_kube_resources_cleans_runs_without_redis_state() {
    let state_store = InMemoryStateStore::new();
    state_store
      .save_run("active-run", &make_state(NodeStatus::Running), 0)
      .await
      .unwrap();

    let dispatcher = Arc::new(TrackingDispatcher::new(vec![
      "stranded-run".to_string(),
      "active-run".to_string(),
    ]));
    sweep_orphan_kube_resources(dispatcher.clone(), state_store.clone()).await;

    let calls = dispatcher.cleanup_calls.lock().unwrap().clone();
    assert_eq!(calls, vec!["stranded-run".to_string()]);
  }
}
