use std::{sync::Arc, time::Duration};

use app_config::AppConfig;
use backplane::Backplane;
use chrono::{DateTime, Utc};
use pipelines::Pipeline;
use run_history::{
  NodeRunRecord, NodeRunRow, PipelineRunRow, RunHistory, RunHistoryError, RunStatus,
};
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
  run_history: Arc<dyn RunHistory>,
) -> JoinHandle<()> {
  tokio::spawn(async move {
    info!("Reaper: eager startup sweep beginning");
    reclaim_orphaned_runs(
      config.clone(),
      dispatcher.clone(),
      state_store.clone(),
      backplane.clone(),
      run_history.clone(),
    )
    .await;
    sweep_stranded_resources(
      config.clone(),
      dispatcher.clone(),
      state_store.clone(),
      backplane.clone(),
      run_history.clone(),
    )
    .await;
    info!("Reaper: eager startup sweep complete; entering periodic loop");

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
            run_history.clone(),
          )
          .await;
        }
        _ = sweep_tick.tick() => {
          sweep_stranded_resources(
            config.clone(),
            dispatcher.clone(),
            state_store.clone(),
            backplane.clone(),
            run_history.clone(),
          )
          .await;
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
  run_history: Arc<dyn RunHistory>,
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

    if state_store.load_run(&run_id).await.ok().flatten().is_none() {
      warn!(run_id, "Orphaned run state disappeared before reclaim");
      continue;
    }

    let pipeline_row = match run_history.get_pipeline_run(&run_id).await {
      Ok(Some(row)) => row,
      Ok(None) => {
        error!(
          run_id,
          "No pipeline_runs row in Postgres for orphaned run; finalizing as Failure"
        );
        finalize_unrecoverable_orphan(&run_id, &dispatcher, &state_store).await;
        continue;
      }
      Err(e) => {
        error!(run_id, error = %e, "Failed to query pipeline_runs for orphaned run; skipping");
        continue;
      }
    };

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
    let run_context = run_context_from_row(pipeline_row);

    match start_coordinator(
      run_id.clone(),
      pipeline,
      CoordinatorServices {
        config: config.clone(),
        dispatcher: dispatcher.clone(),
        state_store: state_store.clone(),
        backplane: backplane.clone(),
        run_history: run_history.clone(),
        status_reporter: None,
      },
      run_context,
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

fn run_context_from_row(row: PipelineRunRow) -> RunContext {
  RunContext {
    owner: row.owner,
    repo: row.repo,
    sha: row.sha,
    branch: row.branch,
    target_branch: row.target_branch,
    tag: row.tag,
    pr_number: row.pr_number,
    trigger: row.trigger,
    pipeline_yaml: row.pipeline_definition,
    created_at: row.created_at,
    retry_of: row.retry_of.map(|u| u.to_string()),
    tenant_slug: row.tenant_slug,
  }
}

async fn finalize_unrecoverable_orphan(
  run_id: &str,
  dispatcher: &Arc<dyn Dispatcher>,
  state_store: &Arc<dyn StateStore>,
) {
  if let Err(e) = dispatcher.cleanup_run(run_id).await {
    warn!(run_id, error = %e, "Failed to cleanup dispatcher resources for unrecoverable orphan");
  }
  if let Err(e) = state_store.release_lease(run_id).await {
    warn!(run_id, error = %e, "Failed to release lease for unrecoverable orphan");
  }
  if let Err(e) = state_store.delete_run(run_id).await {
    warn!(run_id, error = %e, "Failed to delete state for unrecoverable orphan");
  }
}

async fn sweep_stranded_resources(
  config: Arc<AppConfig>,
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  backplane: Arc<dyn Backplane>,
  run_history: Arc<dyn RunHistory>,
) {
  sweep_terminal_redis_runs(dispatcher.clone(), state_store.clone(), run_history.clone()).await;
  sweep_postgres_in_progress_runs(
    config,
    dispatcher.clone(),
    state_store.clone(),
    backplane,
    run_history,
  )
  .await;
  sweep_orphan_kube_resources(dispatcher, state_store).await;
}

async fn sweep_postgres_in_progress_runs(
  config: Arc<AppConfig>,
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  backplane: Arc<dyn Backplane>,
  run_history: Arc<dyn RunHistory>,
) {
  let in_progress = match run_history.list_in_progress_runs().await {
    Ok(rows) => rows,
    Err(e) => {
      warn!(error = %e, "Failed to list in_progress pipeline_runs for reconcile");
      return;
    }
  };
  info!(
    total = in_progress.len(),
    "Postgres-driven sweep: examining in_progress pipeline_runs rows"
  );
  let mut stats = SweepStats::default();
  for row in in_progress {
    let run_id = row.run_id.to_string();
    match state_store.load_run(&run_id).await {
      Ok(Some(state)) => {
        warn!(
          run_id,
          statuses = ?state.statuses,
          "in_progress pipeline_runs row has Redis state; postgres sweep deferring to reclaim/terminal sweeps. \
           If neither path picks it up, inspect the lease key in Redis directly."
        );
        stats.skipped_redis_state_present += 1;
        continue;
      }
      Ok(None) => {
        info!(
          run_id,
          "in_progress pipeline_runs row has no Redis state; entering reconcile_postgres_in_progress_run"
        );
      }
      Err(e) => {
        warn!(run_id, error = %e, "Failed to check Redis state for in_progress run");
        stats.errors += 1;
        continue;
      }
    }
    match reconcile_postgres_in_progress_run(
      &run_id,
      &row,
      config.clone(),
      dispatcher.clone(),
      state_store.clone(),
      backplane.clone(),
      run_history.clone(),
    )
    .await
    {
      Ok(()) => stats.reconciled += 1,
      Err(e) => {
        warn!(run_id, error = %e, "Failed to reconcile in_progress pipeline_runs row");
        stats.errors += 1;
      }
    }
  }
  info!(?stats, "Postgres-driven sweep: done");
}

#[derive(Default, Debug)]
struct SweepStats {
  reconciled: usize,
  skipped_redis_state_present: usize,
  errors: usize,
}

async fn reconcile_postgres_in_progress_run(
  run_id: &str,
  row: &PipelineRunRow,
  config: Arc<AppConfig>,
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  backplane: Arc<dyn Backplane>,
  run_history: Arc<dyn RunHistory>,
) -> Result<(), RunHistoryError> {
  let pipeline = match Pipeline::from_yaml(&row.pipeline_definition) {
    Ok(p) => p,
    Err(e) => {
      warn!(
        run_id,
        error = %e,
        "Failed to parse pipeline_definition; cannot reconcile this run"
      );
      return Ok(());
    }
  };
  let expected: Vec<String> = pipeline
    .node_info()
    .iter()
    .map(|n| n.name.clone())
    .collect();

  let node_runs = run_history.list_node_runs(run_id).await?;
  let mut by_name: std::collections::HashMap<String, NodeRunRow> = node_runs
    .into_iter()
    .map(|nr| (nr.node_name.clone(), nr))
    .collect();

  for node_name in &expected {
    let needs_s3_check = match by_name.get(node_name) {
      Some(nr) => nr.success.is_none(),
      None => false,
    };
    if !needs_s3_check {
      continue;
    }
    let outcome = match dispatcher.get_node_outcome(run_id, node_name).await {
      Ok(Some(o)) => o,
      Ok(None) => continue,
      Err(e) => {
        warn!(run_id, node_name, error = %e, "S3 read failed during Postgres reconcile");
        continue;
      }
    };
    let (Some(success), Some(finished_ms)) = (outcome.success, outcome.finished_at) else {
      continue;
    };
    let existing = by_name.get(node_name).expect("present per match above");
    let started_at = outcome.started_at.and_then(ms_to_datetime);
    let completed_at = ms_to_datetime(finished_ms);
    let record = NodeRunRecord {
      run_id: run_id.to_string(),
      node_name: node_name.clone(),
      node_definition: existing.node_definition.clone(),
      success,
      created_at: existing.created_at,
      started_at,
      completed_at,
      output_log: None,
      failure_reason: None,
    };
    if let Err(e) = run_history.record_node_run(record).await {
      warn!(run_id, node_name, error = %e, "Failed to update node_runs from S3");
      continue;
    }
    if let Some(nr) = by_name.get_mut(node_name) {
      nr.success = Some(success);
      nr.started_at = started_at;
      nr.completed_at = completed_at;
    }
    info!(
      run_id,
      node_name, success, "Reconciled node_runs success from S3 status.json"
    );
  }

  let all_known = expected
    .iter()
    .all(|n| by_name.get(n).and_then(|nr| nr.success).is_some());

  if all_known {
    let all_success = expected
      .iter()
      .all(|n| by_name.get(n).and_then(|nr| nr.success) == Some(true));
    let status = if all_success {
      RunStatus::Success
    } else {
      RunStatus::Failure
    };
    match run_history
      .finalize_pipeline_run_status(run_id, status, Utc::now())
      .await
    {
      Ok(true) => info!(
        run_id,
        %status,
        "All expected nodes terminal in node_runs; finalizing pipeline_runs.status"
      ),
      Ok(false) => {}
      Err(e) => warn!(run_id, error = %e, "Failed to write reconciled status to Postgres"),
    }
    if let Err(e) = dispatcher.cleanup_run(run_id).await {
      warn!(run_id, error = %e, "Failed to cleanup dispatcher resources for reconciled run");
    }
    return Ok(());
  }

  let reconstructed = reconstruct_run_state_from_postgres(&pipeline, &by_name);
  info!(
    run_id,
    "Reconstructing RunState from pipeline_definition + node_runs and resuming execution"
  );
  match state_store.save_run(run_id, &reconstructed, 0).await {
    Ok(true) => {}
    Ok(false) => {
      info!(
        run_id,
        "Reconstructed RunState rejected by state store (someone else got there first); skipping"
      );
      return Ok(());
    }
    Err(e) => {
      warn!(run_id, error = %e, "Failed to save reconstructed RunState; skipping");
      return Ok(());
    }
  }

  let run_context = run_context_from_row(row.clone());
  let services = CoordinatorServices {
    config,
    dispatcher,
    state_store,
    backplane,
    run_history,
    status_reporter: None,
  };
  let pipeline_arc = Arc::new(pipeline);
  match start_coordinator(run_id.to_string(), pipeline_arc, services, run_context).await {
    Some(handle) => {
      let monitor_run_id = run_id.to_string();
      tokio::spawn(async move {
        match handle.await {
          Ok(summary) => info!(
            run_id = %monitor_run_id,
            status = %summary.status,
            "Resumed pipeline (Postgres-driven) completed"
          ),
          Err(e) => warn!(
            run_id = %monitor_run_id,
            error = %e,
            "Resumed coordinator task panicked"
          ),
        }
      });
    }
    None => info!(
      run_id,
      "Another server acquired lease for resumed pipeline; deferring"
    ),
  }
  Ok(())
}

fn reconstruct_run_state_from_postgres(
  pipeline: &Pipeline,
  node_runs_by_name: &std::collections::HashMap<String, NodeRunRow>,
) -> state_store::RunState {
  let nodes = pipeline.node_info();
  let mut statuses = std::collections::HashMap::new();
  let mut dependencies = std::collections::HashMap::new();
  for node in &nodes {
    let status = match node_runs_by_name.get(&node.name) {
      None => state_store::NodeStatus::Pending,
      Some(nr) => match nr.success {
        Some(true) => state_store::NodeStatus::Success,
        Some(false) => state_store::NodeStatus::Failed,
        None => state_store::NodeStatus::Running,
      },
    };
    statuses.insert(node.name.clone(), status);
    dependencies.insert(node.name.clone(), node.dependencies.clone());
  }
  state_store::RunState {
    version: 0,
    statuses,
    dependencies,
    pipeline: pipeline.clone(),
  }
}

fn ms_to_datetime(ms: u128) -> Option<DateTime<Utc>> {
  let secs = i64::try_from(ms / 1000).ok()?;
  let nanos = u32::try_from((ms % 1000) * 1_000_000).ok()?;
  DateTime::from_timestamp(secs, nanos)
}

fn outcome_status_for(state: &state_store::RunState) -> RunStatus {
  if state
    .statuses
    .values()
    .all(|s| *s == state_store::NodeStatus::Success)
  {
    RunStatus::Success
  } else {
    RunStatus::Failure
  }
}

async fn sweep_terminal_redis_runs(
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  run_history: Arc<dyn RunHistory>,
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
    let outcome = match state_store.load_run(&run_id).await {
      Ok(Some(state)) => Some(outcome_status_for(&state)),
      Ok(None) => None,
      Err(e) => {
        warn!(run_id, error = %e, "Failed to load state for terminal run; deferring");
        continue;
      }
    };
    if let Some(status) = outcome {
      match run_history
        .finalize_pipeline_run_status(&run_id, status, chrono::Utc::now())
        .await
      {
        Ok(true) => info!(
          run_id,
          %status,
          "Reconciled abandoned-but-terminal run's pipeline_runs.status to Postgres"
        ),
        Ok(false) => {}
        Err(e) => {
          warn!(
            run_id,
            error = %e,
            "Failed to reconcile pipeline_runs.status for terminal run; deferring cleanup"
          );
          continue;
        }
      }
    }
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
    let state_unloadable = match state_store.load_run(&run_id).await {
      Ok(Some(_)) => continue,
      Ok(None) => false,
      Err(e) => {
        warn!(
          run_id,
          error = %e,
          "State is unloadable; treating as orphan and reaping kube resources + corrupt state"
        );
        true
      }
    };
    info!(
      run_id,
      "Reaping orphan Kubernetes resources for completed run"
    );
    if let Err(e) = dispatcher.cleanup_run(&run_id).await {
      warn!(run_id, error = %e, "Failed to cleanup orphan Kubernetes resources");
    }
    if state_unloadable && let Err(e) = state_store.delete_run(&run_id).await {
      warn!(run_id, error = %e, "Failed to delete corrupt run state");
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
  use crate::dispatcher::{DispatchError, Dispatcher, RunMetadata};

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
      _: &RunMetadata,
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
    sweep_terminal_redis_runs(
      dispatcher.clone(),
      state_store.clone(),
      Arc::new(run_history::NoOpRunHistory),
    )
    .await;

    let calls = dispatcher.cleanup_calls.lock().unwrap().clone();
    assert_eq!(calls, vec!["done-run".to_string()]);
    assert!(state_store.load_run("done-run").await.unwrap().is_none());
    assert!(state_store.load_run("active-run").await.unwrap().is_some());
  }

  #[derive(Default)]
  struct CapturingRunHistory {
    finalize_calls: Mutex<Vec<(String, RunStatus)>>,
    pipeline_row: Mutex<Option<PipelineRunRow>>,
    in_progress_rows: Mutex<Vec<PipelineRunRow>>,
    node_runs_by_run: Mutex<std::collections::HashMap<String, Vec<NodeRunRow>>>,
    recorded_node_runs: Mutex<Vec<NodeRunRecord>>,
  }

  impl CapturingRunHistory {
    fn with_row(row: Option<PipelineRunRow>) -> Arc<Self> {
      Arc::new(Self {
        finalize_calls: Mutex::new(Vec::new()),
        pipeline_row: Mutex::new(row),
        ..Default::default()
      })
    }

    fn with_in_progress(
      rows: Vec<PipelineRunRow>,
      node_runs: std::collections::HashMap<String, Vec<NodeRunRow>>,
    ) -> Arc<Self> {
      Arc::new(Self {
        in_progress_rows: Mutex::new(rows),
        node_runs_by_run: Mutex::new(node_runs),
        ..Default::default()
      })
    }

    fn finalize_calls(&self) -> Vec<(String, RunStatus)> {
      self.finalize_calls.lock().unwrap().clone()
    }
  }

  #[async_trait]
  impl RunHistory for CapturingRunHistory {
    async fn record_pipeline_started(
      &self,
      _: run_history::PipelineStartRecord,
    ) -> Result<(), run_history::RunHistoryError> {
      Ok(())
    }
    async fn record_pipeline_run(
      &self,
      _: run_history::PipelineRunRecord,
    ) -> Result<(), run_history::RunHistoryError> {
      Ok(())
    }
    async fn finalize_pipeline_run_status(
      &self,
      run_id: &str,
      status: RunStatus,
      _: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, run_history::RunHistoryError> {
      self
        .finalize_calls
        .lock()
        .unwrap()
        .push((run_id.to_string(), status));
      Ok(true)
    }
    async fn record_node_dispatched(
      &self,
      _: run_history::NodeDispatchRecord,
    ) -> Result<(), run_history::RunHistoryError> {
      Ok(())
    }
    async fn record_node_run(
      &self,
      r: run_history::NodeRunRecord,
    ) -> Result<(), run_history::RunHistoryError> {
      let mut by_run = self.node_runs_by_run.lock().unwrap();
      let entries = by_run.entry(r.run_id.clone()).or_default();
      if let Some(existing) = entries.iter_mut().find(|nr| nr.node_name == r.node_name) {
        existing.success = Some(r.success);
        existing.started_at = r.started_at;
        existing.completed_at = r.completed_at;
      }
      self.recorded_node_runs.lock().unwrap().push(r);
      Ok(())
    }
    async fn list_pipeline_runs(
      &self,
      _: run_history::ListRunsQuery,
    ) -> Result<Vec<PipelineRunRow>, run_history::RunHistoryError> {
      Ok(vec![])
    }
    async fn list_in_progress_runs(
      &self,
    ) -> Result<Vec<PipelineRunRow>, run_history::RunHistoryError> {
      Ok(self.in_progress_rows.lock().unwrap().clone())
    }
    async fn count_pipeline_runs(
      &self,
      _: &run_history::RunFilters,
    ) -> Result<i64, run_history::RunHistoryError> {
      Ok(0)
    }
    async fn get_pipeline_run(
      &self,
      _: &str,
    ) -> Result<Option<PipelineRunRow>, run_history::RunHistoryError> {
      Ok(self.pipeline_row.lock().unwrap().clone())
    }
    async fn list_originating_runs_for_sha(
      &self,
      _: &str,
      _: &str,
      _: &str,
      _: &str,
    ) -> Result<Vec<PipelineRunRow>, run_history::RunHistoryError> {
      Ok(vec![])
    }
    async fn list_node_runs(
      &self,
      run_id: &str,
    ) -> Result<Vec<run_history::NodeRunRow>, run_history::RunHistoryError> {
      Ok(
        self
          .node_runs_by_run
          .lock()
          .unwrap()
          .get(run_id)
          .cloned()
          .unwrap_or_default(),
      )
    }
    async fn get_node_run(
      &self,
      _: &str,
      _: &str,
    ) -> Result<Option<run_history::NodeRunRow>, run_history::RunHistoryError> {
      Ok(None)
    }
    async fn get_node_log(
      &self,
      _: &str,
      _: &str,
    ) -> Result<Option<String>, run_history::RunHistoryError> {
      Ok(None)
    }
    async fn ping(&self) -> Result<(), run_history::RunHistoryError> {
      Ok(())
    }
  }

  #[tokio::test]
  async fn terminal_sweep_finalizes_pipeline_runs_status_in_postgres() {
    let state_store = InMemoryStateStore::new();
    state_store
      .save_run("wedged-success", &make_state(NodeStatus::Success), 0)
      .await
      .unwrap();
    state_store
      .save_run("wedged-failure", &make_state(NodeStatus::Failed), 0)
      .await
      .unwrap();

    let dispatcher = Arc::new(TrackingDispatcher::new(vec![]));
    let history = CapturingRunHistory::with_row(None);

    sweep_terminal_redis_runs(dispatcher.clone(), state_store.clone(), history.clone()).await;

    let mut calls = history.finalize_calls();
    calls.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
      calls,
      vec![
        ("wedged-failure".to_string(), RunStatus::Failure),
        ("wedged-success".to_string(), RunStatus::Success),
      ]
    );
    assert!(
      state_store
        .load_run("wedged-success")
        .await
        .unwrap()
        .is_none()
    );
    assert!(
      state_store
        .load_run("wedged-failure")
        .await
        .unwrap()
        .is_none()
    );
  }

  #[tokio::test]
  async fn postgres_sweep_finalizes_wedged_pipeline_when_node_runs_already_complete() {
    // The user's scenario: Postgres has node_runs.success=true (written by the
    // rehydrated coordinator) but pipeline_runs.status='in_progress' (coordinator
    // died before finalize_run). Redis state was already cleaned up. The
    // Postgres-driven sweep must reconcile.
    let state_store = InMemoryStateStore::new();
    let dispatcher = Arc::new(TrackingDispatcher::new(vec![]));

    let run_id = uuid::Uuid::new_v4();
    let row = PipelineRunRow {
      run_id,
      pipeline_name: "test-pipeline".to_string(),
      owner: "the-conn".to_string(),
      repo: "jefferies".to_string(),
      sha: "deadbeef".to_string(),
      branch: Some("main".to_string()),
      target_branch: None,
      tag: None,
      pr_number: None,
      trigger: "push".to_string(),
      pipeline_definition: r#"
name: test-pipeline
on:
  push:
    branches: [main]
nodes:
  - name: build
    image: rust:latest
    steps:
      - cargo build
"#
      .to_string(),
      status: RunStatus::InProgress,
      created_at: chrono::Utc::now(),
      completed_at: None,
      retry_of: None,
      tenant_slug: Some("the-conn".to_string()),
    };

    let node_row = NodeRunRow {
      id: 1,
      run_id,
      node_name: "build".to_string(),
      node_definition: "{}".to_string(),
      success: Some(true),
      created_at: chrono::Utc::now(),
      started_at: Some(chrono::Utc::now()),
      completed_at: Some(chrono::Utc::now()),
      failure_reason: None,
    };

    let mut node_runs = std::collections::HashMap::new();
    node_runs.insert(run_id.to_string(), vec![node_row]);
    let history = CapturingRunHistory::with_in_progress(vec![row], node_runs);

    let config = Arc::new(AppConfig::load().expect("test config"));
    let backplane: Arc<dyn Backplane> = backplane::InMemoryBackplane::new();
    sweep_postgres_in_progress_runs(
      config,
      dispatcher.clone(),
      state_store.clone(),
      backplane,
      history.clone(),
    )
    .await;

    let calls = history.finalize_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, run_id.to_string());
    assert_eq!(calls[0].1, RunStatus::Success);
  }

  #[tokio::test]
  async fn postgres_sweep_skips_when_redis_state_present() {
    // If Redis state still exists, defer to the existing terminal-sweep / reclaim path.
    let state_store = InMemoryStateStore::new();
    let dispatcher = Arc::new(TrackingDispatcher::new(vec![]));
    let run_id = uuid::Uuid::new_v4();

    state_store
      .save_run(&run_id.to_string(), &make_state(NodeStatus::Running), 0)
      .await
      .unwrap();

    let row = PipelineRunRow {
      run_id,
      pipeline_name: "test-pipeline".to_string(),
      owner: "the-conn".to_string(),
      repo: "jefferies".to_string(),
      sha: "deadbeef".to_string(),
      branch: None,
      target_branch: None,
      tag: None,
      pr_number: None,
      trigger: "push".to_string(),
      pipeline_definition: "name: test-pipeline\non: { push: { branches: [main] } }\nnodes: []"
        .to_string(),
      status: RunStatus::InProgress,
      created_at: chrono::Utc::now(),
      completed_at: None,
      retry_of: None,
      tenant_slug: None,
    };
    let history = CapturingRunHistory::with_in_progress(vec![row], Default::default());

    let config = Arc::new(AppConfig::load().expect("test config"));
    let backplane: Arc<dyn Backplane> = backplane::InMemoryBackplane::new();
    sweep_postgres_in_progress_runs(config, dispatcher, state_store, backplane, history.clone())
      .await;
    assert!(history.finalize_calls().is_empty());
  }

  #[test]
  fn reconstruct_run_state_handles_build_then_test_with_test_undispatched() {
    let yaml = r#"
name: build-then-test
on:
  push:
    branches: [main]
nodes:
  - name: build
    image: rust:latest
    steps:
      - cargo build
  - name: test
    image: rust:latest
    after:
      - build
    steps:
      - cargo test
"#;
    let pipeline = Pipeline::from_yaml(yaml).expect("valid pipeline");
    let mut by_name = std::collections::HashMap::new();
    by_name.insert(
      "build".to_string(),
      NodeRunRow {
        id: 1,
        run_id: uuid::Uuid::new_v4(),
        node_name: "build".to_string(),
        node_definition: "{}".to_string(),
        success: Some(true),
        created_at: chrono::Utc::now(),
        started_at: Some(chrono::Utc::now()),
        completed_at: Some(chrono::Utc::now()),
        failure_reason: None,
      },
    );

    let state = reconstruct_run_state_from_postgres(&pipeline, &by_name);
    assert_eq!(
      state.statuses.get("build"),
      Some(&state_store::NodeStatus::Success)
    );
    assert_eq!(
      state.statuses.get("test"),
      Some(&state_store::NodeStatus::Pending)
    );
    assert_eq!(
      state.dependencies.get("test"),
      Some(&vec!["build".to_string()])
    );
  }

  #[test]
  fn reconstruct_run_state_treats_success_none_as_running() {
    let yaml = r#"
name: single-node
on: { push: { branches: [main] } }
nodes:
  - name: build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).expect("valid pipeline");
    let mut by_name = std::collections::HashMap::new();
    by_name.insert(
      "build".to_string(),
      NodeRunRow {
        id: 1,
        run_id: uuid::Uuid::new_v4(),
        node_name: "build".to_string(),
        node_definition: "{}".to_string(),
        success: None,
        created_at: chrono::Utc::now(),
        started_at: None,
        completed_at: None,
        failure_reason: None,
      },
    );
    let state = reconstruct_run_state_from_postgres(&pipeline, &by_name);
    assert_eq!(
      state.statuses.get("build"),
      Some(&state_store::NodeStatus::Running)
    );
  }

  fn make_pipeline_run_row(run_id_str: &str, owner: &str, repo: &str) -> PipelineRunRow {
    PipelineRunRow {
      run_id: uuid::Uuid::parse_str(run_id_str).unwrap(),
      pipeline_name: "test-pipeline".to_string(),
      owner: owner.to_string(),
      repo: repo.to_string(),
      sha: "deadbeef".to_string(),
      branch: Some("main".to_string()),
      target_branch: None,
      tag: None,
      pr_number: None,
      trigger: "push".to_string(),
      pipeline_definition: "name: test-pipeline\nnodes: []".to_string(),
      status: run_history::RunStatus::InProgress,
      created_at: chrono::Utc::now(),
      completed_at: None,
      retry_of: None,
      tenant_slug: Some("the-conn".to_string()),
    }
  }

  #[test]
  fn run_context_from_row_populates_all_fields() {
    let run_id = uuid::Uuid::new_v4().to_string();
    let row = make_pipeline_run_row(&run_id, "the-conn", "jefferies");
    let ctx = run_context_from_row(row);
    assert_eq!(ctx.owner, "the-conn");
    assert_eq!(ctx.repo, "jefferies");
    assert_eq!(ctx.sha, "deadbeef");
    assert_eq!(ctx.branch.as_deref(), Some("main"));
    assert_eq!(ctx.trigger, "push");
    assert_eq!(ctx.tenant_slug.as_deref(), Some("the-conn"));
    assert!(!ctx.pipeline_yaml.is_empty());
  }

  #[tokio::test]
  async fn reclaim_orphan_with_no_postgres_row_finalizes_run() {
    let state_store = InMemoryStateStore::new();
    let run_id = uuid::Uuid::new_v4().to_string();
    state_store
      .save_run(&run_id, &make_state(NodeStatus::Running), 0)
      .await
      .unwrap();

    let dispatcher = Arc::new(TrackingDispatcher::new(vec![]));
    let backplane: Arc<dyn Backplane> = backplane::InMemoryBackplane::new();
    let run_history: Arc<dyn RunHistory> = Arc::new(run_history::NoOpRunHistory);
    let config = Arc::new(AppConfig::load().expect("test config"));

    reclaim_orphaned_runs(
      config,
      dispatcher.clone(),
      state_store.clone(),
      backplane,
      run_history,
    )
    .await;

    let cleanup_calls = dispatcher.cleanup_calls.lock().unwrap().clone();
    assert_eq!(cleanup_calls, vec![run_id.clone()]);
    assert!(state_store.load_run(&run_id).await.unwrap().is_none());
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

  struct CorruptStateStore {
    corrupt_run_id: String,
    inner: Arc<dyn StateStore>,
    delete_calls: Mutex<Vec<String>>,
  }

  #[async_trait]
  impl StateStore for CorruptStateStore {
    async fn save_run(
      &self,
      run_id: &str,
      state: &RunState,
      expected_version: u64,
    ) -> Result<bool, state_store::StateStoreError> {
      self.inner.save_run(run_id, state, expected_version).await
    }
    async fn load_run(
      &self,
      run_id: &str,
    ) -> Result<Option<RunState>, state_store::StateStoreError> {
      if run_id == self.corrupt_run_id {
        return Err(state_store::StateStoreError::Store(
          "simulated deserialization failure".to_string(),
        ));
      }
      self.inner.load_run(run_id).await
    }
    async fn delete_run(&self, run_id: &str) -> Result<(), state_store::StateStoreError> {
      self.delete_calls.lock().unwrap().push(run_id.to_string());
      self.inner.delete_run(run_id).await
    }
    async fn try_acquire_lease(
      &self,
      run_id: &str,
      server_id: &str,
      ttl_secs: u64,
    ) -> Result<Option<u64>, state_store::StateStoreError> {
      self
        .inner
        .try_acquire_lease(run_id, server_id, ttl_secs)
        .await
    }
    async fn renew_lease(
      &self,
      run_id: &str,
      server_id: &str,
      version: u64,
      ttl_secs: u64,
    ) -> Result<bool, state_store::StateStoreError> {
      self
        .inner
        .renew_lease(run_id, server_id, version, ttl_secs)
        .await
    }
    async fn release_lease(&self, run_id: &str) -> Result<(), state_store::StateStoreError> {
      self.inner.release_lease(run_id).await
    }
    async fn get_orphaned_runs(&self) -> Result<Vec<String>, state_store::StateStoreError> {
      self.inner.get_orphaned_runs().await
    }
    async fn list_terminal_unleased_runs(
      &self,
    ) -> Result<Vec<String>, state_store::StateStoreError> {
      self.inner.list_terminal_unleased_runs().await
    }
    async fn ping(&self) -> Result<(), state_store::StateStoreError> {
      self.inner.ping().await
    }
  }

  #[tokio::test]
  async fn sweep_reaps_orphan_kube_resources_when_state_is_corrupt() {
    let inner = InMemoryStateStore::new();
    let state_store = Arc::new(CorruptStateStore {
      corrupt_run_id: "corrupt-run".to_string(),
      inner: inner.clone(),
      delete_calls: Mutex::new(Vec::new()),
    });

    let dispatcher = Arc::new(TrackingDispatcher::new(vec!["corrupt-run".to_string()]));
    sweep_orphan_kube_resources(dispatcher.clone(), state_store.clone()).await;

    let cleanup = dispatcher.cleanup_calls.lock().unwrap().clone();
    assert_eq!(
      cleanup,
      vec!["corrupt-run".to_string()],
      "kube resources for the run with corrupt redis state must be reaped"
    );
    let deletes = state_store.delete_calls.lock().unwrap().clone();
    assert_eq!(
      deletes,
      vec!["corrupt-run".to_string()],
      "the corrupt redis state must be deleted so the warn stops repeating"
    );
  }
}
