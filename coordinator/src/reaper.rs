use std::{sync::Arc, time::Duration};

use app_config::AppConfig;
use backplane::Backplane;
use state_store::StateStore;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::{Dispatcher, start_coordinator};

pub fn start_reaper(
  config: Arc<AppConfig>,
  dispatcher: Arc<dyn Dispatcher>,
  state_store: Arc<dyn StateStore>,
  backplane: Arc<dyn Backplane>,
) -> JoinHandle<()> {
  tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.tick().await;
    loop {
      interval.tick().await;
      reclaim_orphaned_runs(
        config.clone(),
        dispatcher.clone(),
        state_store.clone(),
        backplane.clone(),
      )
      .await;
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
      config.clone(),
      dispatcher.clone(),
      state_store.clone(),
      backplane.clone(),
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
                success = summary.success,
                cancelled = summary.cancelled,
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
