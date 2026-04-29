use std::sync::Arc;

use app_config::AppConfig;
use async_trait::async_trait;
use backplane::Backplane;
use pipelines::{NodeInfo, Pipeline};
use thiserror::Error;
use tracing::info;

use crate::source_manager::{NodeOutcome, SourceError};

#[derive(Debug, Error)]
pub enum DispatchError {
  #[error("Dispatch failed: {0}")]
  Failed(String),
  #[error("Source error: {0}")]
  Source(#[from] SourceError),
  #[error("Kubernetes error: {0}")]
  Kube(String),
}

#[async_trait]
pub trait Dispatcher: Send + Sync {
  async fn dispatch(
    &self,
    run_id: &str,
    node: &NodeInfo,
    pipeline: &Pipeline,
    config: &AppConfig,
  ) -> Result<(), DispatchError>;

  async fn cancel_node(
    &self,
    run_id: &str,
    node_name: &str,
    config: &AppConfig,
  ) -> Result<(), DispatchError>;

  async fn cleanup_run(&self, run_id: &str) -> Result<(), DispatchError>;

  async fn get_node_outcome(
    &self,
    _run_id: &str,
    _node_name: &str,
  ) -> Result<Option<NodeOutcome>, DispatchError> {
    Ok(None)
  }

  async fn get_node_log(
    &self,
    _run_id: &str,
    _node_name: &str,
  ) -> Result<Option<String>, DispatchError> {
    Ok(None)
  }
}

pub struct LogDispatcher {
  backplane: Arc<dyn Backplane>,
  _source_manager: Arc<crate::SourceManager>,
}

impl LogDispatcher {
  pub fn new(backplane: Arc<dyn Backplane>, source_manager: Arc<crate::SourceManager>) -> Self {
    Self {
      backplane,
      _source_manager: source_manager,
    }
  }
}

#[async_trait]
impl Dispatcher for LogDispatcher {
  async fn dispatch(
    &self,
    run_id: &str,
    node: &NodeInfo,
    _pipeline: &Pipeline,
    _config: &AppConfig,
  ) -> Result<(), DispatchError> {
    info!(
      run_id,
      node_name = %node.name,
      image = %node.image,
      checkout = node.checkout,
      "Dispatching node"
    );
    let backplane = self.backplane.clone();
    let run_id = run_id.to_string();
    let node_name = node.name.clone();
    tokio::spawn(async move {
      if let Err(e) = backplane
        .publish_node_completed(&run_id, &node_name, true)
        .await
      {
        tracing::warn!(
          run_id,
          node_name,
          error = %e,
          "LogDispatcher failed to publish NodeCompleted"
        );
      }
    });
    Ok(())
  }

  async fn cancel_node(
    &self,
    run_id: &str,
    node_name: &str,
    _config: &AppConfig,
  ) -> Result<(), DispatchError> {
    info!(run_id, node_name, "Cancelling node");
    Ok(())
  }

  async fn cleanup_run(&self, run_id: &str) -> Result<(), DispatchError> {
    info!(run_id, "Skipping S3 cleanup (LogDispatcher)");
    Ok(())
  }
}
