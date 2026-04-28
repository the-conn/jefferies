use std::{sync::Arc, time::Duration};

use app_config::AppConfig;
use axum::{
  Json, Router,
  extract::{Path, State},
  http::{HeaderMap, StatusCode},
  routing::{get, post},
};
use backplane::RabbitmqBackplane;
use coordinator::{KubeDispatcher, SourceError, SourceManager, start_reaper};
use providers::{GithubProvider, ProviderState};
use state_store::RunState;
use thiserror::Error;
use tokio::signal;
use tower_http::{
  cors::CorsLayer,
  trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::{Level, debug, error, info, warn};

#[derive(Error, Debug)]
pub enum ServerError {
  #[error("Server IO error: {0}")]
  IOError(#[from] std::io::Error),
  #[error("State store error: {0}")]
  StateStore(#[from] state_store::StateStoreError),
  #[error("Backplane error: {0}")]
  Backplane(#[from] backplane::BackplaneError),
  #[error("Source manager error: {0}")]
  Source(#[from] SourceError),
  #[error("Connection check failed: {0}")]
  ConnectionFailed(String),
}

fn router(state: Arc<ProviderState>) -> Router {
  let cors = build_cors_layer();

  let health_routes = Router::new()
    .route("/health/live", get(health_live))
    .route("/health/ready", get(health_ready));

  let api_routes = Router::new()
    .route(
      "/api/v1/runs/{run_id}/nodes/{node_name}/poke",
      post(handle_node_poke),
    )
    .route("/api/v1/runs/{run_id}/cancel", post(cancel_pipeline_run))
    .route("/api/v1/runs/{run_id}/status", get(get_run_status))
    .route("/webhooks/github", post(GithubProvider::handle_webhook))
    .layer(
      TraceLayer::new_for_http()
        .make_span_with(make_span)
        .on_request(DefaultOnRequest::new().level(Level::INFO))
        .on_response(DefaultOnResponse::new().level(Level::INFO)),
    );

  Router::new()
    .merge(health_routes)
    .merge(api_routes)
    .layer(cors)
    .with_state(state)
}

fn build_cors_layer() -> CorsLayer {
  CorsLayer::very_permissive()
}

fn make_span(request: &axum::http::Request<axum::body::Body>) -> tracing::Span {
  let headers: &HeaderMap = request.headers();
  let trace_id = headers
    .get("traceparent")
    .or_else(|| headers.get("x-trace-id"))
    .or_else(|| headers.get("x-request-id"))
    .and_then(|v| v.to_str().ok())
    .unwrap_or("");

  tracing::info_span!(
    "http_request",
    method = %request.method(),
    uri = %request.uri(),
    trace_id = %trace_id,
  )
}

async fn shutdown_signal() {
  let ctrl_c = async {
    signal::ctrl_c()
      .await
      .expect("failed to install Ctrl+C handler");
  };

  #[cfg(unix)]
  let terminate = async {
    signal::unix::signal(signal::unix::SignalKind::terminate())
      .expect("failed to install signal handler")
      .recv()
      .await;
  };

  #[cfg(not(unix))]
  let terminate = std::future::pending::<()>();

  tokio::select! {
      _ = ctrl_c => { info!("Received Ctrl-C, shutting down..."); },
      _ = terminate => { info!("Received SIGTERM, shutting down..."); },
  }
}

async fn verify_connections(state: &ProviderState, verbose: bool) -> Result<(), ServerError> {
  macro_rules! conn_info {
    ($($arg:tt)*) => {
      if verbose { info!($($arg)*); } else { debug!($($arg)*); }
    };
  }

  conn_info!(url = %state.config.redis_url(), "Checking Redis connection...");
  state.state_store.ping().await.map_err(|e| {
    error!(url = %state.config.redis_url(), error = %e, "Failed to connect to Redis");
    ServerError::ConnectionFailed(format!("Redis: {e}"))
  })?;
  conn_info!(url = %state.config.redis_url(), "Redis connection successful");

  let scheme = state
    .config
    .rabbitmq_url()
    .split("://")
    .next()
    .ok_or_else(|| ServerError::ConnectionFailed("Invalid RabbitMQ URL. Missing scheme".into()))?;
  let host_part = state
    .config
    .rabbitmq_url()
    .split("@")
    .last()
    .unwrap_or(state.config.rabbitmq_url());
  let sanitized_url = match host_part.contains("://") {
    true => host_part.to_string(),
    false => format!("{}://{}", scheme, host_part),
  };
  conn_info!(url = sanitized_url, "Checking RabbitMQ connection...");
  state.backplane.ping().await.map_err(|e| {
    error!(url = sanitized_url, error = %e, "Failed to connect to RabbitMQ");
    ServerError::ConnectionFailed(format!("RabbitMQ: {e}"))
  })?;
  conn_info!(url = sanitized_url, "RabbitMQ connection successful");

  conn_info!(
    endpoint = %state.config.s3_endpoint(),
    bucket = %state.config.s3_bucket(),
    "Checking S3 connection..."
  );
  state.source_manager.ping().await?;
  conn_info!(
    endpoint = %state.config.s3_endpoint(),
    bucket = %state.config.s3_bucket(),
    "S3 connection successful"
  );

  Ok(())
}

pub async fn serve(config: AppConfig) -> Result<(), ServerError> {
  let shared_config = Arc::new(config);

  let state_store = Arc::new(state_store::RedisStateStore::new(
    shared_config.redis_url(),
    shared_config.redis_password(),
    16,
  )?);

  let backplane = Arc::new(RabbitmqBackplane::new(&shared_config)?);

  let source_manager = Arc::new(SourceManager::new(&shared_config));

  let dispatcher = Arc::new(
    KubeDispatcher::new(&shared_config, source_manager.clone())
      .await
      .map_err(|e| ServerError::ConnectionFailed(format!("Kubernetes: {e}")))?,
  );

  let state = Arc::new(ProviderState::new(
    shared_config.clone(),
    state_store,
    backplane,
    dispatcher,
    source_manager,
  ));

  verify_connections(&state, true).await?;

  let _reaper = start_reaper(
    shared_config.clone(),
    state.dispatcher.clone(),
    state.state_store.clone(),
    state.backplane.clone(),
  );

  let addr = format!("{}:{}", shared_config.host(), shared_config.port());
  let listener = tokio::net::TcpListener::bind(&addr).await?;
  info!(address = %addr, "Starting server...");
  axum::serve(listener, router(state))
    .with_graceful_shutdown(shutdown_signal())
    .await?;
  Ok(())
}

async fn health_live() -> StatusCode {
  StatusCode::OK
}

async fn health_ready(State(state): State<Arc<ProviderState>>) -> StatusCode {
  const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
  match tokio::time::timeout(PROBE_TIMEOUT, verify_connections(&state, false)).await {
    Ok(Ok(())) => StatusCode::OK,
    Ok(Err(e)) => {
      warn!(error = %e, "Readiness check failed");
      StatusCode::SERVICE_UNAVAILABLE
    }
    Err(_) => {
      warn!("Readiness check timed out");
      StatusCode::SERVICE_UNAVAILABLE
    }
  }
}

async fn handle_node_poke(
  State(state): State<Arc<ProviderState>>,
  Path((run_id, node_name)): Path<(String, String)>,
) -> StatusCode {
  let outcome = match state
    .source_manager
    .get_node_status(&run_id, &node_name)
    .await
  {
    Ok(o) => o,
    Err(SourceError::NotFound(_)) => {
      warn!(run_id, node_name, "Status file not found in S3");
      return StatusCode::NOT_FOUND;
    }
    Err(e) => {
      warn!(run_id, node_name, error = %e, "Failed to read node status from S3");
      return StatusCode::INTERNAL_SERVER_ERROR;
    }
  };
  match state
    .backplane
    .publish_node_completed(&run_id, &node_name, outcome.success)
    .await
  {
    Ok(()) => StatusCode::OK,
    Err(e) => {
      warn!(run_id, node_name, error = %e, "Failed to publish node completed event");
      StatusCode::INTERNAL_SERVER_ERROR
    }
  }
}

async fn cancel_pipeline_run(
  State(state): State<Arc<ProviderState>>,
  Path(run_id): Path<String>,
) -> StatusCode {
  if let Err(e) = state.dispatcher.cleanup_run(&run_id).await {
    warn!(run_id, error = %e, "Dispatcher cleanup failed during cancel");
  }
  if let Err(e) = state.backplane.publish_cancel(&run_id).await {
    warn!(run_id, error = %e, "Failed to publish cancel event");
    return StatusCode::INTERNAL_SERVER_ERROR;
  }
  if let Err(e) = state.source_manager.cleanup_run(&run_id).await {
    warn!(run_id, error = %e, "S3 cleanup failed during cancel");
  }
  StatusCode::OK
}

async fn get_run_status(
  State(state): State<Arc<ProviderState>>,
  Path(run_id): Path<String>,
) -> (StatusCode, Json<Option<RunState>>) {
  match state.state_store.load_run(&run_id).await {
    Ok(Some(run_state)) => (StatusCode::OK, Json(Some(run_state))),
    Ok(None) => (StatusCode::NOT_FOUND, Json(None)),
    Err(e) => {
      warn!(run_id, error = %e, "Failed to load run state");
      (StatusCode::INTERNAL_SERVER_ERROR, Json(None))
    }
  }
}

#[cfg(test)]
mod tests {
  use axum::{body::Body, http::Request};
  use backplane::InMemoryBackplane;
  use coordinator::LogDispatcher;
  use state_store::InMemoryStateStore;
  use tower::ServiceExt;

  use super::*;

  fn make_test_state() -> Arc<ProviderState> {
    let config = Arc::new(AppConfig::load().expect("test config"));
    let state_store = InMemoryStateStore::new();
    let backplane = InMemoryBackplane::new();
    let source_manager = Arc::new(SourceManager::new(&config));
    let dispatcher = Arc::new(LogDispatcher::new(
      backplane.clone(),
      source_manager.clone(),
    ));
    Arc::new(ProviderState::new(
      config,
      state_store,
      backplane,
      dispatcher,
      source_manager,
    ))
  }

  #[tokio::test]
  async fn test_health_live_returns_200() {
    let app = router(make_test_state());
    let response = app
      .oneshot(
        Request::builder()
          .uri("/health/live")
          .body(Body::empty())
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
  }

  #[tokio::test]
  async fn test_get_run_status_unknown_run_returns_404() {
    let app = router(make_test_state());
    let response = app
      .oneshot(
        Request::builder()
          .uri("/api/v1/runs/nonexistent-run/status")
          .body(Body::empty())
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
  }

  #[tokio::test]
  async fn test_cancel_pipeline_run_returns_200() {
    let app = router(make_test_state());
    let response = app
      .oneshot(
        Request::builder()
          .method("POST")
          .uri("/api/v1/runs/test-run/cancel")
          .body(Body::empty())
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
  }
}
