use std::sync::Arc;

use app_config::AppConfig;
use axum::{
  Json, Router,
  extract::{Path, State},
  http::{HeaderMap, StatusCode},
  routing::{get, post},
};
use backplane::RabbitmqBackplane;
use coordinator::{LogDispatcher, SourceError, SourceManager, start_reaper};
use providers::{GithubProvider, ProviderState};
use serde::Deserialize;
use state_store::RedisStateStore;
use thiserror::Error;
use tokio::signal;
use tower_http::{
  cors::CorsLayer,
  trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::{Level, error, info, warn};

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

#[derive(Debug, Deserialize)]
pub struct NodeStatusUpdate {
  pub node_name: String,
  pub success: bool,
}

fn router(state: Arc<ProviderState>) -> Router {
  let cors = build_cors_layer();

  Router::new()
    .route("/health", get(health))
    .route("/webhooks/github", post(GithubProvider::handle_webhook))
    .route("/runs/{run_id}/status", post(report_node_status))
    .layer(cors)
    .layer(
      TraceLayer::new_for_http()
        .make_span_with(make_span)
        .on_request(DefaultOnRequest::new().level(Level::INFO))
        .on_response(DefaultOnResponse::new().level(Level::INFO)),
    )
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

async fn report_node_status(
  State(state): State<Arc<ProviderState>>,
  Path(run_id): Path<String>,
  Json(update): Json<NodeStatusUpdate>,
) -> StatusCode {
  match state
    .backplane
    .publish_node_completed(&run_id, &update.node_name, update.success)
    .await
  {
    Ok(()) => StatusCode::OK,
    Err(e) => {
      warn!(run_id, error = %e, "Failed to publish node completed event");
      StatusCode::INTERNAL_SERVER_ERROR
    }
  }
}

async fn verify_connections(
  state_store: &RedisStateStore,
  backplane: &RabbitmqBackplane,
  source_manager: &SourceManager,
  config: &AppConfig,
) -> Result<(), ServerError> {
  info!(url = %config.redis_url(), "Checking Redis connection...");
  if let Err(e) = state_store.ping().await {
    error!(url = %config.redis_url(), error = %e, "Failed to connect to Redis");
    return Err(ServerError::ConnectionFailed(format!("Redis: {e}")));
  }
  info!(url = %config.redis_url(), "Redis connection successful");

  let scheme =
    config.rabbitmq_url().split("://").next().ok_or_else(|| {
      ServerError::ConnectionFailed("Invalid RabbitMQ URL. Missing scheme".into())
    })?;
  let host_part = config
    .rabbitmq_url()
    .split("@")
    .last()
    .unwrap_or(config.rabbitmq_url());
  let sanitized_url = match host_part.contains("://") {
    true => host_part.to_string(),
    false => format!("{}://{}", scheme, host_part),
  };
  info!(url = sanitized_url, "Checking RabbitMQ connection...");
  if let Err(e) = backplane.ping().await {
    error!(url = sanitized_url, error = %e, "Failed to connect to RabbitMQ");
    return Err(ServerError::ConnectionFailed(format!("RabbitMQ: {e}")));
  }
  info!(url = sanitized_url, "RabbitMQ connection successful");

  info!(
    endpoint = %config.s3_endpoint(),
    bucket = %config.s3_bucket(),
    "Checking S3 connection..."
  );
  source_manager.ping().await?;
  info!(
    endpoint = %config.s3_endpoint(),
    bucket = %config.s3_bucket(),
    "S3 connection successful"
  );

  Ok(())
}

pub async fn serve(config: AppConfig) -> Result<(), ServerError> {
  let shared_config = Arc::new(config);

  let state_store = Arc::new(RedisStateStore::new(
    shared_config.redis_url(),
    shared_config.redis_password(),
    16,
  )?);

  let backplane = Arc::new(RabbitmqBackplane::new(&shared_config)?);

  let source_manager = Arc::new(SourceManager::new(&shared_config));

  verify_connections(&state_store, &backplane, &source_manager, &shared_config).await?;

  let dispatcher = Arc::new(LogDispatcher::new(
    backplane.clone(),
    source_manager.clone(),
  ));

  let _reaper = start_reaper(
    shared_config.clone(),
    dispatcher.clone(),
    state_store.clone(),
    backplane.clone(),
  );

  let state = Arc::new(ProviderState::new(
    shared_config.clone(),
    state_store,
    backplane,
    dispatcher,
    source_manager,
  ));

  let addr = format!("{}:{}", shared_config.host(), shared_config.port());
  let listener = tokio::net::TcpListener::bind(&addr).await?;
  info!(address = %addr, "Starting server...");
  axum::serve(listener, router(state))
    .with_graceful_shutdown(shutdown_signal())
    .await?;
  Ok(())
}

async fn health() -> StatusCode {
  StatusCode::OK
}
