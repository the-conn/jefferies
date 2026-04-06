mod github;

use std::sync::Arc;

use app_config::AppConfig;
use axum::http::HeaderMap;
use backplane::Backplane;
use coordinator::Dispatcher;
pub use github::GithubProvider;
use state_store::StateStore;

pub(crate) fn get_header(headers: &HeaderMap, key: &str) -> String {
  headers
    .get(key)
    .and_then(|v| v.to_str().ok())
    .map(|s| s.to_string())
    .unwrap_or_default()
}

#[derive(Clone)]
pub struct ProviderState {
  pub config: Arc<AppConfig>,
  pub state_store: Arc<dyn StateStore>,
  pub backplane: Arc<dyn Backplane>,
  pub dispatcher: Arc<dyn Dispatcher>,
}

impl ProviderState {
  pub fn new(
    config: Arc<AppConfig>,
    state_store: Arc<dyn StateStore>,
    backplane: Arc<dyn Backplane>,
    dispatcher: Arc<dyn Dispatcher>,
  ) -> Self {
    Self {
      config,
      state_store,
      backplane,
      dispatcher,
    }
  }
}
