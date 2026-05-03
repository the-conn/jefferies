use fred::{
  prelude::{ClientLike, Pool},
  types::config::{Config as FredConfig, ReconnectPolicy},
};
use thiserror::Error;
use tower_sessions::{
  Expiry, SessionManagerLayer,
  cookie::{SameSite, time::Duration},
};
use tower_sessions_redis_store::RedisStore;

#[derive(Debug, Error)]
pub enum SessionStoreError {
  #[error("Invalid Redis URL: {0}")]
  InvalidUrl(String),
  #[error("Redis pool error: {0}")]
  Pool(String),
}

const SESSION_COOKIE: &str = "jefferies_session";
const SESSION_INACTIVITY_DAYS: i64 = 7;
const REDIS_POOL_SIZE: usize = 4;

pub async fn build_session_layer(
  redis_url: &str,
  redis_password: &str,
) -> Result<SessionManagerLayer<RedisStore<Pool>>, SessionStoreError> {
  let mut config =
    FredConfig::from_url(redis_url).map_err(|e| SessionStoreError::InvalidUrl(e.to_string()))?;
  if !redis_password.is_empty() {
    config.password = Some(redis_password.to_string());
  }

  let pool = Pool::new(
    config,
    None,
    None,
    Some(ReconnectPolicy::default()),
    REDIS_POOL_SIZE,
  )
  .map_err(|e| SessionStoreError::Pool(e.to_string()))?;

  pool.connect_pool();
  pool
    .wait_for_connect()
    .await
    .map_err(|e| SessionStoreError::Pool(e.to_string()))?;

  let store = RedisStore::new(pool);
  let layer = SessionManagerLayer::new(store)
    .with_name(SESSION_COOKIE)
    .with_secure(true)
    .with_http_only(true)
    .with_same_site(SameSite::Lax)
    .with_expiry(Expiry::OnInactivity(Duration::days(
      SESSION_INACTIVITY_DAYS,
    )));

  Ok(layer)
}
