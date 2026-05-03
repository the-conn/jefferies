use serde::{Deserialize, Serialize};
use tower_sessions::Session;

const AUTH_KEY: &str = "auth";
const PRE_AUTH_KEY: &str = "pre_auth";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSession {
  pub user_id: String,
  #[serde(default)]
  pub email: Option<String>,
  #[serde(default)]
  pub name: Option<String>,
  pub authorized_slugs: Vec<String>,
  pub active_tenant_context: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAuthState {
  pub csrf: String,
  pub pkce_verifier: String,
  pub nonce: String,
  #[serde(default)]
  pub return_to: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
  #[error("Session backend error: {0}")]
  Backend(#[from] tower_sessions::session::Error),
}

pub async fn load_auth(session: &Session) -> Result<Option<AuthSession>, SessionError> {
  Ok(session.get::<AuthSession>(AUTH_KEY).await?)
}

pub async fn store_auth(session: &Session, auth: &AuthSession) -> Result<(), SessionError> {
  session.insert(AUTH_KEY, auth).await?;
  Ok(())
}

pub async fn clear_auth(session: &Session) -> Result<(), SessionError> {
  session.remove::<AuthSession>(AUTH_KEY).await?;
  Ok(())
}

pub async fn load_pre_auth(session: &Session) -> Result<Option<PreAuthState>, SessionError> {
  Ok(session.get::<PreAuthState>(PRE_AUTH_KEY).await?)
}

pub async fn store_pre_auth(
  session: &Session,
  pre_auth: &PreAuthState,
) -> Result<(), SessionError> {
  session.insert(PRE_AUTH_KEY, pre_auth).await?;
  Ok(())
}

pub async fn clear_pre_auth(session: &Session) -> Result<(), SessionError> {
  session.remove::<PreAuthState>(PRE_AUTH_KEY).await?;
  Ok(())
}
