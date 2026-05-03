mod extractor;
mod handlers;
mod http_client;
mod oidc;
mod redis_session;
mod session;

pub use extractor::AuthorizedTenant;
pub use handlers::{AuthRouterState, auth_router};
pub use oidc::{AuthState, OidcError, build_auth_state};
pub use redis_session::{SessionStoreError, build_session_layer};
pub use session::{AuthSession, PreAuthState};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
  #[error("OIDC setup failed: {0}")]
  Oidc(#[from] OidcError),
  #[error("Session store setup failed: {0}")]
  Session(#[from] SessionStoreError),
}
