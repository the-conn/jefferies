use std::collections::HashMap;

use axum::{
  extract::{FromRequestParts, Path},
  http::{StatusCode, request::Parts},
};
use tower_sessions::Session;
use tracing::warn;

use crate::session::{AuthSession, load_auth};

pub struct AuthorizedTenant {
  pub slug: String,
  pub session: AuthSession,
}

impl<S> FromRequestParts<S> for AuthorizedTenant
where
  S: Send + Sync,
{
  type Rejection = StatusCode;

  async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
    let Path(path_params): Path<HashMap<String, String>> = Path::from_request_parts(parts, state)
      .await
      .map_err(|_| StatusCode::BAD_REQUEST)?;

    let Some(slug) = path_params.get("slug").cloned() else {
      return Err(StatusCode::BAD_REQUEST);
    };

    let session = Session::from_request_parts(parts, state)
      .await
      .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let auth = match load_auth(&session).await {
      Ok(Some(auth)) => auth,
      Ok(None) => return Err(StatusCode::UNAUTHORIZED),
      Err(e) => {
        warn!(error = %e, "Failed to load auth session");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
      }
    };

    if !auth.authorized_slugs.iter().any(|s| s == &slug) {
      return Err(StatusCode::FORBIDDEN);
    }

    Ok(AuthorizedTenant {
      slug,
      session: auth,
    })
  }
}
