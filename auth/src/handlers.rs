use std::sync::Arc;

use axum::{
  Json, Router,
  extract::{Query, State},
  http::StatusCode,
  response::{IntoResponse, Redirect, Response},
  routing::{get, post},
};
use base64::Engine;
use openidconnect::{
  AuthenticationFlow, AuthorizationCode, CsrfToken, Nonce, PkceCodeChallenge, PkceCodeVerifier,
  Scope, TokenResponse, core::CoreResponseType,
};
use serde::Deserialize;
use tenancy::TenantRegistry;
use tower_sessions::Session;
use tracing::{info, warn};

use crate::{
  oidc::AuthState,
  session::{
    AuthSession, PreAuthState, clear_auth, clear_pre_auth, load_auth, load_pre_auth, store_auth,
    store_pre_auth,
  },
};

#[derive(Clone)]
pub struct AuthRouterState {
  pub auth: Arc<AuthState>,
  pub tenants: Arc<TenantRegistry>,
}

pub fn auth_router(state: AuthRouterState) -> Router {
  Router::new()
    .route("/login", get(login))
    .route("/callback", get(callback))
    .route("/logout", post(logout))
    .route("/me", get(me))
    .route("/active-tenant", post(active_tenant))
    .with_state(state)
}

#[derive(Debug, Deserialize)]
pub struct LoginParams {
  return_to: Option<String>,
}

async fn login(
  State(state): State<AuthRouterState>,
  session: Session,
  Query(params): Query<LoginParams>,
) -> Response {
  let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
  let (auth_url, csrf_token, nonce) = state
    .auth
    .client
    .authorize_url(
      AuthenticationFlow::<CoreResponseType>::AuthorizationCode,
      CsrfToken::new_random,
      Nonce::new_random,
    )
    .add_scope(Scope::new("openid".to_string()))
    .add_scope(Scope::new("profile".to_string()))
    .add_scope(Scope::new("email".to_string()))
    .add_scope(Scope::new("groups".to_string()))
    .set_pkce_challenge(pkce_challenge)
    .url();

  let pre_auth = PreAuthState {
    csrf: csrf_token.secret().clone(),
    pkce_verifier: pkce_verifier.secret().clone(),
    nonce: nonce.secret().clone(),
    return_to: params.return_to.filter(|r| r.starts_with('/')),
  };

  if let Err(e) = store_pre_auth(&session, &pre_auth).await {
    warn!(error = %e, "Failed to persist pre-auth state");
    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
  }

  Redirect::to(auth_url.as_str()).into_response()
}

#[derive(Debug, Deserialize)]
pub struct CallbackParams {
  code: Option<String>,
  state: Option<String>,
  error: Option<String>,
  error_description: Option<String>,
}

async fn callback(
  State(state): State<AuthRouterState>,
  session: Session,
  Query(params): Query<CallbackParams>,
) -> Response {
  if let Some(err) = params.error.as_deref() {
    warn!(
      error = err,
      description = params.error_description.as_deref().unwrap_or(""),
      "Dex returned an OIDC error to /callback"
    );
    return (StatusCode::UNAUTHORIZED, "OIDC error").into_response();
  }

  let (Some(code), Some(state_param)) = (params.code, params.state) else {
    return (StatusCode::BAD_REQUEST, "Missing code or state").into_response();
  };

  let pre_auth = match load_pre_auth(&session).await {
    Ok(Some(p)) => p,
    Ok(None) => {
      return (StatusCode::BAD_REQUEST, "Missing pre-auth state").into_response();
    }
    Err(e) => {
      warn!(error = %e, "Failed to load pre-auth state");
      return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
  };

  if pre_auth.csrf != state_param {
    warn!("CSRF token mismatch in OIDC callback");
    return (StatusCode::UNAUTHORIZED, "CSRF mismatch").into_response();
  }

  let pkce_verifier = PkceCodeVerifier::new(pre_auth.pkce_verifier.clone());
  let token_request = match state
    .auth
    .client
    .exchange_code(AuthorizationCode::new(code))
  {
    Ok(req) => req,
    Err(e) => {
      warn!(error = %e, "Failed to build token exchange request");
      return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
  };
  let token_response = match token_request
    .set_pkce_verifier(pkce_verifier)
    .request_async(&state.auth.http.inner)
    .await
  {
    Ok(t) => t,
    Err(e) => {
      warn!(error = %e, "OIDC token exchange failed");
      return StatusCode::UNAUTHORIZED.into_response();
    }
  };

  let id_token = match token_response.id_token() {
    Some(t) => t,
    None => {
      warn!("OIDC token response missing id_token");
      return StatusCode::UNAUTHORIZED.into_response();
    }
  };

  let nonce = Nonce::new(pre_auth.nonce.clone());
  let id_token_verifier = state.auth.client.id_token_verifier();
  let claims = match id_token.claims(&id_token_verifier, &nonce) {
    Ok(c) => c,
    Err(e) => {
      warn!(error = %e, "ID token verification failed");
      return StatusCode::UNAUTHORIZED.into_response();
    }
  };

  let user_id = claims.subject().as_str().to_string();
  let email = claims.email().map(|e| e.as_str().to_string());
  let name = claims
    .name()
    .and_then(|n| n.get(None).map(|v| v.as_str().to_string()));

  let groups = extract_groups(&id_token.to_string());
  let authorized_slugs: Vec<String> = groups
    .iter()
    .filter(|g| state.tenants.by_slug(g).is_some())
    .cloned()
    .collect();
  info!(
    user_id,
    ?groups,
    ?authorized_slugs,
    "Resolved OIDC groups to authorized tenant slugs"
  );

  if authorized_slugs.is_empty() {
    info!(
      user_id,
      "Authenticated user has no authorized tenants; rejecting"
    );
    if let Err(e) = clear_pre_auth(&session).await {
      warn!(error = %e, "Failed to clear pre-auth state");
    }
    return (
      StatusCode::FORBIDDEN,
      Json(serde_json::json!({ "error": "no_authorized_tenants" })),
    )
      .into_response();
  }

  let active_tenant_context = authorized_slugs[0].clone();
  let auth_session = AuthSession {
    user_id,
    email,
    name,
    authorized_slugs,
    active_tenant_context,
  };

  if let Err(e) = store_auth(&session, &auth_session).await {
    warn!(error = %e, "Failed to persist auth session");
    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
  }
  if let Err(e) = clear_pre_auth(&session).await {
    warn!(error = %e, "Failed to clear pre-auth state after success");
  }

  let target = pre_auth
    .return_to
    .unwrap_or_else(|| state.auth.post_login_redirect.clone());
  Redirect::to(&target).into_response()
}

async fn logout(session: Session) -> StatusCode {
  if let Err(e) = clear_auth(&session).await {
    warn!(error = %e, "Failed to clear auth session on logout");
    return StatusCode::INTERNAL_SERVER_ERROR;
  }
  if let Err(e) = session.flush().await {
    warn!(error = %e, "Failed to flush session on logout");
  }
  StatusCode::NO_CONTENT
}

async fn me(session: Session) -> Response {
  match load_auth(&session).await {
    Ok(Some(auth)) => Json(auth).into_response(),
    Ok(None) => StatusCode::UNAUTHORIZED.into_response(),
    Err(e) => {
      warn!(error = %e, "Failed to load auth session");
      StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
  }
}

#[derive(Debug, Deserialize)]
pub struct ActiveTenantBody {
  slug: String,
}

async fn active_tenant(session: Session, Json(body): Json<ActiveTenantBody>) -> Response {
  let mut auth = match load_auth(&session).await {
    Ok(Some(a)) => a,
    Ok(None) => return StatusCode::UNAUTHORIZED.into_response(),
    Err(e) => {
      warn!(error = %e, "Failed to load auth session");
      return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
  };

  if !auth.authorized_slugs.iter().any(|s| s == &body.slug) {
    return StatusCode::FORBIDDEN.into_response();
  }

  auth.active_tenant_context = body.slug;
  if let Err(e) = store_auth(&session, &auth).await {
    warn!(error = %e, "Failed to update active tenant");
    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
  }

  StatusCode::NO_CONTENT.into_response()
}

fn extract_groups(jwt: &str) -> Vec<String> {
  let mut parts = jwt.split('.');
  let _ = parts.next();
  let payload_b64 = match parts.next() {
    Some(p) => p,
    None => return Vec::new(),
  };
  let payload_bytes = match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload_b64) {
    Ok(b) => b,
    Err(_) => return Vec::new(),
  };
  let payload: serde_json::Value = match serde_json::from_slice(&payload_bytes) {
    Ok(v) => v,
    Err(_) => return Vec::new(),
  };
  payload
    .get("groups")
    .and_then(|v| v.as_array())
    .map(|arr| {
      arr
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect()
    })
    .unwrap_or_default()
}
