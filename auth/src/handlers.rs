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
    .add_scope(Scope::new("federated:id".to_string()))
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

  let extras = extract_oidc_extras(&id_token.to_string());
  let authorized_slugs = resolve_authorized_slugs(&extras, &state.tenants);
  info!(
    user_id,
    groups = ?extras.groups,
    connector_id = ?extras.connector_id,
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

fn org_from_group(group: &str) -> &str {
  group.split_once(':').map(|(org, _)| org).unwrap_or(group)
}

#[derive(Debug, Default)]
struct OidcExtras {
  groups: Vec<String>,
  connector_id: Option<String>,
}

fn resolve_authorized_slugs(extras: &OidcExtras, tenants: &TenantRegistry) -> Vec<String> {
  let Some(connector_id) = extras.connector_id.as_deref() else {
    return Vec::new();
  };
  let mut authorized: Vec<String> = Vec::new();
  let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
  for group in &extras.groups {
    let org = org_from_group(group);
    if let Some(tenant) = tenants.find_for_connector_and_org(connector_id, org)
      && seen.insert(tenant.slug.clone())
    {
      authorized.push(tenant.slug.clone());
    }
  }
  authorized
}

fn extract_oidc_extras(jwt: &str) -> OidcExtras {
  let mut parts = jwt.split('.');
  let _ = parts.next();
  let Some(payload_b64) = parts.next() else {
    return OidcExtras::default();
  };
  let Ok(payload_bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload_b64)
  else {
    return OidcExtras::default();
  };
  let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&payload_bytes) else {
    return OidcExtras::default();
  };

  let groups = payload
    .get("groups")
    .and_then(|v| v.as_array())
    .map(|arr| {
      arr
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect()
    })
    .unwrap_or_default();
  let connector_id = payload
    .pointer("/federated_claims/connector_id")
    .and_then(|v| v.as_str())
    .map(String::from);

  OidcExtras {
    groups,
    connector_id,
  }
}

#[cfg(test)]
mod tests {
  use tenancy::{
    GithubAppConfig, GithubTenantBinding, TenancyDocument, TenancyRegistry, TenantConfig,
    TenantProvider, TenantRegistry,
  };

  use super::*;

  struct TenantSpec {
    slug: &'static str,
    org_name: &'static str,
    connector_ids: &'static [&'static str],
    app_ref: &'static str,
  }

  fn registry_with(specs: &[TenantSpec]) -> Arc<TenantRegistry> {
    let mut app_ids: Vec<&'static str> = specs.iter().map(|s| s.app_ref).collect();
    app_ids.sort();
    app_ids.dedup();
    let github_apps = app_ids
      .into_iter()
      .map(|id| GithubAppConfig {
        id: id.to_string(),
        app_id: "1".to_string(),
        webhook_secret: "shh".to_string(),
        private_key: "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n"
          .to_string(),
      })
      .collect();
    let tenants = specs
      .iter()
      .map(|spec| TenantConfig {
        slug: spec.slug.to_string(),
        display_name: None,
        connector_ids: spec.connector_ids.iter().map(|s| s.to_string()).collect(),
        provider: TenantProvider::Github(GithubTenantBinding {
          org_name: spec.org_name.to_string(),
          app_ref: spec.app_ref.to_string(),
        }),
      })
      .collect();
    let registry = TenancyRegistry::from_document(TenancyDocument {
      github_apps,
      tenants,
    })
    .expect("valid registry");
    registry.tenants
  }

  fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|s| (*s).to_string()).collect()
  }

  fn extras(groups: &[&str], connector_id: Option<&str>) -> OidcExtras {
    OidcExtras {
      groups: strings(groups),
      connector_id: connector_id.map(String::from),
    }
  }

  #[test]
  fn org_from_group_strips_team_suffix() {
    assert_eq!(org_from_group("the-conn:eng"), "the-conn");
  }

  #[test]
  fn org_from_group_returns_input_when_no_colon() {
    assert_eq!(org_from_group("the-conn"), "the-conn");
  }

  #[test]
  fn org_from_group_handles_empty_team_suffix() {
    assert_eq!(org_from_group("the-conn:"), "the-conn");
  }

  #[test]
  fn org_from_group_splits_on_first_colon() {
    assert_eq!(org_from_group("the-conn:nested:team"), "the-conn");
  }

  #[test]
  fn resolve_authorized_slugs_matches_team_prefixed_groups() {
    let tenants = registry_with(&[TenantSpec {
      slug: "the-conn",
      org_name: "the-conn",
      connector_ids: &["github-global"],
      app_ref: "global",
    }]);
    let e = extras(&["the-conn:eng", "the-conn:ops"], Some("github-global"));
    assert_eq!(resolve_authorized_slugs(&e, &tenants), vec!["the-conn"]);
  }

  #[test]
  fn resolve_authorized_slugs_dedupes_repeat_orgs() {
    let tenants = registry_with(&[TenantSpec {
      slug: "the-conn",
      org_name: "the-conn",
      connector_ids: &["github-global"],
      app_ref: "global",
    }]);
    let e = extras(
      &["the-conn", "the-conn:eng", "the-conn:ops"],
      Some("github-global"),
    );
    assert_eq!(resolve_authorized_slugs(&e, &tenants), vec!["the-conn"]);
  }

  #[test]
  fn resolve_authorized_slugs_drops_unregistered_orgs() {
    let tenants = registry_with(&[TenantSpec {
      slug: "alpha",
      org_name: "alpha",
      connector_ids: &["c"],
      app_ref: "a",
    }]);
    let e = extras(&["beta:eng", "alpha:ops", "gamma"], Some("c"));
    assert_eq!(resolve_authorized_slugs(&e, &tenants), vec!["alpha"]);
  }

  #[test]
  fn resolve_authorized_slugs_returns_empty_when_no_match() {
    let tenants = registry_with(&[TenantSpec {
      slug: "alpha",
      org_name: "alpha",
      connector_ids: &["c"],
      app_ref: "a",
    }]);
    let e = extras(&["beta:eng"], Some("c"));
    assert!(resolve_authorized_slugs(&e, &tenants).is_empty());
  }

  #[test]
  fn resolve_authorized_slugs_filters_when_connector_mismatches() {
    let tenants = registry_with(&[TenantSpec {
      slug: "acme-corp",
      org_name: "acme-corp",
      connector_ids: &["github-acme"],
      app_ref: "acme",
    }]);
    let e = extras(&["acme-corp:eng"], Some("github-global"));
    assert!(resolve_authorized_slugs(&e, &tenants).is_empty());
  }

  #[test]
  fn resolve_authorized_slugs_returns_slug_not_org_name() {
    let tenants = registry_with(&[TenantSpec {
      slug: "acme-corp",
      org_name: "acme",
      connector_ids: &["github-acme"],
      app_ref: "acme",
    }]);
    let e = extras(&["acme:eng"], Some("github-acme"));
    assert_eq!(resolve_authorized_slugs(&e, &tenants), vec!["acme-corp"]);
  }

  #[test]
  fn resolve_authorized_slugs_returns_empty_without_connector_claim() {
    let tenants = registry_with(&[TenantSpec {
      slug: "the-conn",
      org_name: "the-conn",
      connector_ids: &["github-global"],
      app_ref: "global",
    }]);
    let e = extras(&["the-conn:eng"], None);
    assert!(resolve_authorized_slugs(&e, &tenants).is_empty());
  }

  #[test]
  fn resolve_authorized_slugs_picks_correct_tenant_across_connectors() {
    let tenants = registry_with(&[
      TenantSpec {
        slug: "the-conn",
        org_name: "the-conn",
        connector_ids: &["github-global"],
        app_ref: "global",
      },
      TenantSpec {
        slug: "acme-corp",
        org_name: "acme-corp",
        connector_ids: &["github-acme"],
        app_ref: "acme",
      },
    ]);
    let e = extras(&["the-conn:eng", "acme-corp:eng"], Some("github-acme"));
    assert_eq!(resolve_authorized_slugs(&e, &tenants), vec!["acme-corp"]);
  }
}
