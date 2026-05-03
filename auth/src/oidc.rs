use std::sync::Arc;

use app_config::AppConfig;
use openidconnect::{
  ClientId, ClientSecret, IssuerUrl, RedirectUrl,
  core::{CoreClient, CoreProviderMetadata},
};
use thiserror::Error;

use crate::http_client::{HttpClient, HttpClientError};

#[derive(Debug, Error)]
pub enum OidcError {
  #[error("Invalid Dex URL: {0}")]
  InvalidUrl(String),
  #[error("OIDC discovery failed: {0}")]
  Discovery(String),
  #[error("HTTP client setup failed: {0}")]
  Http(#[from] HttpClientError),
}

pub type DexClient = CoreClient<
  openidconnect::EndpointSet,
  openidconnect::EndpointNotSet,
  openidconnect::EndpointNotSet,
  openidconnect::EndpointNotSet,
  openidconnect::EndpointMaybeSet,
  openidconnect::EndpointMaybeSet,
>;

pub struct AuthState {
  pub client: DexClient,
  pub http: HttpClient,
  pub redirect_uri: String,
  pub post_login_redirect: String,
}

pub async fn build_auth_state(config: &AppConfig) -> Result<Arc<AuthState>, OidcError> {
  let issuer = IssuerUrl::new(config.dex_issuer().to_string())
    .map_err(|e| OidcError::InvalidUrl(format!("dex.issuer: {e}")))?;
  let redirect = RedirectUrl::new(config.dex_redirect_uri().to_string())
    .map_err(|e| OidcError::InvalidUrl(format!("dex.redirect_uri: {e}")))?;

  let http = HttpClient::new()?;

  let metadata = CoreProviderMetadata::discover_async(issuer, &http.inner)
    .await
    .map_err(|e| OidcError::Discovery(e.to_string()))?;

  let client = CoreClient::from_provider_metadata(
    metadata,
    ClientId::new(config.dex_client_id().to_string()),
    Some(ClientSecret::new(config.dex_secret().to_string())),
  )
  .set_redirect_uri(redirect);

  Ok(Arc::new(AuthState {
    client,
    http,
    redirect_uri: config.dex_redirect_uri().to_string(),
    post_login_redirect: config.dex_post_login_redirect().to_string(),
  }))
}
