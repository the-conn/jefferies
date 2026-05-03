use std::{collections::HashMap, fmt, path::Path, sync::Arc};

use serde::Deserialize;
use thiserror::Error;

const TENANCY_PATH_ENV: &str = "JEFFERIES__TENANCY__PATH";
const DEFAULT_TENANCY_PATH: &str = "/etc/jefferies/tenancy/tenants.yaml";
const SLUG_MAX_LEN: usize = 63;
const REDACTED: &str = "<redacted>";

#[derive(Debug, Error)]
pub enum TenancyError {
  #[error("Tenancy file not found at {0}")]
  NotFound(String),
  #[error("Failed to read tenancy file at {path}: {source}")]
  Io {
    path: String,
    #[source]
    source: std::io::Error,
  },
  #[error("Failed to parse tenancy YAML: {0}")]
  Parse(String),
  #[error("Invalid tenancy config: {0}")]
  Validation(String),
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
pub struct GithubTenantConfig {
  pub app_id: String,
  pub webhook_secret: String,
  pub private_key: String,
}

impl fmt::Debug for GithubTenantConfig {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("GithubTenantConfig")
      .field("app_id", &self.app_id)
      .field("webhook_secret", &REDACTED)
      .field("private_key", &REDACTED)
      .finish()
  }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum TenantProvider {
  Github(GithubTenantConfig),
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TenantConfig {
  pub slug: String,
  #[serde(default)]
  pub display_name: Option<String>,
  #[serde(flatten)]
  pub provider: TenantProvider,
}

#[derive(Debug, Deserialize)]
pub struct TenancyDocument {
  pub tenants: Vec<TenantConfig>,
}

#[derive(Debug)]
pub struct TenantRegistry {
  by_slug: HashMap<String, Arc<TenantConfig>>,
}

impl TenantRegistry {
  pub fn load_from_env() -> Result<Self, TenancyError> {
    let path = std::env::var(TENANCY_PATH_ENV).unwrap_or_else(|_| DEFAULT_TENANCY_PATH.to_string());
    Self::load_from_path(Path::new(&path))
  }

  pub fn load_from_path(path: &Path) -> Result<Self, TenancyError> {
    let contents = read_tenancy_file(path)?;
    let document = parse_tenancy_yaml(&contents)?;
    Self::from_document(document)
  }

  pub fn from_document(document: TenancyDocument) -> Result<Self, TenancyError> {
    let mut by_slug = HashMap::with_capacity(document.tenants.len());
    for tenant in document.tenants {
      validate_tenant(&tenant)?;
      if by_slug.contains_key(&tenant.slug) {
        return Err(TenancyError::Validation(format!(
          "Duplicate tenant slug: {}",
          tenant.slug
        )));
      }
      by_slug.insert(tenant.slug.clone(), Arc::new(tenant));
    }
    Ok(Self { by_slug })
  }

  pub fn by_slug(&self, slug: &str) -> Option<Arc<TenantConfig>> {
    self.by_slug.get(slug).cloned()
  }

  pub fn iter(&self) -> impl Iterator<Item = &Arc<TenantConfig>> {
    self.by_slug.values()
  }

  pub fn len(&self) -> usize {
    self.by_slug.len()
  }

  pub fn is_empty(&self) -> bool {
    self.by_slug.is_empty()
  }
}

fn read_tenancy_file(path: &Path) -> Result<String, TenancyError> {
  let display = path.display().to_string();
  match std::fs::read_to_string(path) {
    Ok(s) => Ok(s),
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(TenancyError::NotFound(display)),
    Err(source) => Err(TenancyError::Io {
      path: display,
      source,
    }),
  }
}

fn parse_tenancy_yaml(yaml: &str) -> Result<TenancyDocument, TenancyError> {
  serde_saphyr::from_str(yaml).map_err(|e| TenancyError::Parse(e.to_string()))
}

fn validate_tenant(tenant: &TenantConfig) -> Result<(), TenancyError> {
  validate_slug(&tenant.slug)?;
  match &tenant.provider {
    TenantProvider::Github(gh) => validate_github(&tenant.slug, gh),
  }
}

fn validate_slug(slug: &str) -> Result<(), TenancyError> {
  if slug.is_empty() {
    return Err(TenancyError::Validation(
      "Tenant slug must not be empty".into(),
    ));
  }
  if slug.len() > SLUG_MAX_LEN {
    return Err(TenancyError::Validation(format!(
      "Tenant slug '{slug}' exceeds {SLUG_MAX_LEN} characters"
    )));
  }
  for (index, c) in slug.chars().enumerate() {
    if index == 0 {
      if !is_slug_alnum(c) {
        return Err(TenancyError::Validation(format!(
          "Tenant slug '{slug}' must start with a lowercase letter or digit"
        )));
      }
    } else if !is_slug_alnum(c) && c != '-' {
      return Err(TenancyError::Validation(format!(
        "Tenant slug '{slug}' contains invalid character '{c}' (allowed: a-z, 0-9, '-')"
      )));
    }
  }
  Ok(())
}

fn is_slug_alnum(c: char) -> bool {
  c.is_ascii_lowercase() || c.is_ascii_digit()
}

fn validate_github(slug: &str, gh: &GithubTenantConfig) -> Result<(), TenancyError> {
  if gh.app_id.parse::<u64>().is_err() {
    return Err(TenancyError::Validation(format!(
      "Tenant '{slug}' has invalid github.app_id '{}'; expected unsigned integer",
      gh.app_id
    )));
  }
  if gh.webhook_secret.is_empty() {
    return Err(TenancyError::Validation(format!(
      "Tenant '{slug}' has empty github.webhook_secret"
    )));
  }
  if gh.private_key.trim().is_empty() {
    return Err(TenancyError::Validation(format!(
      "Tenant '{slug}' has empty github.private_key"
    )));
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample_yaml(slug: &str) -> String {
    format!(
      r#"
tenants:
  - slug: {slug}
    display_name: "Sample"
    provider: github
    app_id: "12345"
    webhook_secret: "shh"
    private_key: |
      -----BEGIN RSA PRIVATE KEY-----
      MIIBOwIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu
      -----END RSA PRIVATE KEY-----
"#
    )
  }

  fn parse(yaml: &str) -> Result<TenantRegistry, TenancyError> {
    let doc = parse_tenancy_yaml(yaml)?;
    TenantRegistry::from_document(doc)
  }

  #[test]
  fn loads_single_tenant() {
    let registry = parse(&sample_yaml("the-conn")).unwrap();
    assert_eq!(registry.len(), 1);
    let tenant = registry.by_slug("the-conn").unwrap();
    assert_eq!(tenant.slug, "the-conn");
    assert_eq!(tenant.display_name.as_deref(), Some("Sample"));
    let TenantProvider::Github(gh) = &tenant.provider;
    assert_eq!(gh.app_id, "12345");
    assert_eq!(gh.webhook_secret, "shh");
  }

  #[test]
  fn loads_multiple_tenants() {
    let yaml = r#"
tenants:
  - slug: alpha
    provider: github
    app_id: "1"
    webhook_secret: "a"
    private_key: "key-a"
  - slug: beta
    provider: github
    app_id: "2"
    webhook_secret: "b"
    private_key: "key-b"
"#;
    let registry = parse(yaml).unwrap();
    assert_eq!(registry.len(), 2);
    assert!(registry.by_slug("alpha").is_some());
    assert!(registry.by_slug("beta").is_some());
    assert!(registry.by_slug("missing").is_none());
  }

  #[test]
  fn empty_document_is_valid() {
    let registry = TenantRegistry::from_document(TenancyDocument { tenants: vec![] }).unwrap();
    assert_eq!(registry.len(), 0);
    assert!(registry.is_empty());
  }

  #[test]
  fn rejects_duplicate_slug() {
    let yaml = r#"
tenants:
  - slug: dup
    provider: github
    app_id: "1"
    webhook_secret: "a"
    private_key: "k"
  - slug: dup
    provider: github
    app_id: "2"
    webhook_secret: "b"
    private_key: "k"
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("Duplicate"));
  }

  #[test]
  fn rejects_uppercase_slug() {
    let err = parse(&sample_yaml("The-Conn")).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
  }

  #[test]
  fn rejects_leading_hyphen_slug() {
    let err = parse(&sample_yaml("-leading")).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
  }

  #[test]
  fn rejects_invalid_char_slug() {
    let err = parse(&sample_yaml("bad_slug")).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
  }

  #[test]
  fn rejects_too_long_slug() {
    let long = "a".repeat(SLUG_MAX_LEN + 1);
    let err = parse(&sample_yaml(&long)).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
  }

  #[test]
  fn rejects_empty_webhook_secret() {
    let yaml = r#"
tenants:
  - slug: a
    provider: github
    app_id: "1"
    webhook_secret: ""
    private_key: "k"
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
  }

  #[test]
  fn rejects_non_numeric_app_id() {
    let yaml = r#"
tenants:
  - slug: a
    provider: github
    app_id: "not-a-number"
    webhook_secret: "s"
    private_key: "k"
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
  }

  #[test]
  fn rejects_unknown_provider() {
    let yaml = r#"
tenants:
  - slug: a
    provider: gitlab
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Parse(_)));
  }

  #[test]
  fn missing_path_returns_not_found() {
    let err =
      TenantRegistry::load_from_path(Path::new("/nonexistent/jefferies/tenants.yaml")).unwrap_err();
    assert!(matches!(err, TenancyError::NotFound(_)));
  }

  #[test]
  fn debug_redacts_secrets() {
    let gh = GithubTenantConfig {
      app_id: "12345".into(),
      webhook_secret: "super-secret".into(),
      private_key: "PRIVATE".into(),
    };
    let rendered = format!("{gh:?}");
    assert!(rendered.contains("12345"));
    assert!(!rendered.contains("super-secret"));
    assert!(!rendered.contains("PRIVATE"));
    assert!(rendered.contains(REDACTED));
  }
}
