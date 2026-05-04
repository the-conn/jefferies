use std::{collections::HashMap, fmt, path::Path, sync::Arc};

use serde::Deserialize;
use thiserror::Error;

const TENANCY_PATH_ENV: &str = "JEFFERIES__TENANCY__PATH";
const DEFAULT_TENANCY_PATH: &str = "/etc/jefferies/tenancy/tenants.yaml";
const SLUG_MAX_LEN: usize = 63;
const APP_ID_MAX_LEN: usize = 63;
const ORG_NAME_MAX_LEN: usize = 39;
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
pub struct GithubAppConfig {
  pub id: String,
  pub app_id: String,
  pub webhook_secret: String,
  pub private_key: String,
}

impl fmt::Debug for GithubAppConfig {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("GithubAppConfig")
      .field("id", &self.id)
      .field("app_id", &self.app_id)
      .field("webhook_secret", &REDACTED)
      .field("private_key", &REDACTED)
      .finish()
  }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GithubTenantBinding {
  pub org_name: String,
  #[serde(rename = "github_app")]
  pub app_ref: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum TenantProvider {
  Github(GithubTenantBinding),
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TenantConfig {
  pub slug: String,
  #[serde(default)]
  pub display_name: Option<String>,
  pub connector_ids: Vec<String>,
  #[serde(flatten)]
  pub provider: TenantProvider,
}

#[derive(Debug, Deserialize)]
pub struct TenancyDocument {
  #[serde(default)]
  pub github_apps: Vec<GithubAppConfig>,
  pub tenants: Vec<TenantConfig>,
}

#[derive(Debug)]
pub struct GithubAppRegistry {
  by_id: HashMap<String, Arc<GithubAppConfig>>,
}

impl GithubAppRegistry {
  pub fn by_id(&self, id: &str) -> Option<Arc<GithubAppConfig>> {
    self.by_id.get(id).cloned()
  }

  pub fn iter(&self) -> impl Iterator<Item = &Arc<GithubAppConfig>> {
    self.by_id.values()
  }

  pub fn len(&self) -> usize {
    self.by_id.len()
  }

  pub fn is_empty(&self) -> bool {
    self.by_id.is_empty()
  }

  fn build(apps: Vec<GithubAppConfig>) -> Result<Self, TenancyError> {
    let mut by_id = HashMap::with_capacity(apps.len());
    for app in apps {
      validate_app_entry(&app)?;
      if by_id.contains_key(&app.id) {
        return Err(TenancyError::Validation(format!(
          "Duplicate github_apps id: {}",
          app.id
        )));
      }
      by_id.insert(app.id.clone(), Arc::new(app));
    }
    Ok(Self { by_id })
  }
}

#[derive(Debug)]
pub struct TenantRegistry {
  by_slug: HashMap<String, Arc<TenantConfig>>,
  by_connector_and_org: HashMap<(String, String), Arc<TenantConfig>>,
  by_app_and_org: HashMap<(String, String), Arc<TenantConfig>>,
}

impl TenantRegistry {
  pub fn by_slug(&self, slug: &str) -> Option<Arc<TenantConfig>> {
    self.by_slug.get(slug).cloned()
  }

  pub fn find_for_connector_and_org(
    &self,
    connector_id: &str,
    org_name: &str,
  ) -> Option<Arc<TenantConfig>> {
    self
      .by_connector_and_org
      .get(&(connector_id.to_string(), org_name.to_string()))
      .cloned()
  }

  pub fn by_app_and_org(&self, app_ref: &str, org_name: &str) -> Option<Arc<TenantConfig>> {
    self
      .by_app_and_org
      .get(&(app_ref.to_string(), org_name.to_string()))
      .cloned()
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

  fn build(tenants: Vec<TenantConfig>, apps: &GithubAppRegistry) -> Result<Self, TenancyError> {
    let mut by_slug = HashMap::with_capacity(tenants.len());
    let mut by_connector_and_org: HashMap<(String, String), Arc<TenantConfig>> = HashMap::new();
    let mut by_app_and_org: HashMap<(String, String), Arc<TenantConfig>> = HashMap::new();

    for tenant in tenants {
      validate_tenant(&tenant, apps)?;
      if by_slug.contains_key(&tenant.slug) {
        return Err(TenancyError::Validation(format!(
          "Duplicate tenant slug: {}",
          tenant.slug
        )));
      }

      let TenantProvider::Github(binding) = &tenant.provider;
      let app_org_key = (binding.app_ref.clone(), binding.org_name.clone());
      if by_app_and_org.contains_key(&app_org_key) {
        return Err(TenancyError::Validation(format!(
          "Duplicate (github_app, org_name) pair: ({}, {}) referenced by tenant '{}'",
          binding.app_ref, binding.org_name, tenant.slug
        )));
      }

      let arc = Arc::new(tenant);
      let TenantProvider::Github(binding) = &arc.provider;
      for connector_id in &arc.connector_ids {
        let key = (connector_id.clone(), binding.org_name.clone());
        if by_connector_and_org.contains_key(&key) {
          return Err(TenancyError::Validation(format!(
            "Duplicate (connector_id, org_name) pair: ({}, {}) across tenants",
            connector_id, binding.org_name
          )));
        }
        by_connector_and_org.insert(key, arc.clone());
      }
      by_app_and_org.insert(app_org_key, arc.clone());
      by_slug.insert(arc.slug.clone(), arc);
    }

    Ok(Self {
      by_slug,
      by_connector_and_org,
      by_app_and_org,
    })
  }
}

#[derive(Debug, Clone)]
pub struct TenancyRegistry {
  pub apps: Arc<GithubAppRegistry>,
  pub tenants: Arc<TenantRegistry>,
}

impl TenancyRegistry {
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
    let apps = GithubAppRegistry::build(document.github_apps)?;
    let tenants = TenantRegistry::build(document.tenants, &apps)?;
    Ok(Self {
      apps: Arc::new(apps),
      tenants: Arc::new(tenants),
    })
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

fn validate_app_entry(app: &GithubAppConfig) -> Result<(), TenancyError> {
  validate_app_id_field(&app.id)?;
  if app.app_id.parse::<u64>().is_err() {
    return Err(TenancyError::Validation(format!(
      "github_apps[{}] has invalid app_id '{}'; expected unsigned integer",
      app.id, app.app_id
    )));
  }
  if app.webhook_secret.is_empty() {
    return Err(TenancyError::Validation(format!(
      "github_apps[{}] has empty webhook_secret",
      app.id
    )));
  }
  if app.private_key.trim().is_empty() {
    return Err(TenancyError::Validation(format!(
      "github_apps[{}] has empty private_key",
      app.id
    )));
  }
  Ok(())
}

fn validate_tenant(tenant: &TenantConfig, apps: &GithubAppRegistry) -> Result<(), TenancyError> {
  validate_slug(&tenant.slug)?;
  validate_connector_ids(&tenant.slug, &tenant.connector_ids)?;
  match &tenant.provider {
    TenantProvider::Github(binding) => validate_tenant_binding(&tenant.slug, binding, apps),
  }
}

fn validate_connector_ids(slug: &str, ids: &[String]) -> Result<(), TenancyError> {
  if ids.is_empty() {
    return Err(TenancyError::Validation(format!(
      "Tenant '{slug}' has empty connector_ids"
    )));
  }
  for id in ids {
    if id.trim().is_empty() {
      return Err(TenancyError::Validation(format!(
        "Tenant '{slug}' has empty entry in connector_ids"
      )));
    }
  }
  Ok(())
}

fn validate_tenant_binding(
  slug: &str,
  binding: &GithubTenantBinding,
  apps: &GithubAppRegistry,
) -> Result<(), TenancyError> {
  validate_org_name(slug, &binding.org_name)?;
  if apps.by_id(&binding.app_ref).is_none() {
    return Err(TenancyError::Validation(format!(
      "Tenant '{slug}' references unknown github_app '{}'",
      binding.app_ref
    )));
  }
  Ok(())
}

fn validate_slug(slug: &str) -> Result<(), TenancyError> {
  validate_dns_label(slug, SLUG_MAX_LEN, "Tenant slug")
}

fn validate_app_id_field(id: &str) -> Result<(), TenancyError> {
  validate_dns_label(id, APP_ID_MAX_LEN, "github_apps id")
}

fn validate_org_name(slug: &str, org: &str) -> Result<(), TenancyError> {
  if org.is_empty() {
    return Err(TenancyError::Validation(format!(
      "Tenant '{slug}' has empty org_name"
    )));
  }
  if org.len() > ORG_NAME_MAX_LEN {
    return Err(TenancyError::Validation(format!(
      "Tenant '{slug}' org_name '{org}' exceeds {ORG_NAME_MAX_LEN} characters"
    )));
  }
  for (index, c) in org.chars().enumerate() {
    if index == 0 {
      if !is_org_alnum(c) {
        return Err(TenancyError::Validation(format!(
          "Tenant '{slug}' org_name '{org}' must start with an alphanumeric character"
        )));
      }
    } else if !is_org_alnum(c) && c != '-' {
      return Err(TenancyError::Validation(format!(
        "Tenant '{slug}' org_name '{org}' contains invalid character '{c}' (allowed: a-z, A-Z, 0-9, '-')"
      )));
    }
  }
  Ok(())
}

fn validate_dns_label(value: &str, max_len: usize, label: &str) -> Result<(), TenancyError> {
  if value.is_empty() {
    return Err(TenancyError::Validation(format!(
      "{label} must not be empty"
    )));
  }
  if value.len() > max_len {
    return Err(TenancyError::Validation(format!(
      "{label} '{value}' exceeds {max_len} characters"
    )));
  }
  for (index, c) in value.chars().enumerate() {
    if index == 0 {
      if !is_slug_alnum(c) {
        return Err(TenancyError::Validation(format!(
          "{label} '{value}' must start with a lowercase letter or digit"
        )));
      }
    } else if !is_slug_alnum(c) && c != '-' {
      return Err(TenancyError::Validation(format!(
        "{label} '{value}' contains invalid character '{c}' (allowed: a-z, 0-9, '-')"
      )));
    }
  }
  Ok(())
}

fn is_slug_alnum(c: char) -> bool {
  c.is_ascii_lowercase() || c.is_ascii_digit()
}

fn is_org_alnum(c: char) -> bool {
  c.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample_yaml(slug: &str) -> String {
    format!(
      r#"
github_apps:
  - id: primary
    app_id: "12345"
    webhook_secret: "shh"
    private_key: |
      -----BEGIN RSA PRIVATE KEY-----
      MIIBOwIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu
      -----END RSA PRIVATE KEY-----

tenants:
  - slug: {slug}
    display_name: "Sample"
    connector_ids: [github-global]
    provider: github
    org_name: {slug}
    github_app: primary
"#
    )
  }

  fn parse(yaml: &str) -> Result<TenancyRegistry, TenancyError> {
    let doc = parse_tenancy_yaml(yaml)?;
    TenancyRegistry::from_document(doc)
  }

  #[test]
  fn loads_single_tenant() {
    let registry = parse(&sample_yaml("the-conn")).unwrap();
    assert_eq!(registry.tenants.len(), 1);
    assert_eq!(registry.apps.len(), 1);
    let tenant = registry.tenants.by_slug("the-conn").unwrap();
    assert_eq!(tenant.slug, "the-conn");
    assert_eq!(tenant.display_name.as_deref(), Some("Sample"));
    assert_eq!(tenant.connector_ids, vec!["github-global".to_string()]);
    let TenantProvider::Github(binding) = &tenant.provider;
    assert_eq!(binding.org_name, "the-conn");
    assert_eq!(binding.app_ref, "primary");
    let app = registry.apps.by_id("primary").unwrap();
    assert_eq!(app.app_id, "12345");
    assert_eq!(app.webhook_secret, "shh");
  }

  #[test]
  fn loads_multiple_tenants_sharing_app() {
    let yaml = r#"
github_apps:
  - id: global
    app_id: "1"
    webhook_secret: "g"
    private_key: "key-g"

tenants:
  - slug: alpha
    connector_ids: [github-global]
    provider: github
    org_name: alpha
    github_app: global
  - slug: beta
    connector_ids: [github-global]
    provider: github
    org_name: beta
    github_app: global
"#;
    let registry = parse(yaml).unwrap();
    assert_eq!(registry.tenants.len(), 2);
    assert_eq!(registry.apps.len(), 1);
    assert!(registry.tenants.by_slug("alpha").is_some());
    assert!(registry.tenants.by_slug("beta").is_some());
    assert!(registry.tenants.by_slug("missing").is_none());
  }

  #[test]
  fn empty_document_is_valid() {
    let registry = TenancyRegistry::from_document(TenancyDocument {
      github_apps: vec![],
      tenants: vec![],
    })
    .unwrap();
    assert_eq!(registry.tenants.len(), 0);
    assert!(registry.tenants.is_empty());
    assert!(registry.apps.is_empty());
  }

  #[test]
  fn rejects_duplicate_slug() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: dup
    connector_ids: [c1]
    provider: github
    org_name: dup
    github_app: a
  - slug: dup
    connector_ids: [c2]
    provider: github
    org_name: dup2
    github_app: a
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("Duplicate tenant slug"));
  }

  #[test]
  fn rejects_duplicate_app_id() {
    let yaml = r#"
github_apps:
  - id: same
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"
  - id: same
    app_id: "2"
    webhook_secret: "s"
    private_key: "k"

tenants: []
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("Duplicate github_apps id"));
  }

  #[test]
  fn rejects_tenant_referencing_unknown_app() {
    let yaml = r#"
github_apps:
  - id: real
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t
    connector_ids: [c]
    provider: github
    org_name: t
    github_app: ghost
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("unknown github_app"));
  }

  #[test]
  fn rejects_duplicate_app_org_pair() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t1
    connector_ids: [c1]
    provider: github
    org_name: shared
    github_app: a
  - slug: t2
    connector_ids: [c2]
    provider: github
    org_name: shared
    github_app: a
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("Duplicate (github_app, org_name)"));
  }

  #[test]
  fn rejects_duplicate_connector_org_pair() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"
  - id: b
    app_id: "2"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t1
    connector_ids: [shared-conn]
    provider: github
    org_name: shared
    github_app: a
  - slug: t2
    connector_ids: [shared-conn]
    provider: github
    org_name: shared
    github_app: b
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(
      err
        .to_string()
        .contains("Duplicate (connector_id, org_name)")
    );
  }

  #[test]
  fn rejects_empty_connector_ids() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t
    connector_ids: []
    provider: github
    org_name: t
    github_app: a
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("empty connector_ids"));
  }

  #[test]
  fn rejects_empty_connector_id_entry() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t
    connector_ids: [""]
    provider: github
    org_name: t
    github_app: a
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("empty entry in connector_ids"));
  }

  #[test]
  fn rejects_invalid_org_name_underscore() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t
    connector_ids: [c]
    provider: github
    org_name: bad_org
    github_app: a
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("invalid character"));
  }

  #[test]
  fn rejects_invalid_org_name_too_long() {
    let long = "a".repeat(ORG_NAME_MAX_LEN + 1);
    let yaml = format!(
      r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t
    connector_ids: [c]
    provider: github
    org_name: {long}
    github_app: a
"#
    );
    let err = parse(&yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
    assert!(err.to_string().contains("exceeds"));
  }

  #[test]
  fn rejects_invalid_app_entry_id() {
    let yaml = r#"
github_apps:
  - id: Bad
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants: []
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
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
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: ""
    private_key: "k"

tenants: []
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
  }

  #[test]
  fn rejects_non_numeric_app_id() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "not-a-number"
    webhook_secret: "s"
    private_key: "k"

tenants: []
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Validation(_)));
  }

  #[test]
  fn rejects_unknown_provider() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t
    connector_ids: [c]
    provider: gitlab
    org_name: t
    github_app: a
"#;
    let err = parse(yaml).unwrap_err();
    assert!(matches!(err, TenancyError::Parse(_)));
  }

  #[test]
  fn missing_path_returns_not_found() {
    let err = TenancyRegistry::load_from_path(Path::new("/nonexistent/jefferies/tenants.yaml"))
      .unwrap_err();
    assert!(matches!(err, TenancyError::NotFound(_)));
  }

  #[test]
  fn app_debug_redacts_secrets() {
    let app = GithubAppConfig {
      id: "primary".into(),
      app_id: "12345".into(),
      webhook_secret: "super-secret".into(),
      private_key: "PRIVATE".into(),
    };
    let rendered = format!("{app:?}");
    assert!(rendered.contains("primary"));
    assert!(rendered.contains("12345"));
    assert!(!rendered.contains("super-secret"));
    assert!(!rendered.contains("PRIVATE"));
    assert!(rendered.contains(REDACTED));
  }

  #[test]
  fn find_for_connector_and_org_filters_by_connector() {
    let yaml = r#"
github_apps:
  - id: global
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"
  - id: acme
    app_id: "2"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: the-conn
    connector_ids: [github-global]
    provider: github
    org_name: the-conn
    github_app: global
  - slug: acme-corp
    connector_ids: [github-acme]
    provider: github
    org_name: acme-corp
    github_app: acme
"#;
    let registry = parse(yaml).unwrap();
    let tenants = &registry.tenants;
    let matched = tenants
      .find_for_connector_and_org("github-acme", "acme-corp")
      .unwrap();
    assert_eq!(matched.slug, "acme-corp");
    assert!(
      tenants
        .find_for_connector_and_org("github-global", "acme-corp")
        .is_none()
    );
    assert!(
      tenants
        .find_for_connector_and_org("github-acme", "the-conn")
        .is_none()
    );
  }

  #[test]
  fn by_app_and_org_returns_correct_tenant() {
    let yaml = r#"
github_apps:
  - id: a
    app_id: "1"
    webhook_secret: "s"
    private_key: "k"
  - id: b
    app_id: "2"
    webhook_secret: "s"
    private_key: "k"

tenants:
  - slug: t1
    connector_ids: [c1]
    provider: github
    org_name: shared
    github_app: a
  - slug: t2
    connector_ids: [c2]
    provider: github
    org_name: shared
    github_app: b
"#;
    let registry = parse(yaml).unwrap();
    let tenants = &registry.tenants;
    assert_eq!(tenants.by_app_and_org("a", "shared").unwrap().slug, "t1");
    assert_eq!(tenants.by_app_and_org("b", "shared").unwrap().slug, "t2");
    assert!(tenants.by_app_and_org("a", "other").is_none());
  }
}
