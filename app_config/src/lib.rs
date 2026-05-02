use std::env;

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use thiserror::Error;
use tracing::Level;

#[derive(Error, Debug)]
pub enum AppConfigError {
  #[error("Configuration loading failed: {0}")]
  Config(#[from] ConfigError),
}

#[derive(Debug, Deserialize)]
struct ServerConfig {
  host: String,
  port: u16,
}

#[derive(Debug, Deserialize)]
struct LogConfig {
  level: String,
}

#[derive(Debug, Deserialize)]
struct GithubConfig {
  app_id: String,
  webhook_secret: String,
  client_id: String,
  client_secret: String,
  private_key: String,
}

#[derive(Debug, Deserialize)]
struct PipelineConfig {
  default_pipeline_timeout_secs: u64,
  default_node_timeout_secs: u64,
  default_node_startup_timeout_secs: u64,
  fail_fast: bool,
}

#[derive(Debug, Deserialize)]
struct RedisConfig {
  url: String,
  password: String,
}

#[derive(Debug, Deserialize)]
struct RabbitmqConfig {
  url: String,
  pool: RabbitmqPoolConfig,
}

#[derive(Debug, Deserialize)]
struct RabbitmqPoolConfig {
  max_size: usize,
}

#[derive(Debug, Deserialize)]
struct S3Config {
  endpoint: String,
  bucket: String,
  access_key: String,
  secret_key: String,
}

#[derive(Debug, Deserialize)]
struct KubernetesConfig {
  namespace: String,
  tube_image: String,
  default_node_image: String,
  service_account_privileged: String,
  service_account_default: String,
  runtime_class: String,
  default_cpu: String,
  default_memory: String,
}

#[derive(Debug, Deserialize)]
struct PostgresConfig {
  host: String,
  port: u16,
  db: String,
  username: String,
  password: String,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
  server: ServerConfig,
  log: LogConfig,
  github: GithubConfig,
  pipeline: PipelineConfig,
  redis: RedisConfig,
  rabbitmq: RabbitmqConfig,
  s3: S3Config,
  kubernetes: KubernetesConfig,
  postgres: PostgresConfig,
}

impl AppConfig {
  pub fn load() -> Result<Self, AppConfigError> {
    let environment = env::var("ENV").unwrap_or_else(|_| "dev".into());
    let src_config_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config");

    let s = Config::builder()
      // CWD-relative: used by deployed binaries with a config/ directory alongside them
      .add_source(File::with_name("config/default").required(false))
      .add_source(File::with_name(&format!("config/{environment}")).required(false))
      // Source-relative: resolved at compile time, used by tests in any workspace crate
      .add_source(File::from(src_config_dir.join("default")).required(false))
      .add_source(File::from(src_config_dir.join(&environment)).required(false))
      .add_source(Environment::with_prefix("JEFFERIES").separator("__"))
      .build()?;

    s.try_deserialize().map_err(AppConfigError::from)
  }

  pub fn host(&self) -> &str {
    &self.server.host
  }

  pub fn port(&self) -> u16 {
    self.server.port
  }

  pub fn log_level(&self) -> Level {
    self.log.level.parse().unwrap_or(Level::INFO)
  }

  pub fn github_app_id(&self) -> &str {
    &self.github.app_id
  }

  pub fn github_webhook_secret(&self) -> &str {
    &self.github.webhook_secret
  }

  pub fn github_client_id(&self) -> &str {
    &self.github.client_id
  }

  pub fn github_client_secret(&self) -> &str {
    &self.github.client_secret
  }

  pub fn github_private_key(&self) -> &str {
    &self.github.private_key
  }

  pub fn default_pipeline_timeout_secs(&self) -> u64 {
    self.pipeline.default_pipeline_timeout_secs
  }

  pub fn default_node_timeout_secs(&self) -> u64 {
    self.pipeline.default_node_timeout_secs
  }

  pub fn default_node_startup_timeout_secs(&self) -> u64 {
    self.pipeline.default_node_startup_timeout_secs
  }

  pub fn default_fail_fast(&self) -> bool {
    self.pipeline.fail_fast
  }

  pub fn redis_url(&self) -> &str {
    &self.redis.url
  }

  pub fn redis_password(&self) -> &str {
    &self.redis.password
  }

  pub fn rabbitmq_url(&self) -> &str {
    &self.rabbitmq.url
  }

  pub fn rabbitmq_pool_max_size(&self) -> usize {
    self.rabbitmq.pool.max_size
  }

  pub fn s3_endpoint(&self) -> &str {
    &self.s3.endpoint
  }

  pub fn s3_bucket(&self) -> &str {
    &self.s3.bucket
  }

  pub fn s3_access_key(&self) -> &str {
    &self.s3.access_key
  }

  pub fn s3_secret_key(&self) -> &str {
    &self.s3.secret_key
  }

  pub fn kubernetes_namespace(&self) -> &str {
    &self.kubernetes.namespace
  }

  pub fn tube_image(&self) -> &str {
    &self.kubernetes.tube_image
  }

  pub fn default_node_image(&self) -> &str {
    &self.kubernetes.default_node_image
  }

  pub fn service_account_privileged(&self) -> &str {
    &self.kubernetes.service_account_privileged
  }

  pub fn service_account_default(&self) -> &str {
    &self.kubernetes.service_account_default
  }

  pub fn runtime_class(&self) -> &str {
    &self.kubernetes.runtime_class
  }

  pub fn default_node_cpu(&self) -> &str {
    &self.kubernetes.default_cpu
  }

  pub fn default_node_memory(&self) -> &str {
    &self.kubernetes.default_memory
  }

  pub fn postgres_host(&self) -> &str {
    &self.postgres.host
  }

  pub fn postgres_port(&self) -> u16 {
    self.postgres.port
  }

  pub fn postgres_db(&self) -> &str {
    &self.postgres.db
  }

  pub fn postgres_username(&self) -> &str {
    &self.postgres.username
  }

  pub fn postgres_password(&self) -> &str {
    &self.postgres.password
  }
}
