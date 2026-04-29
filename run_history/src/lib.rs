use app_config::AppConfig;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, postgres::PgConnectOptions};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RunHistoryError {
  #[error("Database error: {0}")]
  Database(#[from] sqlx::Error),
  #[error("Migration error: {0}")]
  Migration(#[from] sqlx::migrate::MigrateError),
  #[error("Invalid run ID: {0}")]
  InvalidRunId(String),
}

pub struct PipelineStartRecord {
  pub run_id: String,
  pub pipeline_name: String,
  pub owner: String,
  pub repo: String,
  pub sha: String,
  pub trigger: String,
  pub pipeline_definition: String,
  pub created_at: DateTime<Utc>,
}

pub struct PipelineRunRecord {
  pub run_id: String,
  pub pipeline_name: String,
  pub owner: String,
  pub repo: String,
  pub sha: String,
  pub trigger: String,
  pub pipeline_definition: String,
  pub success: bool,
  pub cancelled: bool,
  pub created_at: DateTime<Utc>,
  pub completed_at: Option<DateTime<Utc>>,
}

pub struct NodeDispatchRecord {
  pub run_id: String,
  pub node_name: String,
  pub node_definition: String,
  pub created_at: DateTime<Utc>,
}

pub struct NodeRunRecord {
  pub run_id: String,
  pub node_name: String,
  pub node_definition: String,
  pub success: bool,
  pub created_at: DateTime<Utc>,
  pub started_at: Option<DateTime<Utc>>,
  pub completed_at: Option<DateTime<Utc>>,
  pub output_log: Option<String>,
}

#[async_trait]
pub trait RunHistory: Send + Sync {
  async fn record_pipeline_started(
    &self,
    record: PipelineStartRecord,
  ) -> Result<(), RunHistoryError>;
  async fn record_pipeline_run(&self, record: PipelineRunRecord) -> Result<(), RunHistoryError>;
  async fn record_node_dispatched(&self, record: NodeDispatchRecord)
  -> Result<(), RunHistoryError>;
  async fn record_node_run(&self, record: NodeRunRecord) -> Result<(), RunHistoryError>;
  async fn ping(&self) -> Result<(), RunHistoryError>;
}

pub struct PostgresRunHistory {
  pool: PgPool,
}

impl PostgresRunHistory {
  pub async fn connect(config: &AppConfig) -> Result<Self, RunHistoryError> {
    let opts = PgConnectOptions::new()
      .host(config.postgres_host())
      .port(config.postgres_port())
      .database(config.postgres_db())
      .username(config.postgres_username())
      .password(config.postgres_password());
    let pool = PgPool::connect_with(opts).await?;
    Ok(Self { pool })
  }

  pub async fn migrate(&self) -> Result<(), RunHistoryError> {
    sqlx::migrate!("./migrations").run(&self.pool).await?;
    Ok(())
  }
}

fn parse_run_id(s: &str) -> Result<Uuid, RunHistoryError> {
  Uuid::parse_str(s).map_err(|e| RunHistoryError::InvalidRunId(e.to_string()))
}

#[async_trait]
impl RunHistory for PostgresRunHistory {
  async fn record_pipeline_started(&self, r: PipelineStartRecord) -> Result<(), RunHistoryError> {
    let run_id = parse_run_id(&r.run_id)?;
    sqlx::query(
      "INSERT INTO pipeline_runs \
       (run_id, pipeline_name, owner, repo, sha, trigger, pipeline_definition, created_at) \
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (run_id) DO NOTHING",
    )
    .bind(run_id)
    .bind(&r.pipeline_name)
    .bind(&r.owner)
    .bind(&r.repo)
    .bind(&r.sha)
    .bind(&r.trigger)
    .bind(&r.pipeline_definition)
    .bind(r.created_at)
    .execute(&self.pool)
    .await?;
    Ok(())
  }

  async fn record_pipeline_run(&self, r: PipelineRunRecord) -> Result<(), RunHistoryError> {
    let run_id = parse_run_id(&r.run_id)?;
    sqlx::query(
      "INSERT INTO pipeline_runs \
       (run_id, pipeline_name, owner, repo, sha, trigger, pipeline_definition, \
        success, cancelled, created_at, completed_at) \
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
       ON CONFLICT (run_id) DO UPDATE SET \
         success = EXCLUDED.success, \
         cancelled = EXCLUDED.cancelled, \
         completed_at = EXCLUDED.completed_at",
    )
    .bind(run_id)
    .bind(&r.pipeline_name)
    .bind(&r.owner)
    .bind(&r.repo)
    .bind(&r.sha)
    .bind(&r.trigger)
    .bind(&r.pipeline_definition)
    .bind(r.success)
    .bind(r.cancelled)
    .bind(r.created_at)
    .bind(r.completed_at)
    .execute(&self.pool)
    .await?;
    Ok(())
  }

  async fn record_node_dispatched(&self, r: NodeDispatchRecord) -> Result<(), RunHistoryError> {
    let run_id = parse_run_id(&r.run_id)?;
    sqlx::query(
      "INSERT INTO node_runs (run_id, node_name, node_definition, created_at) \
       VALUES ($1,$2,$3,$4) ON CONFLICT (run_id, node_name) DO NOTHING",
    )
    .bind(run_id)
    .bind(&r.node_name)
    .bind(&r.node_definition)
    .bind(r.created_at)
    .execute(&self.pool)
    .await?;
    Ok(())
  }

  async fn record_node_run(&self, r: NodeRunRecord) -> Result<(), RunHistoryError> {
    let run_id = parse_run_id(&r.run_id)?;
    sqlx::query(
      "INSERT INTO node_runs \
       (run_id, node_name, node_definition, success, created_at, started_at, completed_at, output_log) \
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) \
       ON CONFLICT (run_id, node_name) DO UPDATE SET \
         success = EXCLUDED.success, \
         started_at = EXCLUDED.started_at, \
         completed_at = EXCLUDED.completed_at, \
         output_log = EXCLUDED.output_log",
    )
    .bind(run_id)
    .bind(&r.node_name)
    .bind(&r.node_definition)
    .bind(r.success)
    .bind(r.created_at)
    .bind(r.started_at)
    .bind(r.completed_at)
    .bind(r.output_log.as_deref())
    .execute(&self.pool)
    .await?;
    Ok(())
  }

  async fn ping(&self) -> Result<(), RunHistoryError> {
    sqlx::query("SELECT 1").execute(&self.pool).await?;
    Ok(())
  }
}

pub struct NoOpRunHistory;

#[async_trait]
impl RunHistory for NoOpRunHistory {
  async fn record_pipeline_started(&self, _: PipelineStartRecord) -> Result<(), RunHistoryError> {
    Ok(())
  }

  async fn record_pipeline_run(&self, _: PipelineRunRecord) -> Result<(), RunHistoryError> {
    Ok(())
  }

  async fn record_node_dispatched(&self, _: NodeDispatchRecord) -> Result<(), RunHistoryError> {
    Ok(())
  }

  async fn record_node_run(&self, _: NodeRunRecord) -> Result<(), RunHistoryError> {
    Ok(())
  }

  async fn ping(&self) -> Result<(), RunHistoryError> {
    Ok(())
  }
}
