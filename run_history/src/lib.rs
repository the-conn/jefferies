use app_config::AppConfig;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{FromRow, PgPool, postgres::PgConnectOptions};
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

#[derive(Debug, Serialize, FromRow)]
pub struct PipelineRunRow {
  pub run_id: Uuid,
  pub pipeline_name: String,
  pub owner: String,
  pub repo: String,
  pub sha: String,
  pub trigger: String,
  pub pipeline_definition: String,
  pub success: Option<bool>,
  pub cancelled: bool,
  pub created_at: DateTime<Utc>,
  pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct NodeRunRow {
  pub id: i64,
  pub run_id: Uuid,
  pub node_name: String,
  pub node_definition: String,
  pub success: Option<bool>,
  pub created_at: DateTime<Utc>,
  pub started_at: Option<DateTime<Utc>>,
  pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy)]
pub enum SortColumn {
  CreatedAt,
  CompletedAt,
  PipelineName,
  Owner,
  Success,
}

impl SortColumn {
  pub fn parse(s: &str) -> Option<Self> {
    match s {
      "created_at" => Some(Self::CreatedAt),
      "completed_at" => Some(Self::CompletedAt),
      "pipeline_name" => Some(Self::PipelineName),
      "owner" => Some(Self::Owner),
      "success" => Some(Self::Success),
      _ => None,
    }
  }

  fn as_sql(self) -> &'static str {
    match self {
      Self::CreatedAt => "created_at",
      Self::CompletedAt => "completed_at",
      Self::PipelineName => "pipeline_name",
      Self::Owner => "owner",
      Self::Success => "success",
    }
  }
}

#[derive(Debug, Clone, Copy)]
pub enum SortOrder {
  Asc,
  Desc,
}

impl SortOrder {
  pub fn parse(s: &str) -> Option<Self> {
    match s {
      "asc" => Some(Self::Asc),
      "desc" => Some(Self::Desc),
      _ => None,
    }
  }

  fn as_sql(self) -> &'static str {
    match self {
      Self::Asc => "ASC",
      Self::Desc => "DESC",
    }
  }
}

pub struct ListRunsQuery {
  pub limit: i64,
  pub offset: i64,
  pub sort_by: SortColumn,
  pub order: SortOrder,
  pub owner: Option<String>,
  pub repo: Option<String>,
  pub pipeline_name: Option<String>,
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
  async fn list_pipeline_runs(
    &self,
    query: ListRunsQuery,
  ) -> Result<Vec<PipelineRunRow>, RunHistoryError>;
  async fn get_pipeline_run(&self, run_id: &str)
  -> Result<Option<PipelineRunRow>, RunHistoryError>;
  async fn list_node_runs(&self, run_id: &str) -> Result<Vec<NodeRunRow>, RunHistoryError>;
  async fn get_node_log(
    &self,
    run_id: &str,
    node_name: &str,
  ) -> Result<Option<String>, RunHistoryError>;
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

  async fn list_pipeline_runs(
    &self,
    q: ListRunsQuery,
  ) -> Result<Vec<PipelineRunRow>, RunHistoryError> {
    let sql = format!(
      "SELECT * FROM pipeline_runs \
       WHERE ($1::text IS NULL OR owner = $1) \
         AND ($2::text IS NULL OR repo = $2) \
         AND ($3::text IS NULL OR pipeline_name = $3) \
       ORDER BY {} {} NULLS LAST \
       LIMIT $4 OFFSET $5",
      q.sort_by.as_sql(),
      q.order.as_sql(),
    );
    let rows = sqlx::query_as::<_, PipelineRunRow>(&sql)
      .bind(q.owner.as_deref())
      .bind(q.repo.as_deref())
      .bind(q.pipeline_name.as_deref())
      .bind(q.limit)
      .bind(q.offset)
      .fetch_all(&self.pool)
      .await?;
    Ok(rows)
  }

  async fn get_pipeline_run(
    &self,
    run_id: &str,
  ) -> Result<Option<PipelineRunRow>, RunHistoryError> {
    let uuid = parse_run_id(run_id)?;
    let row = sqlx::query_as::<_, PipelineRunRow>("SELECT * FROM pipeline_runs WHERE run_id = $1")
      .bind(uuid)
      .fetch_optional(&self.pool)
      .await?;
    Ok(row)
  }

  async fn list_node_runs(&self, run_id: &str) -> Result<Vec<NodeRunRow>, RunHistoryError> {
    let uuid = parse_run_id(run_id)?;
    let rows = sqlx::query_as::<_, NodeRunRow>(
      "SELECT id, run_id, node_name, node_definition, success, created_at, started_at, completed_at \
       FROM node_runs WHERE run_id = $1 ORDER BY id ASC",
    )
    .bind(uuid)
    .fetch_all(&self.pool)
    .await?;
    Ok(rows)
  }

  async fn get_node_log(
    &self,
    run_id: &str,
    node_name: &str,
  ) -> Result<Option<String>, RunHistoryError> {
    let uuid = parse_run_id(run_id)?;
    let row: Option<(Option<String>,)> =
      sqlx::query_as("SELECT output_log FROM node_runs WHERE run_id = $1 AND node_name = $2")
        .bind(uuid)
        .bind(node_name)
        .fetch_optional(&self.pool)
        .await?;
    Ok(row.map(|(log,)| log.unwrap_or_default()))
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

  async fn list_pipeline_runs(
    &self,
    _: ListRunsQuery,
  ) -> Result<Vec<PipelineRunRow>, RunHistoryError> {
    Ok(vec![])
  }

  async fn get_pipeline_run(&self, _: &str) -> Result<Option<PipelineRunRow>, RunHistoryError> {
    Ok(None)
  }

  async fn list_node_runs(&self, _: &str) -> Result<Vec<NodeRunRow>, RunHistoryError> {
    Ok(vec![])
  }

  async fn get_node_log(&self, _: &str, _: &str) -> Result<Option<String>, RunHistoryError> {
    Ok(None)
  }

  async fn ping(&self) -> Result<(), RunHistoryError> {
    Ok(())
  }
}
