use app_config::AppConfig;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{
  FromRow, PgPool, Postgres,
  postgres::{PgConnectOptions, PgTypeInfo},
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
  InProgress,
  Success,
  Failure,
  Cancelled,
}

impl RunStatus {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::InProgress => "in_progress",
      Self::Success => "success",
      Self::Failure => "failure",
      Self::Cancelled => "cancelled",
    }
  }

  fn from_db_str(s: &str) -> Result<Self, sqlx::error::BoxDynError> {
    match s {
      "in_progress" => Ok(Self::InProgress),
      "success" => Ok(Self::Success),
      "failure" => Ok(Self::Failure),
      "cancelled" => Ok(Self::Cancelled),
      other => Err(format!("Unknown run status: {other}").into()),
    }
  }
}

impl std::fmt::Display for RunStatus {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(self.as_str())
  }
}

impl sqlx::Type<Postgres> for RunStatus {
  fn type_info() -> PgTypeInfo {
    <str as sqlx::Type<Postgres>>::type_info()
  }
}

impl<'q> sqlx::Encode<'q, Postgres> for RunStatus {
  fn encode_by_ref(
    &self,
    buf: &mut <Postgres as sqlx::Database>::ArgumentBuffer<'q>,
  ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
    <&str as sqlx::Encode<Postgres>>::encode_by_ref(&self.as_str(), buf)
  }
}

impl<'r> sqlx::Decode<'r, Postgres> for RunStatus {
  fn decode(
    value: <Postgres as sqlx::Database>::ValueRef<'r>,
  ) -> Result<Self, sqlx::error::BoxDynError> {
    let s = <&str as sqlx::Decode<Postgres>>::decode(value)?;
    Self::from_db_str(s)
  }
}

pub struct PipelineStartRecord {
  pub run_id: String,
  pub pipeline_name: String,
  pub owner: String,
  pub repo: String,
  pub sha: String,
  pub branch: Option<String>,
  pub target_branch: Option<String>,
  pub tag: Option<String>,
  pub pr_number: Option<i64>,
  pub trigger: String,
  pub pipeline_definition: String,
  pub created_at: DateTime<Utc>,
  pub retry_of: Option<String>,
  pub tenant_slug: Option<String>,
  pub github_check_run_id: Option<i64>,
}

pub struct PipelineRunRecord {
  pub run_id: String,
  pub pipeline_name: String,
  pub owner: String,
  pub repo: String,
  pub sha: String,
  pub branch: Option<String>,
  pub target_branch: Option<String>,
  pub tag: Option<String>,
  pub pr_number: Option<i64>,
  pub trigger: String,
  pub pipeline_definition: String,
  pub status: RunStatus,
  pub created_at: DateTime<Utc>,
  pub completed_at: Option<DateTime<Utc>>,
  pub retry_of: Option<String>,
  pub tenant_slug: Option<String>,
  pub github_check_run_id: Option<i64>,
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
  pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct PipelineRunRow {
  pub run_id: Uuid,
  pub pipeline_name: String,
  pub owner: String,
  pub repo: String,
  pub sha: String,
  pub branch: Option<String>,
  pub target_branch: Option<String>,
  pub tag: Option<String>,
  pub pr_number: Option<i64>,
  pub trigger: String,
  pub pipeline_definition: String,
  pub status: RunStatus,
  pub created_at: DateTime<Utc>,
  pub completed_at: Option<DateTime<Utc>>,
  pub retry_of: Option<Uuid>,
  pub tenant_slug: Option<String>,
  pub github_check_run_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct NodeRunRow {
  pub id: i64,
  pub run_id: Uuid,
  pub node_name: String,
  pub node_definition: String,
  pub success: Option<bool>,
  pub created_at: DateTime<Utc>,
  pub started_at: Option<DateTime<Utc>>,
  pub completed_at: Option<DateTime<Utc>>,
  pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum SortColumn {
  CreatedAt,
  CompletedAt,
  PipelineName,
  Owner,
  Status,
}

impl SortColumn {
  pub fn parse(s: &str) -> Option<Self> {
    match s {
      "created_at" => Some(Self::CreatedAt),
      "completed_at" => Some(Self::CompletedAt),
      "pipeline_name" => Some(Self::PipelineName),
      "owner" => Some(Self::Owner),
      "status" => Some(Self::Status),
      _ => None,
    }
  }

  fn as_sql(self) -> &'static str {
    match self {
      Self::CreatedAt => "created_at",
      Self::CompletedAt => "completed_at",
      Self::PipelineName => "pipeline_name",
      Self::Owner => "owner",
      Self::Status => "status",
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

#[derive(Debug, Default, Clone)]
pub struct RunFilters {
  pub owner: Option<String>,
  pub repo: Option<String>,
  pub pipeline_name: Option<String>,
  pub sha: Option<String>,
}

pub struct ListRunsQuery {
  pub limit: i64,
  pub offset: i64,
  pub sort_by: SortColumn,
  pub order: SortOrder,
  pub filters: RunFilters,
}

const FILTER_WHERE: &str = "WHERE ($1::text IS NULL OR owner = $1) \
                             AND ($2::text IS NULL OR repo = $2) \
                             AND ($3::text IS NULL OR pipeline_name = $3) \
                             AND ($4::text IS NULL OR sha = $4)";

#[async_trait]
pub trait RunHistory: Send + Sync {
  async fn record_pipeline_started(
    &self,
    record: PipelineStartRecord,
  ) -> Result<(), RunHistoryError>;
  async fn record_pipeline_run(&self, record: PipelineRunRecord) -> Result<(), RunHistoryError>;
  async fn finalize_pipeline_run_status(
    &self,
    run_id: &str,
    status: RunStatus,
    completed_at: DateTime<Utc>,
  ) -> Result<bool, RunHistoryError>;
  async fn list_in_progress_runs(&self) -> Result<Vec<PipelineRunRow>, RunHistoryError>;
  async fn record_node_dispatched(&self, record: NodeDispatchRecord)
  -> Result<(), RunHistoryError>;
  async fn record_node_run(&self, record: NodeRunRecord) -> Result<(), RunHistoryError>;
  async fn list_pipeline_runs(
    &self,
    query: ListRunsQuery,
  ) -> Result<Vec<PipelineRunRow>, RunHistoryError>;
  async fn count_pipeline_runs(&self, filters: &RunFilters) -> Result<i64, RunHistoryError>;
  async fn get_pipeline_run(&self, run_id: &str)
  -> Result<Option<PipelineRunRow>, RunHistoryError>;
  async fn list_originating_runs_for_sha(
    &self,
    tenant_slug: &str,
    owner: &str,
    repo: &str,
    sha: &str,
  ) -> Result<Vec<PipelineRunRow>, RunHistoryError>;
  async fn list_node_runs(&self, run_id: &str) -> Result<Vec<NodeRunRow>, RunHistoryError>;
  async fn get_node_run(
    &self,
    run_id: &str,
    node_name: &str,
  ) -> Result<Option<NodeRunRow>, RunHistoryError>;
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
    let retry_of = r.retry_of.as_deref().map(parse_run_id).transpose()?;
    sqlx::query(
      "INSERT INTO pipeline_runs \
       (run_id, pipeline_name, owner, repo, sha, branch, target_branch, tag, pr_number, \
        trigger, pipeline_definition, created_at, retry_of, tenant_slug, github_check_run_id) \
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
       ON CONFLICT (run_id) DO UPDATE SET \
         github_check_run_id = COALESCE(pipeline_runs.github_check_run_id, EXCLUDED.github_check_run_id)",
    )
    .bind(run_id)
    .bind(&r.pipeline_name)
    .bind(&r.owner)
    .bind(&r.repo)
    .bind(&r.sha)
    .bind(r.branch.as_deref())
    .bind(r.target_branch.as_deref())
    .bind(r.tag.as_deref())
    .bind(r.pr_number)
    .bind(&r.trigger)
    .bind(&r.pipeline_definition)
    .bind(r.created_at)
    .bind(retry_of)
    .bind(r.tenant_slug.as_deref())
    .bind(r.github_check_run_id)
    .execute(&self.pool)
    .await?;
    Ok(())
  }

  async fn record_pipeline_run(&self, r: PipelineRunRecord) -> Result<(), RunHistoryError> {
    let run_id = parse_run_id(&r.run_id)?;
    let retry_of = r.retry_of.as_deref().map(parse_run_id).transpose()?;
    sqlx::query(
      "INSERT INTO pipeline_runs \
       (run_id, pipeline_name, owner, repo, sha, branch, target_branch, tag, pr_number, \
        trigger, pipeline_definition, status, created_at, completed_at, retry_of, tenant_slug, \
        github_check_run_id) \
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) \
       ON CONFLICT (run_id) DO UPDATE SET \
         status = EXCLUDED.status, \
         completed_at = EXCLUDED.completed_at",
    )
    .bind(run_id)
    .bind(&r.pipeline_name)
    .bind(&r.owner)
    .bind(&r.repo)
    .bind(&r.sha)
    .bind(r.branch.as_deref())
    .bind(r.target_branch.as_deref())
    .bind(r.tag.as_deref())
    .bind(r.pr_number)
    .bind(&r.trigger)
    .bind(&r.pipeline_definition)
    .bind(r.status)
    .bind(r.created_at)
    .bind(r.completed_at)
    .bind(retry_of)
    .bind(r.tenant_slug.as_deref())
    .bind(r.github_check_run_id)
    .execute(&self.pool)
    .await?;
    Ok(())
  }

  async fn finalize_pipeline_run_status(
    &self,
    run_id: &str,
    status: RunStatus,
    completed_at: DateTime<Utc>,
  ) -> Result<bool, RunHistoryError> {
    let run_id = parse_run_id(run_id)?;
    let result = sqlx::query(
      "UPDATE pipeline_runs SET status = $2, completed_at = $3 \
       WHERE run_id = $1 AND status = 'in_progress'",
    )
    .bind(run_id)
    .bind(status)
    .bind(completed_at)
    .execute(&self.pool)
    .await?;
    Ok(result.rows_affected() > 0)
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
       (run_id, node_name, node_definition, success, created_at, started_at, completed_at, output_log, failure_reason) \
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
       ON CONFLICT (run_id, node_name) DO UPDATE SET \
         success = EXCLUDED.success, \
         started_at = EXCLUDED.started_at, \
         completed_at = EXCLUDED.completed_at, \
         output_log = EXCLUDED.output_log, \
         failure_reason = EXCLUDED.failure_reason",
    )
    .bind(run_id)
    .bind(&r.node_name)
    .bind(&r.node_definition)
    .bind(r.success)
    .bind(r.created_at)
    .bind(r.started_at)
    .bind(r.completed_at)
    .bind(r.output_log.as_deref())
    .bind(r.failure_reason.as_deref())
    .execute(&self.pool)
    .await?;
    Ok(())
  }

  async fn list_pipeline_runs(
    &self,
    q: ListRunsQuery,
  ) -> Result<Vec<PipelineRunRow>, RunHistoryError> {
    let sql = format!(
      "SELECT * FROM pipeline_runs {FILTER_WHERE} \
       ORDER BY {} {} NULLS LAST \
       LIMIT $5 OFFSET $6",
      q.sort_by.as_sql(),
      q.order.as_sql(),
    );
    let rows = sqlx::query_as::<_, PipelineRunRow>(&sql)
      .bind(q.filters.owner.as_deref())
      .bind(q.filters.repo.as_deref())
      .bind(q.filters.pipeline_name.as_deref())
      .bind(q.filters.sha.as_deref())
      .bind(q.limit)
      .bind(q.offset)
      .fetch_all(&self.pool)
      .await?;
    Ok(rows)
  }

  async fn list_in_progress_runs(&self) -> Result<Vec<PipelineRunRow>, RunHistoryError> {
    let rows = sqlx::query_as::<_, PipelineRunRow>(
      "SELECT * FROM pipeline_runs WHERE status = 'in_progress'",
    )
    .fetch_all(&self.pool)
    .await?;
    Ok(rows)
  }

  async fn count_pipeline_runs(&self, filters: &RunFilters) -> Result<i64, RunHistoryError> {
    let sql = format!("SELECT COUNT(*) FROM pipeline_runs {FILTER_WHERE}");
    let (count,): (i64,) = sqlx::query_as(&sql)
      .bind(filters.owner.as_deref())
      .bind(filters.repo.as_deref())
      .bind(filters.pipeline_name.as_deref())
      .bind(filters.sha.as_deref())
      .fetch_one(&self.pool)
      .await?;
    Ok(count)
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

  async fn list_originating_runs_for_sha(
    &self,
    tenant_slug: &str,
    owner: &str,
    repo: &str,
    sha: &str,
  ) -> Result<Vec<PipelineRunRow>, RunHistoryError> {
    let rows = sqlx::query_as::<_, PipelineRunRow>(
      "SELECT * FROM pipeline_runs \
       WHERE tenant_slug = $1 AND owner = $2 AND repo = $3 AND sha = $4 AND retry_of IS NULL \
       ORDER BY created_at ASC",
    )
    .bind(tenant_slug)
    .bind(owner)
    .bind(repo)
    .bind(sha)
    .fetch_all(&self.pool)
    .await?;
    Ok(rows)
  }

  async fn list_node_runs(&self, run_id: &str) -> Result<Vec<NodeRunRow>, RunHistoryError> {
    let uuid = parse_run_id(run_id)?;
    let rows = sqlx::query_as::<_, NodeRunRow>(
      "SELECT id, run_id, node_name, node_definition, success, created_at, started_at, completed_at, failure_reason \
       FROM node_runs WHERE run_id = $1 ORDER BY id ASC",
    )
    .bind(uuid)
    .fetch_all(&self.pool)
    .await?;
    Ok(rows)
  }

  async fn get_node_run(
    &self,
    run_id: &str,
    node_name: &str,
  ) -> Result<Option<NodeRunRow>, RunHistoryError> {
    let uuid = parse_run_id(run_id)?;
    let row = sqlx::query_as::<_, NodeRunRow>(
      "SELECT id, run_id, node_name, node_definition, success, created_at, started_at, completed_at, failure_reason \
       FROM node_runs WHERE run_id = $1 AND node_name = $2",
    )
    .bind(uuid)
    .bind(node_name)
    .fetch_optional(&self.pool)
    .await?;
    Ok(row)
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

  async fn finalize_pipeline_run_status(
    &self,
    _: &str,
    _: RunStatus,
    _: DateTime<Utc>,
  ) -> Result<bool, RunHistoryError> {
    Ok(false)
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

  async fn list_in_progress_runs(&self) -> Result<Vec<PipelineRunRow>, RunHistoryError> {
    Ok(vec![])
  }

  async fn count_pipeline_runs(&self, _: &RunFilters) -> Result<i64, RunHistoryError> {
    Ok(0)
  }

  async fn get_pipeline_run(&self, _: &str) -> Result<Option<PipelineRunRow>, RunHistoryError> {
    Ok(None)
  }

  async fn list_originating_runs_for_sha(
    &self,
    _: &str,
    _: &str,
    _: &str,
    _: &str,
  ) -> Result<Vec<PipelineRunRow>, RunHistoryError> {
    Ok(vec![])
  }

  async fn list_node_runs(&self, _: &str) -> Result<Vec<NodeRunRow>, RunHistoryError> {
    Ok(vec![])
  }

  async fn get_node_run(&self, _: &str, _: &str) -> Result<Option<NodeRunRow>, RunHistoryError> {
    Ok(None)
  }

  async fn get_node_log(&self, _: &str, _: &str) -> Result<Option<String>, RunHistoryError> {
    Ok(None)
  }

  async fn ping(&self) -> Result<(), RunHistoryError> {
    Ok(())
  }
}
