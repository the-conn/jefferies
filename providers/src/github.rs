use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use axum::{
  Json,
  body::Bytes,
  extract::{Path, State},
  http::{HeaderMap, StatusCode},
};
use chrono::Utc;
use coordinator::{RunContext, RunStatusReporter};
use hmac::{Hmac, KeyInit, Mac};
use jsonwebtoken::EncodingKey;
use octocrab::{
  Octocrab,
  models::{
    CheckRunId,
    webhook_events::{WebhookEvent, WebhookEventPayload, payload::PullRequestWebhookEventAction},
  },
  params::checks::{CheckRunConclusion, CheckRunStatus},
};
use pipelines::Pipeline;
use run_history::RunStatus;
use serde::{Serialize, de::Error};
use sha2::Sha256;
use tenancy::{GithubTenantConfig, TenantConfig, TenantProvider};
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

use super::{ProviderState, get_header};

const GITHUB_EVENT_PUSH: &str = "push";
const GITHUB_EVENT_PULL_REQUEST: &str = "pull_request";
const TRIGGER_RETRY: &str = "retry";
const JEFFERIES_DIR: &str = ".jefferies";

#[derive(Debug, Serialize)]
pub struct RetryResponse {
  pub run_id: String,
}

#[derive(Debug, Error)]
pub enum GithubError {
  #[error("Octocrab error: {0}")]
  Octocrab(#[from] octocrab::Error),
  #[error("Invalid GitHub App ID: {0}")]
  InvalidAppId(#[from] std::num::ParseIntError),
  #[error("Invalid GitHub App private key: {0}")]
  InvalidPrivateKey(#[from] jsonwebtoken::errors::Error),
  #[error("Invalid webhook event payload: {0}")]
  InvalidPayload(#[from] serde_json::Error),
}

pub struct GithubProvider;

impl GithubProvider {
  pub async fn handle_webhook(
    State(state): State<Arc<ProviderState>>,
    Path(tenant_slug): Path<String>,
    headers: HeaderMap,
    body: Bytes,
  ) -> StatusCode {
    let Some(tenant) = state.tenants.by_slug(&tenant_slug) else {
      warn!(tenant_slug, "Webhook received for unknown tenant");
      return StatusCode::NOT_FOUND;
    };
    let TenantProvider::Github(gh) = &tenant.provider;

    let signature = get_header(&headers, "X-Hub-Signature-256");
    if !signature_matches(&body, &signature, &gh.webhook_secret) {
      warn!(
        tenant_slug,
        "Unauthorized webhook attempt: Signature mismatch"
      );
      return StatusCode::UNAUTHORIZED;
    }

    let event_type = get_header(&headers, "X-GitHub-Event");
    match event_type.as_str() {
      GITHUB_EVENT_PUSH => match handle_push(&body, tenant, state).await {
        Ok(status) => status,
        Err(e) => {
          warn!(tenant_slug, error = ?e, "Failed to handle push event");
          StatusCode::INTERNAL_SERVER_ERROR
        }
      },
      GITHUB_EVENT_PULL_REQUEST => match handle_pull_request(&body, tenant, state).await {
        Ok(status) => status,
        Err(e) => {
          warn!(tenant_slug, error = ?e, "Failed to handle pull request event");
          StatusCode::INTERNAL_SERVER_ERROR
        }
      },
      _ => {
        info!(tenant_slug, event_type, "Received unsupported GitHub event");
        StatusCode::NOT_IMPLEMENTED
      }
    }
  }

  pub async fn retry_run(
    State(state): State<Arc<ProviderState>>,
    Path(run_id): Path<String>,
  ) -> (StatusCode, Json<Option<RetryResponse>>) {
    let original = match state.run_history.get_pipeline_run(&run_id).await {
      Ok(Some(row)) => row,
      Ok(None) => {
        warn!(run_id, "Retry requested for unknown run");
        return (StatusCode::NOT_FOUND, Json(None));
      }
      Err(e) => {
        warn!(run_id, error = %e, "Failed to fetch run for retry");
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(None));
      }
    };

    let Some(tenant_slug) = original.tenant_slug.clone() else {
      warn!(run_id, "Retry requested for run without tenant_slug");
      return (StatusCode::INTERNAL_SERVER_ERROR, Json(None));
    };
    let Some(tenant) = state.tenants.by_slug(&tenant_slug) else {
      warn!(
        run_id,
        tenant_slug, "Retry requested for run whose tenant is no longer registered"
      );
      return (StatusCode::INTERNAL_SERVER_ERROR, Json(None));
    };
    let TenantProvider::Github(gh) = &tenant.provider;

    let pipeline = match Pipeline::from_yaml(&original.pipeline_definition) {
      Ok(p) => p,
      Err(e) => {
        warn!(run_id, error = %e, "Stored pipeline YAML failed to parse during retry");
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(None));
      }
    };

    let install_crab = match build_installation_client(&original.owner, &original.repo, gh).await {
      Ok(c) => c,
      Err(e) => {
        warn!(run_id, error = %e, "Failed to build GitHub installation client for retry");
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(None));
      }
    };

    let run_context = RunContext {
      owner: original.owner,
      repo: original.repo,
      sha: original.sha,
      branch: original.branch,
      target_branch: original.target_branch,
      tag: original.tag,
      pr_number: original.pr_number,
      trigger: TRIGGER_RETRY.to_string(),
      pipeline_yaml: original.pipeline_definition,
      created_at: Utc::now(),
      retry_of: Some(run_id),
      tenant_slug: Some(tenant_slug),
    };

    match launch_coordinator_for_pipeline(&pipeline, run_context, &install_crab, state.clone())
      .await
    {
      Some(new_run_id) => (
        StatusCode::OK,
        Json(Some(RetryResponse { run_id: new_run_id })),
      ),
      None => (StatusCode::INTERNAL_SERVER_ERROR, Json(None)),
    }
  }
}

async fn handle_push(
  payload: &[u8],
  tenant: Arc<TenantConfig>,
  state: Arc<ProviderState>,
) -> Result<StatusCode, GithubError> {
  let event = WebhookEvent::try_from_header_and_body(GITHUB_EVENT_PUSH, payload)?;
  let WebhookEventPayload::Push(push_event) = event.specific else {
    return Err(GithubError::InvalidPayload(serde_json::Error::custom(
      "Bad push event payload",
    )));
  };

  let repo = event.repository.ok_or_else(|| {
    GithubError::InvalidPayload(serde_json::Error::custom(
      "Missing repository information in push event",
    ))
  })?;
  let owner = repo
    .owner
    .ok_or_else(|| {
      GithubError::InvalidPayload(serde_json::Error::custom(
        "Missing owner information in push event",
      ))
    })?
    .login;
  let repo_name = repo.name;
  let sha = push_event.after.clone();

  start_push_pipelines(&owner, &repo_name, &sha, &push_event.r#ref, tenant, state).await?;
  Ok(StatusCode::OK)
}

async fn handle_pull_request(
  payload: &[u8],
  tenant: Arc<TenantConfig>,
  state: Arc<ProviderState>,
) -> Result<StatusCode, GithubError> {
  let event = WebhookEvent::try_from_header_and_body(GITHUB_EVENT_PULL_REQUEST, payload)?;
  let WebhookEventPayload::PullRequest(pr_event) = event.specific else {
    return Err(GithubError::InvalidPayload(serde_json::Error::custom(
      "Bad pull request event payload",
    )));
  };

  if pr_event.action != PullRequestWebhookEventAction::Opened
    && pr_event.action != PullRequestWebhookEventAction::Synchronize
  {
    info!(action = ?pr_event.action, "Ignoring pull request event with unsupported action");
    return Ok(StatusCode::NOT_IMPLEMENTED);
  }

  let pr = pr_event.pull_request;
  let head_repo = pr.head.repo.clone().ok_or_else(|| {
    GithubError::InvalidPayload(serde_json::Error::custom(
      "Missing repository information in pull request event",
    ))
  })?;
  let owner = head_repo
    .owner
    .ok_or_else(|| {
      GithubError::InvalidPayload(serde_json::Error::custom(
        "Missing owner information in pull request event",
      ))
    })?
    .login;
  let pr_event = PullRequestEventInfo {
    owner,
    repo: head_repo.name,
    sha: pr.head.sha.clone(),
    head_branch: pr.head.ref_field.clone(),
    base_branch: pr.base.ref_field,
    pr_number: pr.number as i64,
  };
  start_pr_pipelines(pr_event, tenant, state).await?;
  Ok(StatusCode::OK)
}

struct PullRequestEventInfo {
  owner: String,
  repo: String,
  sha: String,
  head_branch: String,
  base_branch: String,
  pr_number: i64,
}

async fn start_push_pipelines(
  owner: &str,
  repo: &str,
  sha: &str,
  git_ref: &str,
  tenant: Arc<TenantConfig>,
  state: Arc<ProviderState>,
) -> Result<(), GithubError> {
  let TenantProvider::Github(gh) = &tenant.provider;
  let install_crab = build_installation_client(owner, repo, gh).await?;
  let (branch, tag) = parse_push_ref(git_ref);
  let match_ref = branch.as_deref().unwrap_or(git_ref);
  let matching = find_matching_pipelines(&install_crab, owner, repo, sha, |pipeline| {
    pipeline.triggered_by_push(match_ref)
  })
  .await?;

  for (raw_yaml, pipeline) in matching {
    info!(
      tenant_slug = %tenant.slug,
      pipeline_name = pipeline.name(),
      owner,
      repo,
      sha,
      branch = branch.as_deref().unwrap_or(""),
      tag = tag.as_deref().unwrap_or(""),
      "Pipeline triggered by push event"
    );
    let run_context = RunContext {
      owner: owner.to_string(),
      repo: repo.to_string(),
      sha: sha.to_string(),
      branch: branch.clone(),
      target_branch: None,
      tag: tag.clone(),
      pr_number: None,
      trigger: GITHUB_EVENT_PUSH.to_string(),
      pipeline_yaml: raw_yaml,
      created_at: Utc::now(),
      retry_of: None,
      tenant_slug: Some(tenant.slug.clone()),
    };
    launch_coordinator_for_pipeline(&pipeline, run_context, &install_crab, state.clone()).await;
  }

  Ok(())
}

async fn start_pr_pipelines(
  event: PullRequestEventInfo,
  tenant: Arc<TenantConfig>,
  state: Arc<ProviderState>,
) -> Result<(), GithubError> {
  let TenantProvider::Github(gh) = &tenant.provider;
  let install_crab = build_installation_client(&event.owner, &event.repo, gh).await?;
  let matching = find_matching_pipelines(
    &install_crab,
    &event.owner,
    &event.repo,
    &event.sha,
    |pipeline| pipeline.triggered_by_pull_request(&event.base_branch),
  )
  .await?;

  for (raw_yaml, pipeline) in matching {
    info!(
      tenant_slug = %tenant.slug,
      pipeline_name = pipeline.name(),
      owner = %event.owner,
      repo = %event.repo,
      sha = %event.sha,
      head_branch = %event.head_branch,
      base_branch = %event.base_branch,
      pr_number = event.pr_number,
      "Pipeline triggered by pull request event"
    );
    let run_context = RunContext {
      owner: event.owner.clone(),
      repo: event.repo.clone(),
      sha: event.sha.clone(),
      branch: Some(event.head_branch.clone()),
      target_branch: Some(event.base_branch.clone()),
      tag: None,
      pr_number: Some(event.pr_number),
      trigger: GITHUB_EVENT_PULL_REQUEST.to_string(),
      pipeline_yaml: raw_yaml,
      created_at: Utc::now(),
      retry_of: None,
      tenant_slug: Some(tenant.slug.clone()),
    };
    launch_coordinator_for_pipeline(&pipeline, run_context, &install_crab, state.clone()).await;
  }

  Ok(())
}

async fn launch_coordinator_for_pipeline(
  pipeline: &Pipeline,
  run_context: RunContext,
  install_crab: &Octocrab,
  state: Arc<ProviderState>,
) -> Option<String> {
  let run_id = Uuid::new_v4().to_string();

  let needs_source = pipeline.node_info().iter().any(|n| n.checkout);
  if needs_source
    && let Err(e) = state
      .source_manager
      .upload_source(
        &run_id,
        &run_context.owner,
        &run_context.repo,
        &run_context.sha,
        install_crab,
      )
      .await
  {
    warn!(
      run_id,
      pipeline_name = pipeline.name(),
      error = %e,
      "Failed to upload source tarball; aborting run"
    );
    return None;
  }

  let status_reporter = create_check_run_reporter(
    install_crab,
    &run_context.owner,
    &run_context.repo,
    &run_context.sha,
    &run_id,
    pipeline.name(),
  )
  .await;

  let pipeline_arc = Arc::new(pipeline.clone());
  let handle = coordinator::start_coordinator(
    run_id.clone(),
    pipeline_arc,
    coordinator::CoordinatorServices {
      config: state.config.clone(),
      dispatcher: state.dispatcher.clone(),
      state_store: state.state_store.clone(),
      backplane: state.backplane.clone(),
      run_history: state.run_history.clone(),
      status_reporter,
    },
    run_context,
  )
  .await;

  let Some(handle) = handle else {
    warn!(
      run_id,
      pipeline_name = pipeline.name(),
      "Failed to acquire lease for new run"
    );
    return None;
  };

  info!(
    run_id,
    pipeline_name = pipeline.name(),
    "Coordinator launched for pipeline run"
  );

  let monitor_run_id = run_id.clone();
  tokio::spawn(async move {
    match handle.await {
      Ok(summary) => {
        info!(
          run_id = %monitor_run_id,
          status = %summary.status,
          "Pipeline run completed"
        );
      }
      Err(e) => {
        warn!(run_id = %monitor_run_id, error = %e, "Coordinator task panicked");
      }
    }
  });

  Some(run_id)
}

async fn build_installation_client(
  owner: &str,
  repo: &str,
  github: &GithubTenantConfig,
) -> Result<Octocrab, GithubError> {
  let app_crab = Octocrab::builder()
    .app(
      github.app_id.parse::<u64>()?.into(),
      EncodingKey::from_rsa_pem(github.private_key.as_bytes())?,
    )
    .build()?;

  let installation = app_crab
    .apps()
    .get_repository_installation(owner, repo)
    .await?;

  Ok(app_crab.installation(installation.id)?)
}

struct GithubCheckRunReporter {
  crab: Octocrab,
  owner: String,
  repo: String,
  check_run_id: CheckRunId,
}

#[async_trait]
impl RunStatusReporter for GithubCheckRunReporter {
  async fn report_completed(&self, status: RunStatus) {
    let conclusion = match status {
      RunStatus::Success => CheckRunConclusion::Success,
      RunStatus::Failure => CheckRunConclusion::Failure,
      RunStatus::Cancelled => CheckRunConclusion::Cancelled,
      RunStatus::InProgress => CheckRunConclusion::Neutral,
    };
    if let Err(e) = self
      .crab
      .checks(&self.owner, &self.repo)
      .update_check_run(self.check_run_id)
      .status(CheckRunStatus::Completed)
      .conclusion(conclusion)
      .completed_at(Utc::now())
      .send()
      .await
    {
      warn!(
        owner = %self.owner,
        repo = %self.repo,
        check_run_id = self.check_run_id.0,
        error = %e,
        "Failed to update GitHub check run on completion"
      );
    }
  }
}

async fn create_check_run_reporter(
  crab: &Octocrab,
  owner: &str,
  repo: &str,
  sha: &str,
  run_id: &str,
  pipeline_name: &str,
) -> Option<Arc<dyn RunStatusReporter>> {
  match crab
    .checks(owner, repo)
    .create_check_run(pipeline_name, sha)
    .external_id(run_id)
    .status(CheckRunStatus::InProgress)
    .send()
    .await
  {
    Ok(check_run) => Some(Arc::new(GithubCheckRunReporter {
      crab: crab.clone(),
      owner: owner.to_string(),
      repo: repo.to_string(),
      check_run_id: check_run.id,
    })),
    Err(e) => {
      warn!(
        owner,
        repo,
        sha,
        pipeline_name,
        error = %e,
        "Failed to create GitHub check run; continuing without check reporting \
         (does this tenant's GH App have checks:write?)"
      );
      None
    }
  }
}

async fn find_matching_pipelines<F>(
  crab: &Octocrab,
  owner: &str,
  repo: &str,
  sha: &str,
  matches_event: F,
) -> Result<Vec<(String, Pipeline)>, GithubError>
where
  F: Fn(&Pipeline) -> bool,
{
  let dir_contents = match crab
    .repos(owner, repo)
    .get_content()
    .path(JEFFERIES_DIR)
    .r#ref(sha)
    .send()
    .await
  {
    Ok(items) => items,
    Err(octocrab::Error::GitHub { source, .. }) if source.status_code.as_u16() == 404 => {
      info!(owner, repo, "No .jefferies directory found in repository");
      return Ok(vec![]);
    }
    Err(e) => return Err(GithubError::Octocrab(e)),
  };

  let mut matching = vec![];
  for item in dir_contents.items {
    if item.r#type != "file" {
      continue;
    }
    if !item.name.ends_with(".yaml") && !item.name.ends_with(".yml") {
      continue;
    }

    let yaml_content = match fetch_file_content(crab, owner, repo, &item.path, sha).await {
      Ok(Some(content)) => content,
      Ok(None) => {
        warn!(
          path = item.path,
          "Could not decode content of pipeline file"
        );
        continue;
      }
      Err(e) => {
        warn!(path = item.path, error = ?e, "Failed to fetch pipeline file");
        continue;
      }
    };

    match Pipeline::from_yaml(&yaml_content) {
      Ok(pipeline) if matches_event(&pipeline) => {
        info!(
          pipeline_name = pipeline.name(),
          path = item.path,
          "Found matching pipeline"
        );
        matching.push((yaml_content, pipeline));
      }
      Ok(_) => {}
      Err(e) => {
        warn!(path = item.path, error = %e, "Failed to parse pipeline file as valid pipeline");
      }
    }
  }

  Ok(matching)
}

async fn fetch_file_content(
  crab: &Octocrab,
  owner: &str,
  repo: &str,
  path: &str,
  sha: &str,
) -> Result<Option<String>, GithubError> {
  let mut content_items = crab
    .repos(owner, repo)
    .get_content()
    .path(path)
    .r#ref(sha)
    .send()
    .await?;

  Ok(
    content_items
      .take_items()
      .into_iter()
      .next()
      .and_then(|item| item.decoded_content()),
  )
}

fn parse_push_ref(git_ref: &str) -> (Option<String>, Option<String>) {
  if let Some(branch) = git_ref.strip_prefix("refs/heads/") {
    (Some(branch.to_string()), None)
  } else if let Some(tag) = git_ref.strip_prefix("refs/tags/") {
    (None, Some(tag.to_string()))
  } else {
    (None, None)
  }
}

fn signature_matches(payload: &[u8], signature_header: &str, secret: &str) -> bool {
  let Some(hex_hash) = signature_header.strip_prefix("sha256=") else {
    return false;
  };

  let Ok(expected_signature) = hex::decode(hex_hash) else {
    return false;
  };

  let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
    return false;
  };

  mac.update(payload);
  mac.verify_slice(&expected_signature).is_ok()
}
