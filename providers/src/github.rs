use std::{fmt::Debug, sync::Arc};

use app_config::AppConfig;
use axum::{
  body::Bytes,
  extract::State,
  http::{HeaderMap, StatusCode},
};
use hmac::{Hmac, KeyInit, Mac};
use jsonwebtoken::EncodingKey;
use octocrab::{
  Octocrab,
  models::webhook_events::{
    WebhookEvent, WebhookEventPayload, payload::PullRequestWebhookEventAction,
  },
};
use pipelines::Pipeline;
use serde::de::Error;
use sha2::Sha256;
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

use super::{ProviderState, get_header};

const GITHUB_EVENT_PUSH: &str = "push";
const GITHUB_EVENT_PULL_REQUEST: &str = "pull_request";
const JEFFERIES_DIR: &str = ".jefferies";

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
    headers: HeaderMap,
    body: Bytes,
  ) -> StatusCode {
    let signature = get_header(&headers, "X-Hub-Signature-256");
    if !signature_matches(&body, &signature, state.config.github_webhook_secret()) {
      warn!("Unauthorized webhook attempt: Signature mismatch");
      return StatusCode::UNAUTHORIZED;
    }

    let event_type = get_header(&headers, "X-GitHub-Event");
    match event_type.as_str() {
      GITHUB_EVENT_PUSH => match handle_push(&body, state).await {
        Ok(status) => status,
        Err(e) => {
          warn!(error = ?e, "Failed to handle push event");
          StatusCode::INTERNAL_SERVER_ERROR
        }
      },
      GITHUB_EVENT_PULL_REQUEST => match handle_pull_request(&body, state).await {
        Ok(status) => status,
        Err(e) => {
          warn!(error = ?e, "Failed to handle pull request event");
          StatusCode::INTERNAL_SERVER_ERROR
        }
      },
      _ => {
        info!(event_type, "Received unsupported GitHub event");
        StatusCode::NOT_IMPLEMENTED
      }
    }
  }
}

async fn handle_push(payload: &[u8], state: Arc<ProviderState>) -> Result<StatusCode, GithubError> {
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
  let branch = extract_branch_from_ref(&push_event.r#ref);

  start_push_pipelines(&owner, &repo_name, &sha, branch, state).await?;
  Ok(StatusCode::OK)
}

async fn handle_pull_request(
  payload: &[u8],
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
  let repo_name = head_repo.name;
  let sha = pr_event.after.ok_or_else(|| {
    GithubError::InvalidPayload(serde_json::Error::custom(
      "Missing after field in pull request event",
    ))
  })?;
  let base_branch = pr.base.ref_field;
  start_pr_pipelines(&owner, &repo_name, &sha, &base_branch, state).await?;
  Ok(StatusCode::OK)
}

async fn start_push_pipelines(
  owner: &str,
  repo: &str,
  sha: &str,
  branch: &str,
  state: Arc<ProviderState>,
) -> Result<(), GithubError> {
  let install_crab = build_installation_client(owner, repo, &state.config).await?;
  let matching = find_matching_pipelines(&install_crab, owner, repo, sha, |pipeline| {
    pipeline.triggered_by_push(branch)
  })
  .await?;

  for pipeline in matching {
    info!(
      pipeline_name = pipeline.name(),
      owner, repo, sha, branch, "Pipeline triggered by push event"
    );
    launch_coordinator_for_pipeline(&pipeline, state.clone()).await;
  }

  Ok(())
}

async fn start_pr_pipelines(
  owner: &str,
  repo: &str,
  sha: &str,
  base_branch: &str,
  state: Arc<ProviderState>,
) -> Result<(), GithubError> {
  let install_crab = build_installation_client(owner, repo, &state.config).await?;
  let matching = find_matching_pipelines(&install_crab, owner, repo, sha, |pipeline| {
    pipeline.triggered_by_pull_request(base_branch)
  })
  .await?;

  for pipeline in matching {
    info!(
      pipeline_name = pipeline.name(),
      owner, repo, sha, base_branch, "Pipeline triggered by pull request event"
    );
    launch_coordinator_for_pipeline(&pipeline, state.clone()).await;
  }

  Ok(())
}

async fn launch_coordinator_for_pipeline(pipeline: &Pipeline, state: Arc<ProviderState>) {
  let run_id = Uuid::new_v4().to_string();
  let pipeline_arc = Arc::new(pipeline.clone());

  let handle = coordinator::start_coordinator(
    run_id.clone(),
    pipeline_arc,
    state.config.clone(),
    state.dispatcher.clone(),
    state.state_store.clone(),
    state.backplane.clone(),
  )
  .await;

  let Some(handle) = handle else {
    warn!(
      run_id,
      pipeline_name = pipeline.name(),
      "Failed to acquire lease for new run"
    );
    return;
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
          success = summary.success,
          cancelled = summary.cancelled,
          "Pipeline run completed"
        );
      }
      Err(e) => {
        warn!(run_id = %monitor_run_id, error = %e, "Coordinator task panicked");
      }
    }
  });
}

async fn build_installation_client(
  owner: &str,
  repo: &str,
  config: &AppConfig,
) -> Result<Octocrab, GithubError> {
  let app_crab = Octocrab::builder()
    .app(
      config.github_app_id().parse::<u64>()?.into(),
      EncodingKey::from_rsa_pem(config.github_private_key().as_bytes())?,
    )
    .build()?;

  let installation = app_crab
    .apps()
    .get_repository_installation(owner, repo)
    .await?;

  Ok(app_crab.installation(installation.id)?)
}

async fn find_matching_pipelines<F>(
  crab: &Octocrab,
  owner: &str,
  repo: &str,
  sha: &str,
  matches_event: F,
) -> Result<Vec<Pipeline>, GithubError>
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
        matching.push(pipeline);
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

fn extract_branch_from_ref(git_ref: &str) -> &str {
  git_ref.strip_prefix("refs/heads/").unwrap_or(git_ref)
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
