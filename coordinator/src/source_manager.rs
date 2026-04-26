use std::time::Duration;

use app_config::AppConfig;
use aws_sdk_s3::{
  Client,
  config::{BehaviorVersion, Credentials, Region},
  presigning::PresigningConfig,
  primitives::ByteStream,
};
use http_body_util::BodyExt;
use octocrab::Octocrab;
use thiserror::Error;
use tracing::info;

const PRESIGN_EXPIRES_SECS: u64 = 3600;

#[derive(Error, Debug)]
pub enum SourceError {
  #[error("GitHub tarball download failed: {0}")]
  Download(String),
  #[error("S3 operation failed: {0}")]
  S3(String),
  #[error("Presign configuration error: {0}")]
  Presign(String),
}

pub struct SourceManager {
  s3: Client,
  bucket: String,
}

impl SourceManager {
  pub fn new(config: &AppConfig) -> Self {
    let credentials = Credentials::new(
      config.s3_access_key(),
      config.s3_secret_key(),
      None,
      None,
      "jefferies",
    );

    let s3_config = aws_sdk_s3::Config::builder()
      .endpoint_url(config.s3_endpoint())
      .credentials_provider(credentials)
      .region(Region::new("us-east-1"))
      .force_path_style(true)
      .behavior_version(BehaviorVersion::latest())
      .build();

    Self {
      s3: Client::from_conf(s3_config),
      bucket: config.s3_bucket().to_string(),
    }
  }

  pub async fn upload_source(
    &self,
    run_id: &str,
    owner: &str,
    repo: &str,
    sha: &str,
    crab: &Octocrab,
  ) -> Result<(), SourceError> {
    let tarball_url = format!("https://api.github.com/repos/{owner}/{repo}/tarball/{sha}");
    info!(
      run_id,
      owner, repo, sha, "Downloading source tarball from GitHub"
    );

    let response = crab
      ._get(&tarball_url)
      .await
      .map_err(|e| SourceError::Download(e.to_string()))?;

    let bytes = response
      .into_body()
      .collect()
      .await
      .map_err(|e| SourceError::Download(e.to_string()))?
      .to_bytes();

    let key = source_key(run_id);
    let size = bytes.len();
    info!(run_id, bytes = size, %key, "Uploading source tarball to S3");

    self
      .s3
      .put_object()
      .bucket(&self.bucket)
      .key(&key)
      .content_type("application/gzip")
      .body(ByteStream::from(bytes))
      .send()
      .await
      .map_err(|e| SourceError::S3(e.to_string()))?;

    info!(run_id, %key, "Source tarball uploaded successfully");
    Ok(())
  }

  pub async fn get_source_url(&self, run_id: &str) -> Result<String, SourceError> {
    let key = source_key(run_id);
    let presigned = self
      .s3
      .get_object()
      .bucket(&self.bucket)
      .key(&key)
      .presigned(presign_config()?)
      .await
      .map_err(|e| SourceError::Presign(e.to_string()))?;

    Ok(presigned.uri().to_string())
  }

  pub async fn put_status_url(&self, run_id: &str, node_name: &str) -> Result<String, SourceError> {
    let key = status_key(run_id, node_name);
    let presigned = self
      .s3
      .put_object()
      .bucket(&self.bucket)
      .key(&key)
      .presigned(presign_config()?)
      .await
      .map_err(|e| SourceError::Presign(e.to_string()))?;

    Ok(presigned.uri().to_string())
  }
}

fn source_key(run_id: &str) -> String {
  format!("runs/{run_id}/source.tar.gz")
}

fn status_key(run_id: &str, node_name: &str) -> String {
  format!("runs/{run_id}/nodes/{node_name}/status.json")
}

fn presign_config() -> Result<PresigningConfig, SourceError> {
  PresigningConfig::expires_in(Duration::from_secs(PRESIGN_EXPIRES_SECS))
    .map_err(|e| SourceError::Presign(e.to_string()))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_source_key_format() {
    assert_eq!(source_key("abc-123"), "runs/abc-123/source.tar.gz");
  }

  #[test]
  fn test_status_key_format() {
    assert_eq!(
      status_key("abc-123", "build"),
      "runs/abc-123/nodes/build/status.json"
    );
    assert_eq!(
      status_key("run-id", "lint-and-test"),
      "runs/run-id/nodes/lint-and-test/status.json"
    );
  }
}
