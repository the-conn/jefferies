use std::time::Duration;

use app_config::AppConfig;
use aws_sdk_s3::{
  Client,
  config::{BehaviorVersion, Credentials, Region},
  presigning::PresigningConfig,
  primitives::{ByteStream, SdkBody},
  types::{Delete, ObjectIdentifier},
};
use octocrab::Octocrab;
use thiserror::Error;
use tracing::info;

const PRESIGN_EXPIRES_SECS: u64 = 43200;

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
    info!(
      run_id,
      owner, repo, sha, "Streaming source tarball from GitHub to S3"
    );

    let response = crab
      .repos(owner, repo)
      .download_tarball(sha.to_string())
      .await
      .map_err(|e| SourceError::Download(e.to_string()))?;

    let content_length = response
      .headers()
      .get("content-length")
      .and_then(|v| v.to_str().ok())
      .and_then(|v| v.parse::<i64>().ok());

    let key = source_key(run_id);
    info!(run_id, %key, "Uploading source tarball to S3");

    let body = ByteStream::new(SdkBody::from_body_1_x(response.into_body()));

    let mut req = self
      .s3
      .put_object()
      .bucket(&self.bucket)
      .key(&key)
      .content_type("application/gzip")
      .body(body);

    if let Some(length) = content_length {
      req = req.content_length(length);
    }

    req
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

  pub async fn ping(&self) -> Result<(), SourceError> {
    match self.s3.head_bucket().bucket(&self.bucket).send().await {
      Ok(_) => Ok(()),
      Err(e)
        if e
          .as_service_error()
          .map(|se| se.is_not_found())
          .unwrap_or(false) =>
      {
        self
          .s3
          .create_bucket()
          .bucket(&self.bucket)
          .send()
          .await
          .map_err(|e| SourceError::S3(e.to_string()))?;
        Ok(())
      }
      Err(e) => Err(SourceError::S3(e.to_string())),
    }
  }

  pub async fn cleanup_run(&self, run_id: &str) -> Result<(), SourceError> {
    let prefix = format!("runs/{run_id}/");

    let listed = self
      .s3
      .list_objects_v2()
      .bucket(&self.bucket)
      .prefix(&prefix)
      .send()
      .await
      .map_err(|e| SourceError::S3(e.to_string()))?;

    let identifiers: Vec<ObjectIdentifier> = listed
      .contents()
      .iter()
      .filter_map(|obj| {
        obj
          .key()
          .and_then(|k| ObjectIdentifier::builder().key(k).build().ok())
      })
      .collect();

    if identifiers.is_empty() {
      return Ok(());
    }

    let delete = Delete::builder()
      .set_objects(Some(identifiers))
      .quiet(true)
      .build()
      .map_err(|e| SourceError::S3(e.to_string()))?;

    self
      .s3
      .delete_objects()
      .bucket(&self.bucket)
      .delete(delete)
      .send()
      .await
      .map_err(|e| SourceError::S3(e.to_string()))?;

    info!(run_id, %prefix, "Cleaned up S3 objects for run");
    Ok(())
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
