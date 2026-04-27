use std::time::Duration;

use app_config::AppConfig;
use aws_sdk_s3::{
  Client,
  config::{BehaviorVersion, Credentials, Region},
  presigning::PresigningConfig,
  primitives::ByteStream,
  types::{
    BucketLifecycleConfiguration, CompletedMultipartUpload, CompletedPart, Delete,
    ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, ObjectIdentifier,
  },
};
use aws_smithy_runtime_api::{
  box_error::BoxError,
  client::{
    interceptors::{Intercept, context::BeforeTransmitInterceptorContextMut},
    runtime_components::RuntimeComponents,
  },
};
use aws_smithy_types::config_bag::ConfigBag;
use http_body_util::BodyExt;
use octocrab::Octocrab;
use thiserror::Error;
use tracing::info;

const PRESIGN_EXPIRES_SECS: u64 = 43200;
const PART_SIZE: usize = 8 * 1024 * 1024; // 8 MiB; S3 minimum is 5 MiB except for the last part

#[derive(Debug)]
struct ContentLengthZeroInterceptor;

impl Intercept for ContentLengthZeroInterceptor {
  fn name(&self) -> &'static str {
    "ContentLengthZeroInterceptor"
  }

  fn modify_before_transmit(
    &self,
    context: &mut BeforeTransmitInterceptorContextMut<'_>,
    _: &RuntimeComponents,
    _: &mut ConfigBag,
  ) -> Result<(), BoxError> {
    let request = context.request_mut();
    if !request.headers().contains_key("content-length") {
      request.headers_mut().insert("content-length", "0");
    }
    Ok(())
  }
}

#[derive(Error, Debug)]
pub enum SourceError {
  #[error("GitHub tarball download failed: {0}")]
  Download(String),
  #[error("S3 operation failed: {0}")]
  S3(String),
  #[error("Presign configuration error: {0}")]
  Presign(String),
  #[error("Status not found: {0}")]
  NotFound(String),
}

#[derive(serde::Deserialize)]
pub struct NodeOutcome {
  pub success: bool,
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
      .interceptor(ContentLengthZeroInterceptor)
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

    let body = crab
      .repos(owner, repo)
      .download_tarball(sha.to_string())
      .await
      .map_err(|e| SourceError::Download(e.to_string()))?
      .into_body();

    let key = source_key(run_id);
    info!(run_id, %key, "Starting multipart upload to S3");

    let upload = self
      .s3
      .create_multipart_upload()
      .bucket(&self.bucket)
      .key(&key)
      .content_type("application/gzip")
      .send()
      .await
      .map_err(|e| SourceError::S3(e.to_string()))?;

    let upload_id = upload
      .upload_id()
      .ok_or_else(|| SourceError::S3("S3 did not return an upload ID".to_string()))?
      .to_string();

    match self.upload_parts(&key, &upload_id, body).await {
      Ok(parts) => {
        let completed = CompletedMultipartUpload::builder()
          .set_parts(Some(parts))
          .build();
        self
          .s3
          .complete_multipart_upload()
          .bucket(&self.bucket)
          .key(&key)
          .upload_id(&upload_id)
          .multipart_upload(completed)
          .send()
          .await
          .map_err(|e| SourceError::S3(e.to_string()))?;
      }
      Err(e) => {
        let _ = self
          .s3
          .abort_multipart_upload()
          .bucket(&self.bucket)
          .key(&key)
          .upload_id(&upload_id)
          .send()
          .await;
        return Err(e);
      }
    }

    info!(run_id, %key, "Source tarball uploaded successfully");
    Ok(())
  }

  async fn upload_parts<B>(
    &self,
    key: &str,
    upload_id: &str,
    mut body: B,
  ) -> Result<Vec<CompletedPart>, SourceError>
  where
    B: BodyExt + Unpin,
    B::Data: AsRef<[u8]>,
    B::Error: std::fmt::Display,
  {
    let mut parts = Vec::new();
    let mut part_number = 1i32;
    let mut buffer: Vec<u8> = Vec::new();

    loop {
      let body_done = loop {
        if buffer.len() >= PART_SIZE {
          break false;
        }
        match body.frame().await {
          None => break true,
          Some(Ok(frame)) => {
            if let Ok(chunk) = frame.into_data() {
              buffer.extend_from_slice(chunk.as_ref());
            }
          }
          Some(Err(e)) => return Err(SourceError::Download(e.to_string())),
        }
      };

      if buffer.is_empty() {
        break;
      }

      let part_vec: Vec<u8> = if buffer.len() > PART_SIZE {
        buffer.drain(..PART_SIZE).collect()
      } else {
        std::mem::take(&mut buffer)
      };
      let part_len = part_vec.len() as i64;

      let part = self
        .s3
        .upload_part()
        .bucket(&self.bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .content_length(part_len)
        .body(ByteStream::from(part_vec))
        .send()
        .await
        .map_err(|e| SourceError::S3(e.to_string()))?;

      parts.push(
        CompletedPart::builder()
          .e_tag(part.e_tag().unwrap_or_default())
          .part_number(part_number)
          .build(),
      );

      part_number += 1;

      if body_done {
        break;
      }
    }

    Ok(parts)
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
      Ok(_) => {}
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
      }
      Err(e) => return Err(SourceError::S3(e.to_string())),
    }
    self.apply_lifecycle_policy().await
  }

  async fn apply_lifecycle_policy(&self) -> Result<(), SourceError> {
    let expiration_days = PRESIGN_EXPIRES_SECS.div_ceil(86400) as i32;

    let rule = LifecycleRule::builder()
      .id("run-artifact-expiration")
      .filter(LifecycleRuleFilter::builder().prefix("runs/").build())
      .expiration(LifecycleExpiration::builder().days(expiration_days).build())
      .status(ExpirationStatus::Enabled)
      .build()
      .map_err(|e| SourceError::S3(e.to_string()))?;

    let config = BucketLifecycleConfiguration::builder()
      .rules(rule)
      .build()
      .map_err(|e| SourceError::S3(e.to_string()))?;

    self
      .s3
      .put_bucket_lifecycle_configuration()
      .bucket(&self.bucket)
      .lifecycle_configuration(config)
      .send()
      .await
      .map_err(|e| SourceError::S3(e.to_string()))?;
    Ok(())
  }

  pub async fn get_node_status(
    &self,
    run_id: &str,
    node_name: &str,
  ) -> Result<NodeOutcome, SourceError> {
    let key = status_key(run_id, node_name);
    match self
      .s3
      .get_object()
      .bucket(&self.bucket)
      .key(&key)
      .send()
      .await
    {
      Err(e)
        if e
          .as_service_error()
          .map(|se| se.is_no_such_key())
          .unwrap_or(false) =>
      {
        Err(SourceError::NotFound(format!(
          "run={run_id} node={node_name}"
        )))
      }
      Err(e) => Err(SourceError::S3(e.to_string())),
      Ok(output) => {
        let bytes = output
          .body
          .collect()
          .await
          .map_err(|e| SourceError::S3(e.to_string()))?
          .into_bytes();
        serde_json::from_slice::<NodeOutcome>(&bytes).map_err(|e| SourceError::S3(e.to_string()))
      }
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
  fn test_source_error_not_found_display() {
    let e = SourceError::NotFound("run=foo node=bar".to_string());
    assert!(e.to_string().contains("Status not found"));
  }

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
