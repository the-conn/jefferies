use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use pipelines::Pipeline;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
  Pending,
  Running,
  Success,
  Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
  pub version: u64,
  pub statuses: HashMap<String, NodeStatus>,
  pub dependencies: HashMap<String, Vec<String>>,
  pub pipeline: Pipeline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseData {
  pub version: u64,
  pub server_id: String,
}

#[derive(Debug, Error)]
pub enum StateStoreError {
  #[error("Redis error: {0}")]
  Redis(#[from] deadpool_redis::redis::RedisError),
  #[error("Pool error: {0}")]
  Pool(#[from] deadpool_redis::PoolError),
  #[error("Serialization error: {0}")]
  Serialization(#[from] serde_json::Error),
  #[error("Store error: {0}")]
  Store(String),
}

#[async_trait]
pub trait StateStore: Send + Sync {
  async fn save_run(
    &self,
    run_id: &str,
    state: &RunState,
    expected_version: u64,
  ) -> Result<bool, StateStoreError>;

  async fn load_run(&self, run_id: &str) -> Result<Option<RunState>, StateStoreError>;

  async fn delete_run(&self, run_id: &str) -> Result<(), StateStoreError>;

  async fn try_acquire_lease(
    &self,
    run_id: &str,
    server_id: &str,
    ttl_secs: u64,
  ) -> Result<Option<u64>, StateStoreError>;

  async fn renew_lease(
    &self,
    run_id: &str,
    server_id: &str,
    lease_version: u64,
    ttl_secs: u64,
  ) -> Result<bool, StateStoreError>;

  async fn release_lease(&self, run_id: &str) -> Result<(), StateStoreError>;

  async fn get_orphaned_runs(&self) -> Result<Vec<String>, StateStoreError>;
}

fn state_key(run_id: &str) -> String {
  format!("jefferies:run:{run_id}:state")
}

fn lease_key(run_id: &str) -> String {
  format!("jefferies:run:{run_id}:lease")
}

pub struct RedisStateStore {
  pool: deadpool_redis::Pool,
}

impl RedisStateStore {
  pub fn new(url: &str, password: &str, pool_size: usize) -> Result<Self, StateStoreError> {
    let full_url = if password.is_empty() {
      url.to_string()
    } else if url.starts_with("redis://") {
      format!("redis://:{password}@{}", &url[8..])
    } else {
      url.to_string()
    };
    let mut cfg = deadpool_redis::Config::from_url(full_url);
    cfg.pool = Some(deadpool_redis::PoolConfig {
      max_size: pool_size,
      ..Default::default()
    });
    let pool = cfg
      .create_pool(Some(deadpool_redis::Runtime::Tokio1))
      .map_err(|e| StateStoreError::Store(e.to_string()))?;
    Ok(Self { pool })
  }

  pub async fn ping(&self) -> Result<(), StateStoreError> {
    let mut conn = self.pool.get().await?;
    let _: String = deadpool_redis::redis::cmd("PING").query_async(&mut conn).await?;
    Ok(())
  }
}

#[async_trait]
impl StateStore for RedisStateStore {
  async fn save_run(
    &self,
    run_id: &str,
    state: &RunState,
    expected_version: u64,
  ) -> Result<bool, StateStoreError> {
    let mut conn = self.pool.get().await?;
    let key = state_key(run_id);
    let value = serde_json::to_string(state)?;

    let script = deadpool_redis::redis::Script::new(
      r#"
            local current = redis.call('GET', KEYS[1])
            if current == false then
                if tonumber(ARGV[2]) == 0 then
                    redis.call('SET', KEYS[1], ARGV[1])
                    return 1
                else
                    return 0
                end
            end
            local decoded = cjson.decode(current)
            if decoded.version == tonumber(ARGV[2]) then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            else
                return 0
            end
            "#,
    );

    let result: i64 = script
      .key(&key)
      .arg(&value)
      .arg(expected_version)
      .invoke_async(&mut conn)
      .await?;

    Ok(result == 1)
  }

  async fn load_run(&self, run_id: &str) -> Result<Option<RunState>, StateStoreError> {
    let mut conn = self.pool.get().await?;
    let key = state_key(run_id);
    let value: Option<String> = deadpool_redis::redis::cmd("GET").arg(&key).query_async(&mut conn).await?;
    match value {
      Some(json) => Ok(Some(serde_json::from_str(&json)?)),
      None => Ok(None),
    }
  }

  async fn delete_run(&self, run_id: &str) -> Result<(), StateStoreError> {
    let mut conn = self.pool.get().await?;
    let key = state_key(run_id);
    let _: i64 = deadpool_redis::redis::cmd("DEL").arg(&key).query_async(&mut conn).await?;
    Ok(())
  }

  async fn try_acquire_lease(
    &self,
    run_id: &str,
    server_id: &str,
    ttl_secs: u64,
  ) -> Result<Option<u64>, StateStoreError> {
    let mut conn = self.pool.get().await?;
    let key = lease_key(run_id);

    let script = deadpool_redis::redis::Script::new(
      r#"
            local existing = redis.call('GET', KEYS[1])
            if existing ~= false then
                return nil
            end
            local version = redis.call('INCR', KEYS[1] .. ':ver')
            local data = cjson.encode({version=version, server_id=ARGV[1]})
            redis.call('SET', KEYS[1], data)
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
            return version
            "#,
    );

    let result: Option<i64> = script
      .key(&key)
      .arg(server_id)
      .arg(ttl_secs)
      .invoke_async(&mut conn)
      .await?;

    Ok(result.map(|v| v as u64))
  }

  async fn renew_lease(
    &self,
    run_id: &str,
    server_id: &str,
    lease_version: u64,
    ttl_secs: u64,
  ) -> Result<bool, StateStoreError> {
    let mut conn = self.pool.get().await?;
    let key = lease_key(run_id);

    let script = deadpool_redis::redis::Script::new(
      r#"
            local current = redis.call('GET', KEYS[1])
            if current == false then
                return 0
            end
            local decoded = cjson.decode(current)
            if decoded.server_id == ARGV[1] and decoded.version == tonumber(ARGV[2]) then
                redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
                return 1
            end
            return 0
            "#,
    );

    let result: i64 = script
      .key(&key)
      .arg(server_id)
      .arg(lease_version)
      .arg(ttl_secs)
      .invoke_async(&mut conn)
      .await?;

    Ok(result == 1)
  }

  async fn release_lease(&self, run_id: &str) -> Result<(), StateStoreError> {
    let mut conn = self.pool.get().await?;
    let key = lease_key(run_id);
    let _: i64 = deadpool_redis::redis::cmd("DEL").arg(&key).query_async(&mut conn).await?;
    Ok(())
  }

  async fn get_orphaned_runs(&self) -> Result<Vec<String>, StateStoreError> {
    let mut conn = self.pool.get().await?;
    let pattern = "jefferies:run:*:state";

    let keys: Vec<String> = deadpool_redis::redis::cmd("KEYS")
      .arg(pattern)
      .query_async(&mut conn)
      .await?;

    let mut orphaned = vec![];
    for key in &keys {
      let parts: Vec<&str> = key.split(':').collect();
      if parts.len() < 4 {
        continue;
      }
      let run_id = parts[2];

      let value: Option<String> = deadpool_redis::redis::cmd("GET").arg(key).query_async(&mut conn).await?;
      let Some(json) = value else { continue };

      let Ok(state): Result<RunState, _> = serde_json::from_str(&json) else {
        continue;
      };

      let has_running = state.statuses.values().any(|s| *s == NodeStatus::Running);
      if !has_running {
        continue;
      }

      let lease_k = lease_key(run_id);
      let lease_exists: bool = deadpool_redis::redis::cmd("EXISTS")
        .arg(&lease_k)
        .query_async(&mut conn)
        .await
        .map(|n: i64| n > 0)
        .unwrap_or(false);

      if !lease_exists {
        debug!(run_id, "Found orphaned run");
        orphaned.push(run_id.to_string());
      }
    }

    Ok(orphaned)
  }
}

struct InMemoryRunEntry {
  state: RunState,
}

struct InMemoryLeaseEntry {
  server_id: String,
  version: u64,
}

pub struct InMemoryStateStore {
  runs: Mutex<HashMap<String, InMemoryRunEntry>>,
  leases: Mutex<HashMap<String, InMemoryLeaseEntry>>,
  lease_counter: Mutex<u64>,
}

impl InMemoryStateStore {
  pub fn new() -> Arc<Self> {
    Arc::new(Self::default())
  }
}

impl Default for InMemoryStateStore {
  fn default() -> Self {
    Self {
      runs: Mutex::new(HashMap::new()),
      leases: Mutex::new(HashMap::new()),
      lease_counter: Mutex::new(0),
    }
  }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
  async fn save_run(
    &self,
    run_id: &str,
    state: &RunState,
    expected_version: u64,
  ) -> Result<bool, StateStoreError> {
    let mut runs = self.runs.lock().await;
    match runs.get(run_id) {
      Some(entry) if entry.state.version != expected_version => Ok(false),
      None if expected_version != 0 => Ok(false),
      _ => {
        runs.insert(
          run_id.to_string(),
          InMemoryRunEntry {
            state: state.clone(),
          },
        );
        Ok(true)
      }
    }
  }

  async fn load_run(&self, run_id: &str) -> Result<Option<RunState>, StateStoreError> {
    let runs = self.runs.lock().await;
    Ok(runs.get(run_id).map(|e| e.state.clone()))
  }

  async fn delete_run(&self, run_id: &str) -> Result<(), StateStoreError> {
    self.runs.lock().await.remove(run_id);
    Ok(())
  }

  async fn try_acquire_lease(
    &self,
    run_id: &str,
    server_id: &str,
    _ttl_secs: u64,
  ) -> Result<Option<u64>, StateStoreError> {
    let mut leases = self.leases.lock().await;
    if leases.contains_key(run_id) {
      return Ok(None);
    }
    let mut counter = self.lease_counter.lock().await;
    *counter += 1;
    let version = *counter;
    leases.insert(
      run_id.to_string(),
      InMemoryLeaseEntry {
        server_id: server_id.to_string(),
        version,
      },
    );
    Ok(Some(version))
  }

  async fn renew_lease(
    &self,
    run_id: &str,
    server_id: &str,
    lease_version: u64,
    _ttl_secs: u64,
  ) -> Result<bool, StateStoreError> {
    let leases = self.leases.lock().await;
    match leases.get(run_id) {
      Some(entry) if entry.server_id == server_id && entry.version == lease_version => Ok(true),
      _ => Ok(false),
    }
  }

  async fn release_lease(&self, run_id: &str) -> Result<(), StateStoreError> {
    self.leases.lock().await.remove(run_id);
    Ok(())
  }

  async fn get_orphaned_runs(&self) -> Result<Vec<String>, StateStoreError> {
    let runs = self.runs.lock().await;
    let leases = self.leases.lock().await;
    let orphaned = runs
      .iter()
      .filter(|(run_id, entry)| {
        let has_running = entry
          .state
          .statuses
          .values()
          .any(|s| *s == NodeStatus::Running);
        has_running && !leases.contains_key(*run_id)
      })
      .map(|(run_id, _)| run_id.clone())
      .collect();
    Ok(orphaned)
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use pipelines::Pipeline;

  use super::*;

  fn make_pipeline() -> Pipeline {
    Pipeline::from_yaml(
      r#"
name: test-pipeline
on:
  push:
    branches: [main]
nodes:
  - name: build
    image: rust:latest
    steps:
      - cargo build
"#,
    )
    .unwrap()
  }

  fn make_run_state(version: u64) -> RunState {
    RunState {
      version,
      statuses: HashMap::from([("build".to_string(), NodeStatus::Pending)]),
      dependencies: HashMap::from([("build".to_string(), vec![])]),
      pipeline: make_pipeline(),
    }
  }

  #[tokio::test]
  async fn test_save_and_load_run() {
    let store = InMemoryStateStore::new();
    let state = make_run_state(0);
    let saved = store.save_run("run1", &state, 0).await.unwrap();
    assert!(saved);
    let loaded = store.load_run("run1").await.unwrap();
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().version, 0);
  }

  #[tokio::test]
  async fn test_version_fencing() {
    let store = InMemoryStateStore::new();
    let state = make_run_state(0);
    store.save_run("run1", &state, 0).await.unwrap();

    let state_v1 = make_run_state(1);
    let accepted = store.save_run("run1", &state_v1, 1).await.unwrap();
    assert!(!accepted, "Should reject wrong expected version");

    let accepted = store.save_run("run1", &state_v1, 0).await.unwrap();
    assert!(accepted, "Should accept correct expected version");
  }

  #[tokio::test]
  async fn test_lease_acquire_renew_release() {
    let store = InMemoryStateStore::new();
    let version = store
      .try_acquire_lease("run1", "server-a", 30)
      .await
      .unwrap();
    assert!(version.is_some());
    let v = version.unwrap();

    let renewed = store.renew_lease("run1", "server-a", v, 30).await.unwrap();
    assert!(renewed);

    let second = store
      .try_acquire_lease("run1", "server-b", 30)
      .await
      .unwrap();
    assert!(second.is_none(), "Should not acquire lease already held");

    store.release_lease("run1").await.unwrap();

    let third = store
      .try_acquire_lease("run1", "server-b", 30)
      .await
      .unwrap();
    assert!(third.is_some(), "Should acquire lease after release");
  }

  #[tokio::test]
  async fn test_orphaned_run_detection() {
    let store = InMemoryStateStore::new();

    let mut state = make_run_state(0);
    state
      .statuses
      .insert("build".to_string(), NodeStatus::Running);
    store.save_run("run1", &state, 0).await.unwrap();

    let orphans = store.get_orphaned_runs().await.unwrap();
    assert_eq!(orphans, vec!["run1"]);

    store
      .try_acquire_lease("run1", "server-a", 30)
      .await
      .unwrap();
    let orphans = store.get_orphaned_runs().await.unwrap();
    assert!(orphans.is_empty(), "Should not be orphaned when lease held");
  }
}
