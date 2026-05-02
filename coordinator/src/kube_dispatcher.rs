use std::{collections::BTreeMap, sync::Arc};

use app_config::AppConfig;
use async_trait::async_trait;
use k8s_openapi::{
  api::{
    batch::v1::{Job, JobSpec},
    core::v1::{
      ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar, PodSpec,
      PodTemplateSpec, ResourceRequirements, SecurityContext, Volume, VolumeMount,
    },
  },
  apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::ObjectMeta},
};
use kube::api::{DeleteParams, ListParams, PostParams};
use pipelines::{NodeInfo, Pipeline};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{info, warn};

use crate::{
  dispatcher::{DispatchError, Dispatcher},
  pod_watcher::{NODE_NAME_ANNOTATION, PodSignal, PodWatcher, WatcherCommand},
  source_manager::{NodeOutcome, SourceError, SourceManager},
};

pub struct KubeDispatcher {
  source_manager: Arc<SourceManager>,
  client: kube::Client,
  namespace: String,
  tube_image: String,
  default_node_image: String,
  service_account_privileged: String,
  service_account_default: String,
  vault_role: String,
  runtime_class: String,
  default_cpu: String,
  default_memory: String,
}

impl KubeDispatcher {
  pub async fn new(
    config: &AppConfig,
    source_manager: Arc<SourceManager>,
  ) -> Result<Self, DispatchError> {
    let client = kube::Client::try_default()
      .await
      .map_err(|e| DispatchError::Kube(e.to_string()))?;
    Ok(Self {
      source_manager,
      client,
      namespace: config.kubernetes_namespace().to_string(),
      tube_image: config.tube_image().to_string(),
      default_node_image: config.default_node_image().to_string(),
      service_account_privileged: config.service_account_privileged().to_string(),
      service_account_default: config.service_account_default().to_string(),
      vault_role: config.vault_role().to_string(),
      runtime_class: config.runtime_class().to_string(),
      default_cpu: config.default_node_cpu().to_string(),
      default_memory: config.default_node_memory().to_string(),
    })
  }
}

fn sanitize_k8s_name(s: &str) -> String {
  let sanitized: String = s
    .chars()
    .map(|c| {
      if c.is_ascii_alphanumeric() {
        c.to_ascii_lowercase()
      } else {
        '-'
      }
    })
    .collect();
  sanitized.trim_matches('-').to_string()
}

const RUN_ID_PREFIX_LEN: usize = 8;
const NODE_NAME_PREFIX_LEN: usize = 24;
const STABLE_HASH_LEN: usize = 8;

fn stable_hash(s: &str) -> String {
  use sha2::{Digest, Sha256};
  let digest = Sha256::digest(s.as_bytes());
  hex::encode(&digest[..STABLE_HASH_LEN / 2])
}

fn truncate_with_hash(s: &str, prefix_len: usize) -> String {
  let sanitized = sanitize_k8s_name(s);
  if sanitized.len() <= prefix_len + 1 + STABLE_HASH_LEN {
    return sanitized;
  }
  let prefix: String = sanitized.chars().take(prefix_len).collect();
  let trimmed = prefix.trim_end_matches('-');
  format!("{}-{}", trimmed, stable_hash(s))
}

fn short_run_id(run_id: &str) -> String {
  truncate_with_hash(run_id, RUN_ID_PREFIX_LEN)
}

fn short_node_name(node_name: &str) -> String {
  truncate_with_hash(node_name, NODE_NAME_PREFIX_LEN)
}

fn job_name(run_id: &str, node_name: &str) -> String {
  format!(
    "run-{}-{}",
    short_run_id(run_id),
    short_node_name(node_name)
  )
}

fn configmap_name(run_id: &str, node_name: &str) -> String {
  format!("{}-script", job_name(run_id, node_name))
}

fn run_labels(run_id: &str) -> BTreeMap<String, String> {
  BTreeMap::from([
    ("the-conn.com/run-id".to_string(), run_id.to_string()),
    (
      "the-conn.com/managed-by".to_string(),
      "jefferies".to_string(),
    ),
  ])
}

fn node_annotations(node_name: &str) -> BTreeMap<String, String> {
  BTreeMap::from([(NODE_NAME_ANNOTATION.to_string(), node_name.to_string())])
}

const VAULT_SECRETS_MOUNT_PATH: &str = "/etc/tube/secrets";

fn vault_primary_path(owner: &str, repo: &str) -> String {
  format!("secret/data/{owner}/{repo}")
}

fn vault_fallback_path(owner: &str) -> String {
  format!("secret/data/{owner}")
}

fn vault_template(owner: &str, repo: &str, key: &str) -> String {
  let primary = vault_primary_path(owner, repo);
  let fallback = vault_fallback_path(owner);
  format!(
    "{{{{- with secret \"{primary}\" -}}}}\n  {{{{ index .Data.data \"{key}\" }}}}\n{{{{- else -}}}}\n  {{{{- with secret \"{fallback}\" -}}}}\n    {{{{ index .Data.data \"{key}\" }}}}\n  {{{{- end -}}}}\n{{{{- end -}}}}"
  )
}

fn vault_annotations(
  secrets: &[String],
  owner: &str,
  repo: &str,
  role: &str,
) -> BTreeMap<String, String> {
  let mut out = BTreeMap::new();
  if secrets.is_empty() {
    return out;
  }
  out.insert(
    "vault.hashicorp.com/agent-inject".to_string(),
    "true".to_string(),
  );
  out.insert(
    "vault.hashicorp.com/agent-pre-populate-only".to_string(),
    "true".to_string(),
  );
  out.insert(
    "vault.hashicorp.com/secret-volume-path".to_string(),
    VAULT_SECRETS_MOUNT_PATH.to_string(),
  );
  out.insert("vault.hashicorp.com/role".to_string(), role.to_string());
  let primary = vault_primary_path(owner, repo);
  for key in secrets {
    out.insert(
      format!("vault.hashicorp.com/agent-inject-secret-{key}"),
      primary.clone(),
    );
    out.insert(
      format!("vault.hashicorp.com/agent-inject-template-{key}"),
      vault_template(owner, repo, key),
    );
  }
  out
}

fn log_vault_paths(run_id: &str, node_name: &str, secrets: &[String], owner: &str, repo: &str) {
  if secrets.is_empty() {
    return;
  }
  info!(
    run_id,
    node_name,
    primary_path = %vault_primary_path(owner, repo),
    fallback_path = %vault_fallback_path(owner),
    keys = ?secrets,
    "Vault Agent Injector annotations applied"
  );
}

fn build_script(steps: &[String]) -> String {
  let mut script = String::from("#!/bin/bash\nset -euxo pipefail\n");
  for step in steps {
    script.push_str(step);
    script.push('\n');
  }
  script
}

fn env_var(name: &str, value: &str) -> EnvVar {
  EnvVar {
    name: name.to_string(),
    value: Some(value.to_string()),
    ..Default::default()
  }
}

fn build_env_vars(
  run_id: &str,
  node_name: &str,
  status_put_url: &str,
  logs_put_url: &str,
  get_url: &str,
) -> Vec<EnvVar> {
  let poke_url = format!(
    "http://jefferies.jefferies.svc.cluster.local./api/v1/runs/{run_id}/nodes/{node_name}/poke"
  );
  vec![
    env_var("TUBE__EXECUTION__USER_SCRIPT_PATH", "/etc/conn/script.sh"),
    env_var("TUBE__EXECUTION__RUN_ID", run_id),
    env_var("TUBE__EXECUTION__NODE_NAME", node_name),
    env_var("TUBE__EXECUTION__STATUS_PUT_URL", status_put_url),
    env_var("TUBE__EXECUTION__LOGS_PUT_URL", logs_put_url),
    env_var("TUBE__EXECUTION__POKE_URL", &poke_url),
    env_var("TUBE__EXECUTION__LOG_LEVEL", "info"),
    env_var("TUBE__LOG__LEVEL", "warn"),
    env_var("TUBE__WORKSPACE__GET_URL", get_url),
    env_var("TUBE__WORKSPACE__DIR", "/workspace"),
  ]
}

const SAFETY_DEADLINE_BUFFER_SECS: u64 = 60;

struct JobContext<'a> {
  run_id: &'a str,
  node_name: &'a str,
  cm_name: &'a str,
  labels: BTreeMap<String, String>,
  annotations: BTreeMap<String, String>,
  env_vars: Vec<EnvVar>,
  safety_deadline_secs: i64,
}

fn compute_safety_deadline_secs(node: &NodeInfo, config: &AppConfig) -> i64 {
  let runtime = node
    .timeout_secs
    .unwrap_or_else(|| config.default_node_timeout_secs());
  let startup = node
    .startup_timeout_secs
    .unwrap_or_else(|| config.default_node_startup_timeout_secs());
  let total = startup
    .saturating_add(runtime)
    .saturating_add(SAFETY_DEADLINE_BUFFER_SECS);
  total.try_into().unwrap_or(i64::MAX)
}

fn resource_requirements(cpu: &str, memory: &str) -> ResourceRequirements {
  let map = BTreeMap::from([
    ("cpu".to_string(), Quantity(cpu.to_string())),
    ("memory".to_string(), Quantity(memory.to_string())),
  ]);
  ResourceRequirements {
    requests: Some(map.clone()),
    limits: Some(map),
    ..Default::default()
  }
}

const CACHE_MOUNT_PATH: &str = "/tmp/cache";

fn append_user_volumes(
  user_volumes: &std::collections::HashMap<String, String>,
  mounts: &mut Vec<VolumeMount>,
  volumes: &mut Vec<Volume>,
) {
  let mut names: Vec<&String> = user_volumes.keys().collect();
  names.sort();
  for name in names {
    let mount_path = &user_volumes[name];
    mounts.push(VolumeMount {
      name: name.clone(),
      mount_path: mount_path.clone(),
      ..Default::default()
    });
    volumes.push(Volume {
      name: name.clone(),
      empty_dir: Some(EmptyDirVolumeSource::default()),
      ..Default::default()
    });
  }
}

#[async_trait]
impl Dispatcher for KubeDispatcher {
  async fn dispatch(
    &self,
    run_id: &str,
    owner: &str,
    repo: &str,
    node: &NodeInfo,
    _pipeline: &Pipeline,
    config: &AppConfig,
  ) -> Result<(), DispatchError> {
    if !node.secrets.is_empty() && (owner.is_empty() || repo.is_empty()) {
      tracing::error!(
        run_id,
        node_name = %node.name,
        owner,
        repo,
        keys = ?node.secrets,
        "Vault injection requested but owner/repo missing on RunContext; failing dispatch"
      );
      return Err(DispatchError::Failed(
        "Vault injection requested but owner/repo missing on RunContext".to_string(),
      ));
    }

    let status_put_url = self
      .source_manager
      .put_status_url(run_id, &node.name)
      .await?;
    let logs_put_url = self.source_manager.put_logs_url(run_id, &node.name).await?;
    let get_url = self.source_manager.get_source_url(run_id).await?;

    let cm_name = configmap_name(run_id, &node.name);
    let labels = run_labels(run_id);
    let mut annotations = node_annotations(&node.name);
    let vault = vault_annotations(&node.secrets, owner, repo, &self.vault_role);
    if !vault.is_empty() {
      log_vault_paths(run_id, &node.name, &node.secrets, owner, repo);
      annotations.extend(vault);
    }
    let mut env_vars = build_env_vars(run_id, &node.name, &status_put_url, &logs_put_url, &get_url);
    env_vars.extend(node.env.iter().map(|(k, v)| env_var(k, v)));

    let safety_deadline_secs = compute_safety_deadline_secs(node, config);

    let script = build_script(&node.steps);
    self
      .create_script_configmap(run_id, &node.name, &cm_name, script, &self.namespace)
      .await?;
    let ctx = JobContext {
      run_id,
      node_name: &node.name,
      cm_name: &cm_name,
      labels,
      annotations,
      env_vars,
      safety_deadline_secs,
    };
    let job = self.build_job(ctx, node);
    self
      .create_job_or_rollback_configmap(&self.namespace, &job, &cm_name, run_id, &node.name)
      .await?;
    info!(run_id, node_name = %node.name, "Dispatched Kubernetes Job");

    Ok(())
  }

  async fn cancel_node(
    &self,
    run_id: &str,
    node_name: &str,
    _config: &AppConfig,
  ) -> Result<(), DispatchError> {
    let ns = self.namespace.as_str();
    let jobs: kube::Api<Job> = kube::Api::namespaced(self.client.clone(), ns);
    if let Err(e) = jobs
      .delete(&job_name(run_id, node_name), &DeleteParams::background())
      .await
    {
      warn!(run_id, node_name, namespace = ns, error = %e, "Failed to delete Job");
    }

    let cms: kube::Api<ConfigMap> = kube::Api::namespaced(self.client.clone(), ns);
    if let Err(e) = cms
      .delete(&configmap_name(run_id, node_name), &DeleteParams::default())
      .await
    {
      warn!(run_id, node_name, namespace = ns, error = %e, "Failed to delete ConfigMap");
    }

    Ok(())
  }

  async fn cleanup_run(&self, run_id: &str) -> Result<(), DispatchError> {
    let lp = ListParams::default().labels(&format!("the-conn.com/run-id={run_id}"));
    let ns = self.namespace.as_str();

    let jobs: kube::Api<Job> = kube::Api::namespaced(self.client.clone(), ns);
    if let Err(e) = jobs
      .delete_collection(&DeleteParams::background(), &lp)
      .await
    {
      warn!(run_id, namespace = ns, error = %e, "Failed to delete Job collection");
    }

    let cms: kube::Api<ConfigMap> = kube::Api::namespaced(self.client.clone(), ns);
    if let Err(e) = cms.delete_collection(&DeleteParams::default(), &lp).await {
      warn!(run_id, namespace = ns, error = %e, "Failed to delete ConfigMap collection");
    }

    self.source_manager.cleanup_run(run_id).await?;
    Ok(())
  }

  async fn get_node_outcome(
    &self,
    run_id: &str,
    node_name: &str,
  ) -> Result<Option<NodeOutcome>, DispatchError> {
    match self.source_manager.get_node_status(run_id, node_name).await {
      Ok(outcome) => Ok(Some(outcome)),
      Err(SourceError::NotFound(_)) => Ok(None),
      Err(e) => Err(DispatchError::Source(e)),
    }
  }

  async fn get_node_log(
    &self,
    run_id: &str,
    node_name: &str,
  ) -> Result<Option<String>, DispatchError> {
    self
      .source_manager
      .get_node_log(run_id, node_name)
      .await
      .map_err(DispatchError::Source)
  }

  async fn start_pod_watcher(
    &self,
    run_id: &str,
    signal_tx: mpsc::Sender<PodSignal>,
    cmd_rx: mpsc::Receiver<WatcherCommand>,
  ) -> Option<JoinHandle<()>> {
    let watcher = PodWatcher::new(
      self.client.clone(),
      vec![self.namespace.clone()],
      run_id.to_string(),
      signal_tx,
      cmd_rx,
    );
    Some(tokio::spawn(watcher.run()))
  }

  async fn list_managed_run_ids(&self) -> Result<Vec<String>, DispatchError> {
    use std::collections::HashSet;

    let lp = ListParams::default().labels("the-conn.com/managed-by=jefferies");
    let mut run_ids: HashSet<String> = HashSet::new();
    let ns = self.namespace.as_str();

    let jobs: kube::Api<Job> = kube::Api::namespaced(self.client.clone(), ns);
    let job_list = jobs
      .list(&lp)
      .await
      .map_err(|e| DispatchError::Kube(e.to_string()))?;
    for job in job_list.items {
      if let Some(labels) = job.metadata.labels.as_ref()
        && let Some(run_id) = labels.get("the-conn.com/run-id")
      {
        run_ids.insert(run_id.clone());
      }
    }

    let cms: kube::Api<ConfigMap> = kube::Api::namespaced(self.client.clone(), ns);
    let cm_list = cms
      .list(&lp)
      .await
      .map_err(|e| DispatchError::Kube(e.to_string()))?;
    for cm in cm_list.items {
      if let Some(labels) = cm.metadata.labels.as_ref()
        && let Some(run_id) = labels.get("the-conn.com/run-id")
      {
        run_ids.insert(run_id.clone());
      }
    }

    Ok(run_ids.into_iter().collect())
  }
}

impl KubeDispatcher {
  fn build_job(&self, ctx: JobContext<'_>, node: &NodeInfo) -> Job {
    let image = if node.image.is_empty() {
      self.default_node_image.clone()
    } else {
      node.image.clone()
    };
    let cpu = node.cpu.as_deref().unwrap_or(&self.default_cpu);
    let memory = node.memory.as_deref().unwrap_or(&self.default_memory);

    let mut user_volume_mounts = vec![
      VolumeMount {
        name: "tube-bin".to_string(),
        mount_path: "/shared".to_string(),
        ..Default::default()
      },
      VolumeMount {
        name: "workspace".to_string(),
        mount_path: "/workspace".to_string(),
        ..Default::default()
      },
      VolumeMount {
        name: "user-script".to_string(),
        mount_path: "/etc/conn/script.sh".to_string(),
        sub_path: Some("script.sh".to_string()),
        ..Default::default()
      },
    ];
    let mut volumes = vec![
      Volume {
        name: "tube-bin".to_string(),
        empty_dir: Some(EmptyDirVolumeSource::default()),
        ..Default::default()
      },
      Volume {
        name: "workspace".to_string(),
        empty_dir: Some(EmptyDirVolumeSource::default()),
        ..Default::default()
      },
      Volume {
        name: "user-script".to_string(),
        config_map: Some(ConfigMapVolumeSource {
          name: ctx.cm_name.to_string(),
          default_mode: Some(0o755),
          ..Default::default()
        }),
        ..Default::default()
      },
    ];
    if let Some(cache_size) = node.cache_size.as_deref() {
      user_volume_mounts.push(VolumeMount {
        name: "cache".to_string(),
        mount_path: CACHE_MOUNT_PATH.to_string(),
        ..Default::default()
      });
      volumes.push(Volume {
        name: "cache".to_string(),
        empty_dir: Some(EmptyDirVolumeSource {
          medium: Some("Memory".to_string()),
          size_limit: Some(Quantity(cache_size.to_string())),
        }),
        ..Default::default()
      });
    }

    append_user_volumes(&node.volumes, &mut user_volume_mounts, &mut volumes);

    let security_context = node.privileged.then(|| SecurityContext {
      privileged: Some(true),
      ..Default::default()
    });

    let JobContext {
      run_id,
      node_name,
      cm_name: _,
      labels,
      annotations,
      env_vars,
      safety_deadline_secs,
    } = ctx;

    Job {
      metadata: ObjectMeta {
        name: Some(job_name(run_id, node_name)),
        namespace: Some(self.namespace.clone()),
        labels: Some(labels.clone()),
        annotations: Some(annotations.clone()),
        ..Default::default()
      },
      spec: Some(JobSpec {
        active_deadline_seconds: Some(safety_deadline_secs),
        backoff_limit: Some(0),
        template: PodTemplateSpec {
          metadata: Some(ObjectMeta {
            labels: Some(labels),
            annotations: Some(annotations),
            ..Default::default()
          }),
          spec: Some(PodSpec {
            restart_policy: Some("Never".to_string()),
            runtime_class_name: node.privileged.then(|| self.runtime_class.clone()),
            service_account_name: Some(if node.privileged {
              self.service_account_privileged.clone()
            } else {
              self.service_account_default.clone()
            }),
            init_containers: Some(vec![Container {
              name: "tube-init".to_string(),
              image: Some(self.tube_image.clone()),
              command: Some(vec![
                "cp".to_string(),
                "/usr/local/bin/tube".to_string(),
                "/shared/tube".to_string(),
              ]),
              volume_mounts: Some(vec![VolumeMount {
                name: "tube-bin".to_string(),
                mount_path: "/shared".to_string(),
                ..Default::default()
              }]),
              ..Default::default()
            }]),
            containers: vec![Container {
              name: "tube".to_string(),
              image: Some(image),
              command: Some(vec!["/shared/tube".to_string()]),
              args: Some(vec![]),
              env: Some(env_vars),
              resources: Some(resource_requirements(cpu, memory)),
              security_context,
              volume_mounts: Some(user_volume_mounts),
              ..Default::default()
            }],
            volumes: Some(volumes),
            ..Default::default()
          }),
        },
        ..Default::default()
      }),
      ..Default::default()
    }
  }

  async fn create_script_configmap(
    &self,
    run_id: &str,
    node_name: &str,
    cm_name: &str,
    script: String,
    namespace: &str,
  ) -> Result<(), DispatchError> {
    let cm = ConfigMap {
      metadata: ObjectMeta {
        name: Some(cm_name.to_string()),
        namespace: Some(namespace.to_string()),
        labels: Some(run_labels(run_id)),
        ..Default::default()
      },
      data: Some(BTreeMap::from([("script.sh".to_string(), script)])),
      ..Default::default()
    };

    let cms: kube::Api<ConfigMap> = kube::Api::namespaced(self.client.clone(), namespace);
    cms
      .create(&PostParams::default(), &cm)
      .await
      .map_err(|e| DispatchError::Kube(e.to_string()))?;

    info!(run_id, node_name, "Created script ConfigMap");
    Ok(())
  }

  async fn create_job_or_rollback_configmap(
    &self,
    namespace: &str,
    job: &Job,
    cm_name: &str,
    run_id: &str,
    node_name: &str,
  ) -> Result<(), DispatchError> {
    let jobs: kube::Api<Job> = kube::Api::namespaced(self.client.clone(), namespace);
    match jobs.create(&PostParams::default(), job).await {
      Ok(_) => Ok(()),
      Err(e) => {
        let cms: kube::Api<ConfigMap> = kube::Api::namespaced(self.client.clone(), namespace);
        if let Err(cm_err) = cms.delete(cm_name, &DeleteParams::default()).await {
          warn!(
            run_id,
            node_name,
            namespace,
            error = %cm_err,
            "Failed to roll back ConfigMap after Job creation error"
          );
        }
        Err(DispatchError::Kube(e.to_string()))
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const K8S_DNS_LABEL_MAX: usize = 63;

  #[test]
  fn short_node_name_truncates_long_names_with_stable_hash() {
    let long = "Format, Lint, Test, Deploy, Notify, Cleanup, And A Lot More Words";
    let a = short_node_name(long);
    let b = short_node_name(long);
    assert_eq!(a, b, "hash must be deterministic across calls");
    assert!(
      a.len() <= NODE_NAME_PREFIX_LEN + 1 + STABLE_HASH_LEN,
      "got {} ({} chars)",
      a,
      a.len()
    );
    assert!(!a.starts_with('-') && !a.ends_with('-'));
  }

  #[test]
  fn short_node_name_passes_through_short_names_untouched() {
    assert_eq!(short_node_name("build"), "build");
    assert_eq!(
      short_node_name("Format, Lint and Test"),
      "format--lint-and-test"
    );
  }

  #[test]
  fn short_node_name_disambiguates_names_that_truncate_to_the_same_prefix() {
    let a = short_node_name("Format-Lint-And-Test-Run-One-Of-Many-Different-Names");
    let b = short_node_name("Format-Lint-And-Test-Run-Two-Of-Many-Different-Names");
    assert_ne!(
      a, b,
      "different inputs must produce different shortened ids"
    );
  }

  #[test]
  fn job_and_configmap_names_fit_under_dns_label_limit() {
    let run_id = "abc12345-6789-0abc-def1-23456789abcd";
    let long_node = "Format, Lint, Test, Deploy, Notify, Cleanup, And A Lot More Words";
    let job = job_name(run_id, long_node);
    let cm = configmap_name(run_id, long_node);
    assert!(
      job.len() <= K8S_DNS_LABEL_MAX,
      "job_name length {} should be <= {} ({})",
      job.len(),
      K8S_DNS_LABEL_MAX,
      job
    );
    assert!(
      cm.len() <= K8S_DNS_LABEL_MAX,
      "configmap_name length {} should be <= {} ({})",
      cm.len(),
      K8S_DNS_LABEL_MAX,
      cm
    );
  }

  #[test]
  fn append_user_volumes_creates_paired_mount_and_empty_dir() {
    let mut user_volumes = std::collections::HashMap::new();
    user_volumes.insert(
      "var-lib-containers".to_string(),
      "/var/lib/containers".to_string(),
    );
    user_volumes.insert(
      "buildah-cache".to_string(),
      "/var/cache/buildah".to_string(),
    );
    let mut mounts = Vec::new();
    let mut volumes = Vec::new();

    append_user_volumes(&user_volumes, &mut mounts, &mut volumes);

    assert_eq!(mounts.len(), 2);
    assert_eq!(volumes.len(), 2);
    let by_name: std::collections::HashMap<_, _> = mounts
      .iter()
      .map(|m| (m.name.as_str(), m.mount_path.as_str()))
      .collect();
    assert_eq!(
      by_name.get("var-lib-containers").copied(),
      Some("/var/lib/containers")
    );
    assert_eq!(
      by_name.get("buildah-cache").copied(),
      Some("/var/cache/buildah")
    );
    for vol in &volumes {
      assert!(
        vol.empty_dir.is_some(),
        "user volume {} must use emptyDir",
        vol.name
      );
      assert!(
        vol.empty_dir.as_ref().unwrap().medium.is_none(),
        "default empty_dir must be disk-backed"
      );
      assert!(
        vol.empty_dir.as_ref().unwrap().size_limit.is_none(),
        "default empty_dir must have no size limit"
      );
    }
  }

  #[test]
  fn append_user_volumes_is_a_noop_for_empty_input() {
    let user_volumes: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut mounts = Vec::new();
    let mut volumes = Vec::new();
    append_user_volumes(&user_volumes, &mut mounts, &mut volumes);
    assert!(mounts.is_empty());
    assert!(volumes.is_empty());
  }

  #[test]
  fn append_user_volumes_emits_in_deterministic_order() {
    let mut user_volumes = std::collections::HashMap::new();
    user_volumes.insert("zeta".to_string(), "/zeta".to_string());
    user_volumes.insert("alpha".to_string(), "/alpha".to_string());
    user_volumes.insert("mu".to_string(), "/mu".to_string());
    let mut mounts = Vec::new();
    let mut volumes = Vec::new();
    append_user_volumes(&user_volumes, &mut mounts, &mut volumes);
    let names: Vec<&str> = mounts.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, vec!["alpha", "mu", "zeta"]);
  }

  #[test]
  fn vault_annotations_empty_when_no_secrets() {
    let map = vault_annotations(&[], "the-conn", "jefferies", "the-conn-role");
    assert!(map.is_empty());
  }

  #[test]
  fn vault_annotations_emits_global_and_per_key_entries() {
    let secrets = vec!["QUAY_USERNAME".to_string(), "QUAY_PASSWORD".to_string()];
    let map = vault_annotations(&secrets, "the-conn", "jefferies", "the-conn-role");

    assert_eq!(
      map
        .get("vault.hashicorp.com/agent-inject")
        .map(String::as_str),
      Some("true")
    );
    assert_eq!(
      map
        .get("vault.hashicorp.com/agent-pre-populate-only")
        .map(String::as_str),
      Some("true")
    );
    assert_eq!(
      map
        .get("vault.hashicorp.com/secret-volume-path")
        .map(String::as_str),
      Some("/etc/tube/secrets")
    );
    assert_eq!(
      map.get("vault.hashicorp.com/role").map(String::as_str),
      Some("the-conn-role")
    );

    for key in &secrets {
      let secret_key = format!("vault.hashicorp.com/agent-inject-secret-{key}");
      assert_eq!(
        map.get(&secret_key).map(String::as_str),
        Some("secret/data/the-conn/jefferies"),
        "expected secret annotation for {key}"
      );
      let template_key = format!("vault.hashicorp.com/agent-inject-template-{key}");
      let template = map
        .get(&template_key)
        .unwrap_or_else(|| panic!("expected template annotation for {key}"));
      assert!(
        template.contains("secret/data/the-conn/jefferies"),
        "template missing primary path: {template}"
      );
      assert!(
        template.contains("secret/data/the-conn"),
        "template missing fallback path: {template}"
      );
      assert!(
        template.contains(&format!("index .Data.data \"{key}\"")),
        "template missing key index: {template}"
      );
    }
  }

  #[test]
  fn short_run_id_truncates_uuid_to_predictable_length() {
    let run_id = "abc12345-6789-0abc-def1-23456789abcd";
    let short = short_run_id(run_id);
    assert_eq!(short.len(), RUN_ID_PREFIX_LEN + 1 + STABLE_HASH_LEN);
    assert_eq!(short_run_id(run_id), short, "must be deterministic");
  }
}
