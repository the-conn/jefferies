use std::{collections::BTreeMap, sync::Arc};

use app_config::AppConfig;
use async_trait::async_trait;
use k8s_openapi::{
  api::{
    batch::v1::{Job, JobSpec},
    core::v1::{
      Capabilities, ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar,
      PodSecurityContext, PodSpec, PodTemplateSpec, ResourceRequirements, SecurityContext, Volume,
      VolumeMount,
    },
  },
  apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::ObjectMeta},
};
use kube::api::{DeleteParams, ListParams, PostParams};
use pipelines::{BuildConfig, NodeInfo, NodeKind, Pipeline};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{info, warn};

use crate::{
  dispatcher::{DispatchError, Dispatcher},
  pod_watcher::{PodSignal, PodWatcher, WatcherCommand},
  source_manager::{NodeOutcome, SourceError, SourceManager},
};

pub struct KubeDispatcher {
  source_manager: Arc<SourceManager>,
  client: kube::Client,
  namespace: String,
  tube_image: String,
  default_node_image: String,
  builder_namespace: String,
  buildah_image: String,
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
      builder_namespace: config.builder_namespace().to_string(),
      buildah_image: config.buildah_image().to_string(),
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

fn job_name(run_id: &str, node_name: &str) -> String {
  format!(
    "run-{}-{}",
    sanitize_k8s_name(run_id),
    sanitize_k8s_name(node_name)
  )
}

fn configmap_name(run_id: &str, node_name: &str) -> String {
  format!(
    "run-{}-{}-script",
    sanitize_k8s_name(run_id),
    sanitize_k8s_name(node_name)
  )
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

fn pod_labels(run_id: &str, node_name: &str) -> BTreeMap<String, String> {
  let mut labels = run_labels(run_id);
  labels.insert("the-conn.com/node-name".to_string(), node_name.to_string());
  labels
}

fn build_script(steps: &[String]) -> String {
  let mut script = String::from("#!/bin/bash\nset -euxo pipefail\n");
  for step in steps {
    script.push_str(step);
    script.push('\n');
  }
  script
}

fn build_buildah_script(config: &BuildConfig) -> String {
  let mut cmd = format!("buildah bud -f {}", config.containerfile);
  for tag in &config.tags {
    cmd.push_str(&format!(" -t {tag}"));
  }
  for arg in &config.build_args {
    cmd.push_str(&format!(" --build-arg {arg}"));
  }
  cmd.push_str(" .");
  format!("#!/bin/bash\nset -euxo pipefail\n{cmd}\n")
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

fn resource_requirements() -> ResourceRequirements {
  ResourceRequirements {
    requests: Some(BTreeMap::from([
      ("cpu".to_string(), Quantity("500m".to_string())),
      ("memory".to_string(), Quantity("1Gi".to_string())),
    ])),
    limits: Some(BTreeMap::from([
      ("cpu".to_string(), Quantity("2".to_string())),
      ("memory".to_string(), Quantity("4Gi".to_string())),
    ])),
    ..Default::default()
  }
}

#[async_trait]
impl Dispatcher for KubeDispatcher {
  async fn dispatch(
    &self,
    run_id: &str,
    node: &NodeInfo,
    _pipeline: &Pipeline,
    config: &AppConfig,
  ) -> Result<(), DispatchError> {
    let status_put_url = self
      .source_manager
      .put_status_url(run_id, &node.name)
      .await?;
    let logs_put_url = self.source_manager.put_logs_url(run_id, &node.name).await?;
    let get_url = self.source_manager.get_source_url(run_id).await?;

    let cm_name = configmap_name(run_id, &node.name);
    let labels = pod_labels(run_id, &node.name);
    let mut env_vars = build_env_vars(run_id, &node.name, &status_put_url, &logs_put_url, &get_url);
    env_vars.extend(node.env.iter().map(|(k, v)| env_var(k, v)));

    let safety_deadline_secs = compute_safety_deadline_secs(node, config);

    match &node.kind {
      NodeKind::Exec => {
        let script = build_script(&node.steps);
        self
          .create_script_configmap(run_id, &node.name, &cm_name, script, &self.namespace)
          .await?;
        let ctx = JobContext {
          run_id,
          node_name: &node.name,
          cm_name: &cm_name,
          labels,
          env_vars,
          safety_deadline_secs,
        };
        let job = self.build_job(ctx, node);
        let jobs: kube::Api<Job> = kube::Api::namespaced(self.client.clone(), &self.namespace);
        jobs
          .create(&PostParams::default(), &job)
          .await
          .map_err(|e| DispatchError::Kube(e.to_string()))?;
        info!(run_id, node_name = %node.name, "Dispatched Kubernetes Job");
      }
      NodeKind::Build(build_config) => {
        let script = build_buildah_script(build_config);
        self
          .create_script_configmap(
            run_id,
            &node.name,
            &cm_name,
            script,
            &self.builder_namespace,
          )
          .await?;
        env_vars.push(env_var("STORAGE_DRIVER", "vfs"));
        let ctx = JobContext {
          run_id,
          node_name: &node.name,
          cm_name: &cm_name,
          labels,
          env_vars,
          safety_deadline_secs,
        };
        let job = self.build_buildah_job(ctx);
        let jobs: kube::Api<Job> =
          kube::Api::namespaced(self.client.clone(), &self.builder_namespace);
        jobs
          .create(&PostParams::default(), &job)
          .await
          .map_err(|e| DispatchError::Kube(e.to_string()))?;
        info!(run_id, node_name = %node.name, "Dispatched buildah Job");
      }
    }

    Ok(())
  }

  async fn cancel_node(
    &self,
    run_id: &str,
    node_name: &str,
    _config: &AppConfig,
  ) -> Result<(), DispatchError> {
    for ns in [self.namespace.as_str(), self.builder_namespace.as_str()] {
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
    }

    Ok(())
  }

  async fn cleanup_run(&self, run_id: &str) -> Result<(), DispatchError> {
    let lp = ListParams::default().labels(&format!("the-conn.com/run-id={run_id}"));

    let jobs: kube::Api<Job> = kube::Api::namespaced(self.client.clone(), &self.namespace);
    if let Err(e) = jobs
      .delete_collection(&DeleteParams::background(), &lp)
      .await
    {
      warn!(run_id, error = %e, "Failed to delete Job collection");
    }

    let cms: kube::Api<ConfigMap> = kube::Api::namespaced(self.client.clone(), &self.namespace);
    if let Err(e) = cms.delete_collection(&DeleteParams::default(), &lp).await {
      warn!(run_id, error = %e, "Failed to delete ConfigMap collection");
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
      vec![self.namespace.clone(), self.builder_namespace.clone()],
      run_id.to_string(),
      signal_tx,
      cmd_rx,
    );
    Some(tokio::spawn(watcher.run()))
  }
}

impl KubeDispatcher {
  fn build_job(&self, ctx: JobContext<'_>, node: &NodeInfo) -> Job {
    let image = if node.image.is_empty() {
      self.default_node_image.clone()
    } else {
      node.image.clone()
    };
    let JobContext {
      run_id,
      node_name,
      cm_name,
      labels,
      env_vars,
      safety_deadline_secs,
    } = ctx;

    Job {
      metadata: ObjectMeta {
        name: Some(job_name(run_id, node_name)),
        namespace: Some(self.namespace.clone()),
        labels: Some(labels.clone()),
        ..Default::default()
      },
      spec: Some(JobSpec {
        active_deadline_seconds: Some(safety_deadline_secs),
        backoff_limit: Some(0),
        template: PodTemplateSpec {
          metadata: Some(ObjectMeta {
            labels: Some(labels),
            ..Default::default()
          }),
          spec: Some(PodSpec {
            restart_policy: Some("Never".to_string()),
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
              resources: Some(resource_requirements()),
              volume_mounts: Some(vec![
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
              ]),
              ..Default::default()
            }],
            volumes: Some(vec![
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
                  name: cm_name.to_string(),
                  default_mode: Some(0o755),
                  ..Default::default()
                }),
                ..Default::default()
              },
            ]),
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

  fn build_buildah_job(&self, ctx: JobContext<'_>) -> Job {
    let init_cmd = "cp /usr/local/bin/tube /shared/tube && \
      mkdir -p /var/lib/containers/storage /var/lib/containers/runroot";
    let JobContext {
      run_id,
      node_name,
      cm_name,
      labels,
      env_vars,
      safety_deadline_secs,
    } = ctx;

    Job {
      metadata: ObjectMeta {
        name: Some(job_name(run_id, node_name)),
        namespace: Some(self.builder_namespace.clone()),
        labels: Some(labels.clone()),
        ..Default::default()
      },
      spec: Some(JobSpec {
        active_deadline_seconds: Some(safety_deadline_secs),
        backoff_limit: Some(0),
        template: PodTemplateSpec {
          metadata: Some(ObjectMeta {
            labels: Some(labels),
            ..Default::default()
          }),
          spec: Some(PodSpec {
            restart_policy: Some("Never".to_string()),
            service_account_name: Some("pipelines-sa-userid-1000".to_string()),
            security_context: Some(PodSecurityContext {
              fs_group: Some(1000),
              ..Default::default()
            }),
            init_containers: Some(vec![Container {
              name: "tube-init".to_string(),
              image: Some(self.tube_image.clone()),
              command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                init_cmd.to_string(),
              ]),
              volume_mounts: Some(vec![
                VolumeMount {
                  name: "tube-bin".to_string(),
                  mount_path: "/shared".to_string(),
                  ..Default::default()
                },
                VolumeMount {
                  name: "varlibcontainers".to_string(),
                  mount_path: "/var/lib/containers".to_string(),
                  ..Default::default()
                },
              ]),
              ..Default::default()
            }]),
            containers: vec![Container {
              name: "buildah".to_string(),
              image: Some(self.buildah_image.clone()),
              command: Some(vec!["/shared/tube".to_string()]),
              args: Some(vec![]),
              env: Some(env_vars),
              resources: Some(resource_requirements()),
              security_context: Some(SecurityContext {
                run_as_user: Some(1000),
                run_as_group: Some(1000),
                allow_privilege_escalation: Some(true),
                capabilities: Some(Capabilities {
                  add: Some(vec![
                    "SETUID".to_string(),
                    "SETGID".to_string(),
                    "SETFCAP".to_string(),
                  ]),
                  ..Default::default()
                }),
                ..Default::default()
              }),
              volume_mounts: Some(vec![
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
                VolumeMount {
                  name: "varlibcontainers".to_string(),
                  mount_path: "/var/lib/containers".to_string(),
                  ..Default::default()
                },
                VolumeMount {
                  name: "storage-config".to_string(),
                  mount_path: "/home/build/.config/containers/storage.conf".to_string(),
                  sub_path: Some("storage.conf".to_string()),
                  ..Default::default()
                },
              ]),
              ..Default::default()
            }],
            volumes: Some(vec![
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
                  name: cm_name.to_string(),
                  default_mode: Some(0o755),
                  ..Default::default()
                }),
                ..Default::default()
              },
              Volume {
                name: "varlibcontainers".to_string(),
                empty_dir: Some(EmptyDirVolumeSource::default()),
                ..Default::default()
              },
              Volume {
                name: "storage-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                  name: "buildah-storage-config".to_string(),
                  ..Default::default()
                }),
                ..Default::default()
              },
            ]),
            ..Default::default()
          }),
        },
        ..Default::default()
      }),
      ..Default::default()
    }
  }
}
