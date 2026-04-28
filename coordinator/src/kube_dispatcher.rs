use std::{collections::BTreeMap, sync::Arc};

use app_config::AppConfig;
use async_trait::async_trait;
use k8s_openapi::{
  api::{
    batch::v1::{Job, JobSpec},
    core::v1::{
      ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar, PodSpec,
      PodTemplateSpec, ResourceRequirements, Volume, VolumeMount,
    },
  },
  apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::ObjectMeta},
};
use kube::api::{DeleteParams, ListParams, PostParams};
use pipelines::{NodeInfo, Pipeline};
use tracing::{info, warn};

use crate::{
  dispatcher::{DispatchError, Dispatcher},
  source_manager::SourceManager,
};

pub struct KubeDispatcher {
  source_manager: Arc<SourceManager>,
  client: kube::Client,
  namespace: String,
  tube_image: String,
  default_node_image: String,
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

fn build_env_vars(run_id: &str, node_name: &str, put_url: &str, get_url: &str) -> Vec<EnvVar> {
  let poke_url = format!(
    "http://jefferies.jefferies.svc.cluster.local./api/v1/runs/{run_id}/nodes/{node_name}/poke"
  );
  vec![
    env_var("TUBE__EXECUTION__USER_SCRIPT_PATH", "/etc/conn/script.sh"),
    env_var("TUBE__EXECUTION__RUN_ID", run_id),
    env_var("TUBE__EXECUTION__NODE_NAME", node_name),
    env_var("TUBE__EXECUTION__PUT_URL", put_url),
    env_var("TUBE__EXECUTION__POKE_URL", &poke_url),
    env_var("TUBE__LOG__LEVEL", "info"),
    env_var("TUBE__WORKSPACE__GET_URL", get_url),
    env_var("TUBE__WORKSPACE__DIR", "/workspace"),
  ]
}

fn resource_requirements() -> ResourceRequirements {
  ResourceRequirements {
    requests: Some(BTreeMap::from([
      ("cpu".to_string(), Quantity("1".to_string())),
      ("memory".to_string(), Quantity("2Gi".to_string())),
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
    _config: &AppConfig,
  ) -> Result<(), DispatchError> {
    let put_url = self
      .source_manager
      .put_status_url(run_id, &node.name)
      .await?;
    let get_url = self.source_manager.get_source_url(run_id).await?;

    let cm_name = configmap_name(run_id, &node.name);
    let script = build_script(&node.steps);
    self
      .create_script_configmap(run_id, &node.name, &cm_name, script)
      .await?;

    let labels = run_labels(run_id);
    let env_vars = build_env_vars(run_id, &node.name, &put_url, &get_url);
    let job = self.build_job(run_id, &node.name, node, &cm_name, labels, env_vars);

    let jobs: kube::Api<Job> = kube::Api::namespaced(self.client.clone(), &self.namespace);
    jobs
      .create(&PostParams::default(), &job)
      .await
      .map_err(|e| DispatchError::Kube(e.to_string()))?;

    info!(run_id, node_name = %node.name, "Dispatched Kubernetes Job");
    Ok(())
  }

  async fn cancel_node(
    &self,
    run_id: &str,
    node_name: &str,
    _config: &AppConfig,
  ) -> Result<(), DispatchError> {
    let jobs: kube::Api<Job> = kube::Api::namespaced(self.client.clone(), &self.namespace);
    if let Err(e) = jobs
      .delete(&job_name(run_id, node_name), &DeleteParams::background())
      .await
    {
      warn!(run_id, node_name, error = %e, "Failed to delete Job");
    }

    let cms: kube::Api<ConfigMap> = kube::Api::namespaced(self.client.clone(), &self.namespace);
    if let Err(e) = cms
      .delete(&configmap_name(run_id, node_name), &DeleteParams::default())
      .await
    {
      warn!(run_id, node_name, error = %e, "Failed to delete ConfigMap");
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
}

impl KubeDispatcher {
  fn build_job(
    &self,
    run_id: &str,
    node_name: &str,
    node: &NodeInfo,
    cm_name: &str,
    labels: BTreeMap<String, String>,
    env_vars: Vec<EnvVar>,
  ) -> Job {
    let image = if node.image.is_empty() {
      &self.default_node_image
    } else {
      &node.image
    };

    Job {
      metadata: ObjectMeta {
        name: Some(job_name(run_id, node_name)),
        namespace: Some(self.namespace.clone()),
        labels: Some(labels.clone()),
        ..Default::default()
      },
      spec: Some(JobSpec {
        active_deadline_seconds: node.timeout_secs.map(|t| t as i64),
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
              image: Some(image.to_string()),
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
  ) -> Result<(), DispatchError> {
    let cm = ConfigMap {
      metadata: ObjectMeta {
        name: Some(cm_name.to_string()),
        namespace: Some(self.namespace.clone()),
        labels: Some(run_labels(run_id)),
        ..Default::default()
      },
      data: Some(BTreeMap::from([("script.sh".to_string(), script)])),
      ..Default::default()
    };

    let cms: kube::Api<ConfigMap> = kube::Api::namespaced(self.client.clone(), &self.namespace);
    cms
      .create(&PostParams::default(), &cm)
      .await
      .map_err(|e| DispatchError::Kube(e.to_string()))?;

    info!(run_id, node_name, "Created script ConfigMap");
    Ok(())
  }
}
