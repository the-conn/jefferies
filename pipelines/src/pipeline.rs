use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineError {
  #[error("Failed to parse YAML: {0}")]
  YamlParseError(String),
  #[error("Node '{node}' has unknown dependency '{dependency}'")]
  UnknownDependency { node: String, dependency: String },
  #[error("Node '{node}' depends on itself")]
  SelfDependency { node: String },
  #[error("Node '{node}' of type 'exec' must have at least one step")]
  EmptySteps { node: String },
  #[error("Node '{node}' of type 'build' is missing required 'config' field")]
  MissingBuildConfig { node: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
  name: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  timeout_secs: Option<u64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  fail_fast: Option<bool>,
  #[serde(skip_serializing_if = "Option::is_none")]
  on: Option<PipelineTriggers>,
  #[serde(default, skip_serializing_if = "HashMap::is_empty")]
  env: HashMap<String, String>,
  nodes: Vec<PipelineNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PipelineTriggers {
  #[serde(
    default,
    skip_serializing_if = "Option::is_none",
    deserialize_with = "deserialize_trigger"
  )]
  push: Option<Refs>,
  #[serde(
    default,
    skip_serializing_if = "Option::is_none",
    deserialize_with = "deserialize_trigger"
  )]
  pull_request: Option<Refs>,
  #[serde(skip_serializing_if = "Option::is_none")]
  schedule: Option<String>,
}

fn deserialize_trigger<'de, D>(deserializer: D) -> Result<Option<Refs>, D::Error>
where
  D: Deserializer<'de>,
{
  let opt = Option::<Refs>::deserialize(deserializer)?;
  Ok(Some(opt.unwrap_or_default()))
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Refs {
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  branches: Vec<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  tags: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
enum NodeType {
  #[default]
  Exec,
  Build,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildConfig {
  pub containerfile: String,
  #[serde(default)]
  pub tags: Vec<String>,
  #[serde(default)]
  pub build_args: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum NodeKind {
  Exec,
  Build(BuildConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PipelineNode {
  name: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  image: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  timeout_secs: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  checkout: Option<bool>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  after: Vec<String>,
  #[serde(default, skip_serializing_if = "HashMap::is_empty")]
  env: HashMap<String, String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  steps: Vec<PipelineStep>,
  #[serde(default, rename = "type")]
  node_type: NodeType,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  config: Option<BuildConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum PipelineStep {
  Inline(String),
  Named(NamedStep),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NamedStep {
  name: String,
  run: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
  pub name: String,
  pub image: String,
  pub steps: Vec<String>,
  pub dependencies: Vec<String>,
  pub timeout_secs: Option<u64>,
  pub checkout: bool,
  pub env: HashMap<String, String>,
  pub kind: NodeKind,
}

impl Pipeline {
  pub fn from_yaml(yaml: &str) -> Result<Self, PipelineError> {
    match serde_saphyr::from_str(yaml) {
      Ok(pipeline) => {
        validate_dependencies(&pipeline)?;
        validate_node_types(&pipeline)?;
        Ok(pipeline)
      }
      Err(e) => Err(PipelineError::YamlParseError(e.to_string())),
    }
  }

  pub fn name(&self) -> &str {
    &self.name
  }

  pub fn pipeline_timeout_secs(&self) -> Option<u64> {
    self.timeout_secs
  }

  pub fn fail_fast_override(&self) -> Option<bool> {
    self.fail_fast
  }
  pub fn triggered_by_push(&self, branch: &str) -> bool {
    let Some(triggers) = &self.on else {
      return false;
    };
    let Some(push_trigger) = &triggers.push else {
      return false;
    };
    refs_match_branch(push_trigger, branch)
  }

  pub fn node_info(&self) -> Vec<NodeInfo> {
    self
      .nodes
      .iter()
      .map(|n| {
        let mut env = self.env.clone();
        env.extend(n.env.clone());
        let kind = match (&n.node_type, n.config.as_ref()) {
          (NodeType::Build, Some(config)) => NodeKind::Build(config.clone()),
          _ => NodeKind::Exec,
        };
        NodeInfo {
          name: n.name.clone(),
          image: n.image.clone().unwrap_or_default(),
          steps: n.steps.iter().map(step_command).collect(),
          dependencies: n.after.clone(),
          timeout_secs: n.timeout_secs,
          checkout: n.checkout.unwrap_or(false),
          env,
          kind,
        }
      })
      .collect()
  }

  pub fn triggered_by_pull_request(&self, branch: &str) -> bool {
    let Some(triggers) = &self.on else {
      return false;
    };
    let Some(pr_trigger) = &triggers.pull_request else {
      return false;
    };
    refs_match_branch(pr_trigger, branch)
  }
}

fn validate_node_types(pipeline: &Pipeline) -> Result<(), PipelineError> {
  for node in &pipeline.nodes {
    match node.node_type {
      NodeType::Exec if node.steps.is_empty() => {
        return Err(PipelineError::EmptySteps {
          node: node.name.clone(),
        });
      }
      NodeType::Build if node.config.is_none() => {
        return Err(PipelineError::MissingBuildConfig {
          node: node.name.clone(),
        });
      }
      _ => {}
    }
  }
  Ok(())
}

fn validate_dependencies(pipeline: &Pipeline) -> Result<(), PipelineError> {
  let node_names: HashSet<&str> = pipeline.nodes.iter().map(|n| n.name.as_str()).collect();
  for node in &pipeline.nodes {
    for dep in &node.after {
      if dep == &node.name {
        return Err(PipelineError::SelfDependency {
          node: node.name.clone(),
        });
      }
      if !node_names.contains(dep.as_str()) {
        return Err(PipelineError::UnknownDependency {
          node: node.name.clone(),
          dependency: dep.clone(),
        });
      }
    }
  }
  Ok(())
}

fn step_command(step: &PipelineStep) -> String {
  match step {
    PipelineStep::Inline(cmd) => cmd.clone(),
    PipelineStep::Named(ns) => ns.run.clone(),
  }
}

fn refs_match_branch(refs: &Refs, branch: &str) -> bool {
  let no_branch_filter = refs.branches.is_empty();
  no_branch_filter || refs.branches.iter().any(|b| b == branch)
}

#[cfg(test)]
mod tests {
  use std::{fs, path::PathBuf};

  use super::*;

  #[test]
  fn test_load_real_pipeline_file() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut workspace_root = crate_dir.to_path_buf();

    while !workspace_root.join(".jefferies").exists() {
      if !workspace_root.pop() {
        panic!(
          "Could not find .jefferies directory in any parent of {:?}",
          crate_dir
        );
      }
    }

    let path = workspace_root.join(".jefferies").join("pipeline.yaml");
    let yaml_content = fs::read_to_string(&path).expect("Should be able to read the pipeline file");

    let pipeline = Pipeline::from_yaml(&yaml_content)
      .expect("Should successfully parse .jefferies/pipeline.yaml");

    assert_eq!(pipeline.name, "Jefferies Pipeline");
    assert!(
      !pipeline.nodes.is_empty(),
      "Pipeline should have at least one node"
    );

    let first_node = &pipeline.nodes[0];
    assert!(
      first_node.image.as_deref().unwrap_or("").contains("rust"),
      "First node should use a Rust image"
    );

    assert!(first_node.checkout.is_some());
  }

  #[test]
  fn test_triggered_by_push_matching_branch() {
    let yaml = r#"
name: Test Pipeline
on:
  push:
    branches:
      - main
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert!(pipeline.triggered_by_push("main"));
    assert!(!pipeline.triggered_by_push("feature-branch"));
  }

  #[test]
  fn test_triggered_by_push_no_branch_filter() {
    let yaml = r#"
name: Test Pipeline
on:
  push:
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert!(pipeline.triggered_by_push("main"));
    assert!(pipeline.triggered_by_push("any-branch"));
  }

  #[test]
  fn test_triggered_by_pull_request_matching_branch() {
    let yaml = r#"
name: Test Pipeline
on:
  pull_request:
    branches:
      - main
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert!(pipeline.triggered_by_pull_request("main"));
    assert!(!pipeline.triggered_by_pull_request("feature-branch"));
  }

  #[test]
  fn test_not_triggered_without_matching_event() {
    let yaml = r#"
name: Test Pipeline
on:
  push:
    branches:
      - main
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert!(!pipeline.triggered_by_pull_request("main"));
  }

  #[test]
  fn test_not_triggered_without_on_block() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert!(!pipeline.triggered_by_push("main"));
    assert!(!pipeline.triggered_by_pull_request("main"));
  }

  #[test]
  fn test_node_info_image_defaults_to_empty_when_absent() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert_eq!(infos[0].image, "");
  }

  #[test]
  fn test_node_info_includes_image_and_steps() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
      - cargo test
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].name, "Build");
    assert_eq!(infos[0].image, "rust:latest");
    assert_eq!(infos[0].steps, vec!["cargo build", "cargo test"]);
    assert!(infos[0].timeout_secs.is_none());
    assert!(!infos[0].checkout);
  }

  #[test]
  fn test_node_info_per_node_timeout() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    timeout_secs: 120
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert_eq!(infos[0].timeout_secs, Some(120));
  }

  #[test]
  fn test_pipeline_timeout() {
    let yaml = r#"
name: Test Pipeline
timeout_secs: 7200
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert_eq!(pipeline.pipeline_timeout_secs(), Some(7200));
  }

  #[test]
  fn test_fail_fast_override_explicit_false() {
    let yaml = r#"
name: Test Pipeline
fail_fast: false
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert_eq!(pipeline.fail_fast_override(), Some(false));
  }

  #[test]
  fn test_fail_fast_override_explicit_true() {
    let yaml = r#"
name: Test Pipeline
fail_fast: true
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert_eq!(pipeline.fail_fast_override(), Some(true));
  }

  #[test]
  fn test_fail_fast_override_absent() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert_eq!(pipeline.fail_fast_override(), None);
  }

  #[test]
  fn test_node_info_checkout_defaults_to_false() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert!(!infos[0].checkout);
  }

  #[test]
  fn test_node_info_checkout_explicit_true() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    checkout: true
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert!(infos[0].checkout);
  }

  #[test]
  fn test_node_env_vars_override_pipeline_env_vars() {
    let yaml = r#"
name: Test Pipeline
env:
  SHARED: pipeline
  OVERRIDE: pipeline
nodes:
  - name: Build
    image: rust:latest
    env:
      OVERRIDE: node
      NODE_ONLY: present
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert_eq!(
      infos[0].env.get("SHARED").map(String::as_str),
      Some("pipeline")
    );
    assert_eq!(
      infos[0].env.get("OVERRIDE").map(String::as_str),
      Some("node")
    );
    assert_eq!(
      infos[0].env.get("NODE_ONLY").map(String::as_str),
      Some("present")
    );
  }

  #[test]
  fn test_pipeline_env_propagates_to_all_nodes() {
    let yaml = r#"
name: Test Pipeline
env:
  SHARED: value
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
  - name: Test
    image: rust:latest
    steps:
      - cargo test
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert_eq!(
      infos[0].env.get("SHARED").map(String::as_str),
      Some("value")
    );
    assert_eq!(
      infos[1].env.get("SHARED").map(String::as_str),
      Some("value")
    );
  }

  #[test]
  fn test_env_defaults_to_empty_when_absent() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert!(infos[0].env.is_empty());
  }

  #[test]
  fn test_unknown_dependency_returns_error() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
  - name: Test
    image: rust:latest
    after:
      - Typo
    steps:
      - cargo test
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(
      result,
      Err(PipelineError::UnknownDependency {
        ref node,
        ref dependency
      }) if node == "Test" && dependency == "Typo"
    ));
  }

  #[test]
  fn test_valid_dependencies_pass_validation() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
  - name: Test
    image: rust:latest
    after:
      - Build
    steps:
      - cargo test
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    assert_eq!(pipeline.node_info().len(), 2);
  }

  #[test]
  fn test_self_reference_returns_error() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    after:
      - Build
    steps:
      - cargo build
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(
      result,
      Err(PipelineError::SelfDependency { ref node }) if node == "Build"
    ));
  }

  #[test]
  fn test_build_node_parses_correctly() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build Image
    type: build
    checkout: true
    config:
      containerfile: Containerfile
      tags:
        - quay.io/the-conn/jefferies
      build_args:
        - RUST_TAG=1.95-slim
"#;
    let pipeline = Pipeline::from_yaml(yaml).unwrap();
    let infos = pipeline.node_info();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].name, "Build Image");
    assert!(infos[0].checkout);
    if let NodeKind::Build(config) = &infos[0].kind {
      assert_eq!(config.containerfile, "Containerfile");
      assert_eq!(config.tags, vec!["quay.io/the-conn/jefferies"]);
      assert_eq!(config.build_args, vec!["RUST_TAG=1.95-slim"]);
    } else {
      panic!("Expected NodeKind::Build");
    }
  }

  #[test]
  fn test_exec_node_without_steps_fails_validation() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(
      result,
      Err(PipelineError::EmptySteps { ref node }) if node == "Build"
    ));
  }

  #[test]
  fn test_build_node_without_config_fails_validation() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build Image
    type: build
    checkout: true
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(
      result,
      Err(PipelineError::MissingBuildConfig { ref node }) if node == "Build Image"
    ));
  }
}
