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
  #[error("Node '{node}' must have at least one step")]
  EmptySteps { node: String },
  #[error("Node '{node}' has invalid quantity for '{field}': '{value}'")]
  InvalidQuantity {
    node: String,
    field: &'static str,
    value: String,
  },
  #[error("Node '{node}' has invalid volume '{name}': {reason}")]
  InvalidVolume {
    node: String,
    name: String,
    reason: String,
  },
  #[error("Node '{node}' has invalid secret '{name}': {reason}")]
  InvalidSecret {
    node: String,
    name: String,
    reason: String,
  },
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
  #[serde(default, skip_serializing_if = "Secrets::is_empty")]
  secrets: Secrets,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PipelineNode {
  name: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  image: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  timeout_secs: Option<u64>,
  #[serde(skip_serializing_if = "Option::is_none")]
  startup_timeout_secs: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  checkout: Option<bool>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  after: Vec<String>,
  #[serde(default, skip_serializing_if = "HashMap::is_empty")]
  env: HashMap<String, String>,
  #[serde(default, skip_serializing_if = "Secrets::is_empty")]
  secrets: Secrets,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  steps: Vec<PipelineStep>,
  #[serde(default, skip_serializing_if = "is_false")]
  privileged: bool,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  cache_size: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  cpu: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  memory: Option<String>,
  #[serde(default, skip_serializing_if = "HashMap::is_empty")]
  volumes: HashMap<String, String>,
}

fn is_false(b: &bool) -> bool {
  !*b
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Secrets {
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  env: Vec<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  files: Vec<FileSecret>,
}

impl Secrets {
  fn is_empty(&self) -> bool {
    self.env.is_empty() && self.files.is_empty()
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum FileSecret {
  Default(String),
  Custom { name: String, path: String },
}

impl FileSecret {
  fn name(&self) -> &str {
    match self {
      FileSecret::Default(name) => name,
      FileSecret::Custom { name, .. } => name,
    }
  }

  fn path(&self) -> Option<&str> {
    match self {
      FileSecret::Default(_) => None,
      FileSecret::Custom { path, .. } => Some(path),
    }
  }
}

#[derive(Debug, Clone, Serialize)]
pub struct FileSecretSpec {
  pub name: String,
  pub path: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
  pub name: String,
  pub image: String,
  pub steps: Vec<String>,
  pub dependencies: Vec<String>,
  pub timeout_secs: Option<u64>,
  pub startup_timeout_secs: Option<u64>,
  pub checkout: bool,
  pub env: HashMap<String, String>,
  pub env_secrets: Vec<String>,
  pub file_secrets: Vec<FileSecretSpec>,
  pub privileged: bool,
  pub cache_size: Option<String>,
  pub cpu: Option<String>,
  pub memory: Option<String>,
  pub volumes: HashMap<String, String>,
}

impl Pipeline {
  pub fn from_yaml(yaml: &str) -> Result<Self, PipelineError> {
    match serde_saphyr::from_str(yaml) {
      Ok(pipeline) => {
        validate_dependencies(&pipeline)?;
        validate_steps_present(&pipeline)?;
        validate_quantities(&pipeline)?;
        validate_volumes(&pipeline)?;
        validate_secrets(&pipeline)?;
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
        let env_secrets = merge_env_secrets(&self.secrets.env, &n.secrets.env);
        let file_secrets = merge_file_secrets(&self.secrets.files, &n.secrets.files);
        NodeInfo {
          name: n.name.clone(),
          image: n.image.clone().unwrap_or_default(),
          steps: n.steps.iter().map(step_command).collect(),
          dependencies: n.after.clone(),
          timeout_secs: n.timeout_secs,
          startup_timeout_secs: n.startup_timeout_secs,
          checkout: n.checkout.unwrap_or(false),
          env,
          env_secrets,
          file_secrets,
          privileged: n.privileged,
          cache_size: n.cache_size.clone(),
          cpu: n.cpu.clone(),
          memory: n.memory.clone(),
          volumes: n.volumes.clone(),
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

fn validate_steps_present(pipeline: &Pipeline) -> Result<(), PipelineError> {
  for node in &pipeline.nodes {
    if node.steps.is_empty() {
      return Err(PipelineError::EmptySteps {
        node: node.name.clone(),
      });
    }
  }
  Ok(())
}

fn validate_quantities(pipeline: &Pipeline) -> Result<(), PipelineError> {
  for node in &pipeline.nodes {
    for (field, value) in [
      ("cpu", node.cpu.as_deref()),
      ("memory", node.memory.as_deref()),
      ("cache_size", node.cache_size.as_deref()),
    ] {
      if let Some(v) = value
        && !is_valid_quantity(v)
      {
        return Err(PipelineError::InvalidQuantity {
          node: node.name.clone(),
          field,
          value: v.to_string(),
        });
      }
    }
  }
  Ok(())
}

pub const RESERVED_VOLUME_NAMES: &[&str] = &["tube-bin", "workspace", "user-script", "cache"];
pub const RESERVED_MOUNT_PATHS: &[&str] = &["/shared", "/workspace", "/etc/conn", "/tmp/cache"];

fn validate_volumes(pipeline: &Pipeline) -> Result<(), PipelineError> {
  for node in &pipeline.nodes {
    let mut seen_paths: HashSet<&str> = HashSet::new();
    for (name, mount_path) in &node.volumes {
      if !is_valid_dns1123_label(name) {
        return Err(PipelineError::InvalidVolume {
          node: node.name.clone(),
          name: name.clone(),
          reason: "name must be a valid DNS-1123 label (lowercase alphanumeric and '-', \
                   1-63 chars, must start and end with an alphanumeric character)"
            .to_string(),
        });
      }
      if RESERVED_VOLUME_NAMES.contains(&name.as_str()) {
        return Err(PipelineError::InvalidVolume {
          node: node.name.clone(),
          name: name.clone(),
          reason: format!("name '{name}' is reserved for internal use"),
        });
      }
      if !mount_path.starts_with('/') {
        return Err(PipelineError::InvalidVolume {
          node: node.name.clone(),
          name: name.clone(),
          reason: format!("mount path '{mount_path}' must be absolute"),
        });
      }
      if RESERVED_MOUNT_PATHS
        .iter()
        .any(|reserved| is_reserved_path(mount_path, reserved))
      {
        return Err(PipelineError::InvalidVolume {
          node: node.name.clone(),
          name: name.clone(),
          reason: format!("mount path '{mount_path}' overlaps a reserved mount"),
        });
      }
      if !seen_paths.insert(mount_path.as_str()) {
        return Err(PipelineError::InvalidVolume {
          node: node.name.clone(),
          name: name.clone(),
          reason: format!("mount path '{mount_path}' is used by another volume on this node"),
        });
      }
    }
  }
  Ok(())
}

fn is_reserved_path(candidate: &str, reserved: &str) -> bool {
  candidate == reserved
    || candidate.starts_with(&format!("{reserved}/"))
    || reserved.starts_with(&format!("{candidate}/"))
}

const MAX_SECRET_NAME_LEN: usize = 30;

fn merge_env_secrets(pipeline_env: &[String], node_env: &[String]) -> Vec<String> {
  let mut set: std::collections::BTreeSet<String> = pipeline_env.iter().cloned().collect();
  set.extend(node_env.iter().cloned());
  set.into_iter().collect()
}

fn merge_file_secrets(
  pipeline_files: &[FileSecret],
  node_files: &[FileSecret],
) -> Vec<FileSecretSpec> {
  let mut by_name: std::collections::BTreeMap<String, FileSecretSpec> =
    std::collections::BTreeMap::new();
  for file in pipeline_files.iter().chain(node_files.iter()) {
    by_name.insert(
      file.name().to_string(),
      FileSecretSpec {
        name: file.name().to_string(),
        path: file.path().map(str::to_string),
      },
    );
  }
  by_name.into_values().collect()
}

fn validate_secrets(pipeline: &Pipeline) -> Result<(), PipelineError> {
  for node in &pipeline.nodes {
    for name in merge_env_secrets(&pipeline.secrets.env, &node.secrets.env) {
      if let Some(reason) = secret_name_violation(&name) {
        return Err(PipelineError::InvalidSecret {
          node: node.name.clone(),
          name,
          reason,
        });
      }
    }
    for spec in merge_file_secrets(&pipeline.secrets.files, &node.secrets.files) {
      if let Some(reason) = secret_name_violation(&spec.name) {
        return Err(PipelineError::InvalidSecret {
          node: node.name.clone(),
          name: spec.name,
          reason,
        });
      }
      if let Some(path) = spec.path.as_deref()
        && let Some(reason) = file_secret_path_violation(path)
      {
        return Err(PipelineError::InvalidSecret {
          node: node.name.clone(),
          name: spec.name,
          reason,
        });
      }
    }
  }
  Ok(())
}

fn secret_name_violation(name: &str) -> Option<String> {
  if name.is_empty() {
    return Some("name must not be empty".to_string());
  }
  if name.len() > MAX_SECRET_NAME_LEN {
    return Some(format!(
      "name must be at most {MAX_SECRET_NAME_LEN} characters"
    ));
  }
  if !is_valid_env_var_name(name) {
    return Some("name must match POSIX env-var pattern: [A-Za-z_][A-Za-z0-9_]*".to_string());
  }
  None
}

fn file_secret_path_violation(path: &str) -> Option<String> {
  if !path.starts_with('/') {
    return Some(format!("path '{path}' must be absolute"));
  }
  if path.ends_with('/') {
    return Some(format!("path '{path}' must not end with '/'"));
  }
  if path.contains("//") {
    return Some(format!("path '{path}' must not contain '//'"));
  }
  if path.split('/').any(|segment| segment == "..") {
    return Some(format!("path '{path}' must not contain '..' segments"));
  }
  None
}

fn is_valid_env_var_name(s: &str) -> bool {
  let bytes = s.as_bytes();
  if bytes.is_empty() {
    return false;
  }
  let is_first = |b: u8| b.is_ascii_alphabetic() || b == b'_';
  let is_rest = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
  if !is_first(bytes[0]) {
    return false;
  }
  bytes[1..].iter().all(|&b| is_rest(b))
}

fn is_valid_dns1123_label(s: &str) -> bool {
  if s.is_empty() || s.len() > 63 {
    return false;
  }
  let bytes = s.as_bytes();
  let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
  if !is_alnum(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
    return false;
  }
  bytes.iter().all(|&b| is_alnum(b) || b == b'-')
}

fn is_valid_quantity(s: &str) -> bool {
  if s.is_empty() {
    return false;
  }
  let bytes = s.as_bytes();
  let mut i = 0;
  if matches!(bytes[i], b'+' | b'-') {
    i += 1;
  }
  let mut saw_digit = false;
  let mut saw_dot = false;
  while i < bytes.len() {
    match bytes[i] {
      b'0'..=b'9' => {
        saw_digit = true;
        i += 1;
      }
      b'.' if !saw_dot => {
        saw_dot = true;
        i += 1;
      }
      _ => break,
    }
  }
  if !saw_digit {
    return false;
  }
  if i == bytes.len() {
    return true;
  }
  let is_exponent = matches!(bytes[i], b'e' | b'E')
    && i + 1 < bytes.len()
    && matches!(bytes[i + 1], b'0'..=b'9' | b'+' | b'-');
  if is_exponent {
    i += 1;
    if matches!(bytes[i], b'+' | b'-') {
      i += 1;
    }
    let mut any = false;
    while i < bytes.len() {
      if !bytes[i].is_ascii_digit() {
        return false;
      }
      any = true;
      i += 1;
    }
    return any;
  }
  match bytes[i] {
    b'n' | b'u' | b'm' | b'k' => {
      i += 1;
      i == bytes.len()
    }
    b'K' | b'M' | b'G' | b'T' | b'P' | b'E' => {
      i += 1;
      if i < bytes.len() && bytes[i] == b'i' {
        i += 1;
      }
      i == bytes.len()
    }
    _ => false,
  }
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

    let path = workspace_root.join(".jefferies").join("pr.yaml");
    let yaml_content = fs::read_to_string(&path).expect("Should be able to read the pipeline file");

    let pipeline =
      Pipeline::from_yaml(&yaml_content).expect("Should successfully parse .jefferies/pr.yaml");

    assert_eq!(pipeline.name, "Jefferies Main PR Pipeline");
    assert!(
      !pipeline.nodes.is_empty(),
      "Pipeline should have at least one node"
    );

    let first_node = &pipeline.nodes[0];
    assert!(
      first_node.image.as_deref().is_some_and(|s| !s.is_empty()),
      "First node should declare a non-empty image"
    );
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
  fn test_node_without_steps_fails_validation() {
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
  fn test_node_info_runtime_knobs_default_when_absent() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    assert!(!infos[0].privileged);
    assert!(infos[0].cache_size.is_none());
    assert!(infos[0].cpu.is_none());
    assert!(infos[0].memory.is_none());
  }

  #[test]
  fn test_node_info_runtime_knobs_parse_when_present() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build Image
    image: quay.io/buildah/stable:latest
    privileged: true
    cache_size: 2Gi
    cpu: "2"
    memory: 4Gi
    steps:
      - buildah bud .
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    assert!(infos[0].privileged);
    assert_eq!(infos[0].cache_size.as_deref(), Some("2Gi"));
    assert_eq!(infos[0].cpu.as_deref(), Some("2"));
    assert_eq!(infos[0].memory.as_deref(), Some("4Gi"));
  }

  #[test]
  fn test_invalid_quantity_rejected() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    cpu: "garbage"
    steps:
      - cargo build
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(
      result,
      Err(PipelineError::InvalidQuantity { ref node, field: "cpu", ref value })
        if node == "Build" && value == "garbage"
    ));
  }

  #[test]
  fn test_quantity_validator_accepts_common_forms() {
    for q in [
      "1", "100m", "2", "2.5", "2Gi", "2G", "500Mi", "1.5", "100k", "2e3", "2E5", "2E", "2Ei", "0",
      "+5", "0.5",
    ] {
      assert!(is_valid_quantity(q), "expected '{q}' to be valid");
    }
  }

  #[test]
  fn test_quantity_validator_rejects_garbage() {
    for q in ["", "abc", "Mi", "1.2.3", "1Q", "2ix", "e5", "."] {
      assert!(!is_valid_quantity(q), "expected '{q}' to be rejected");
    }
  }

  #[test]
  fn test_volumes_parse_into_node_info() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    volumes:
      var-lib-containers: /var/lib/containers
      buildah-cache: /var/cache/buildah
    steps:
      - buildah bud .
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    assert_eq!(infos[0].volumes.len(), 2);
    assert_eq!(
      infos[0]
        .volumes
        .get("var-lib-containers")
        .map(String::as_str),
      Some("/var/lib/containers")
    );
    assert_eq!(
      infos[0].volumes.get("buildah-cache").map(String::as_str),
      Some("/var/cache/buildah")
    );
  }

  #[test]
  fn test_volumes_default_to_empty() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    assert!(infos[0].volumes.is_empty());
  }

  #[test]
  fn test_volume_with_invalid_dns_name_is_rejected() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    volumes:
      Invalid_Name: /data
    steps:
      - cargo build
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(
      result,
      Err(PipelineError::InvalidVolume { ref name, .. }) if name == "Invalid_Name"
    ));
  }

  #[test]
  fn test_reserved_volume_name_is_rejected() {
    for reserved in RESERVED_VOLUME_NAMES {
      let yaml = format!(
        r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    volumes:
      {reserved}: /data
    steps:
      - cargo build
"#
      );
      let result = Pipeline::from_yaml(&yaml);
      assert!(
        matches!(result, Err(PipelineError::InvalidVolume { ref name, .. }) if name == reserved),
        "expected reserved volume name '{reserved}' to be rejected"
      );
    }
  }

  #[test]
  fn test_relative_mount_path_is_rejected() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    volumes:
      data: relative/path
    steps:
      - cargo build
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(result, Err(PipelineError::InvalidVolume { .. })));
  }

  #[test]
  fn test_mount_path_overlapping_reserved_path_is_rejected() {
    for path in ["/workspace", "/workspace/sub", "/tmp/cache"] {
      let yaml = format!(
        r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    volumes:
      data: {path}
    steps:
      - cargo build
"#
      );
      let result = Pipeline::from_yaml(&yaml);
      assert!(
        matches!(result, Err(PipelineError::InvalidVolume { .. })),
        "expected mount path '{path}' to be rejected"
      );
    }
  }

  #[test]
  fn merge_env_secrets_dedup_and_sort() {
    let yaml = r#"
name: Test Pipeline
secrets:
  env:
    - QUAY_PASSWORD
    - GLOBAL_TOKEN
nodes:
  - name: Build
    image: rust:latest
    secrets:
      env:
        - QUAY_USERNAME
        - QUAY_PASSWORD
    steps:
      - cargo build
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    assert_eq!(
      infos[0].env_secrets,
      vec![
        "GLOBAL_TOKEN".to_string(),
        "QUAY_PASSWORD".to_string(),
        "QUAY_USERNAME".to_string(),
      ]
    );
  }

  #[test]
  fn merge_file_secrets_node_overrides_pipeline_path() {
    let yaml = r#"
name: Test Pipeline
secrets:
  files:
    - KUBECONFIG
nodes:
  - name: Build
    image: rust:latest
    secrets:
      files:
        - name: KUBECONFIG
          path: /custom/kubeconfig
    steps:
      - cargo build
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    assert_eq!(infos[0].file_secrets.len(), 1);
    assert_eq!(infos[0].file_secrets[0].name, "KUBECONFIG");
    assert_eq!(
      infos[0].file_secrets[0].path.as_deref(),
      Some("/custom/kubeconfig")
    );
  }

  #[test]
  fn merge_file_secrets_pipeline_default_combines_with_node_unique() {
    let yaml = r#"
name: Test Pipeline
secrets:
  files:
    - KUBECONFIG
nodes:
  - name: Build
    image: rust:latest
    secrets:
      files:
        - name: PROXY_CERT
          path: /etc/pki/ca-trust/source/anchors/proxy.crt
    steps:
      - cargo build
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    let names: Vec<&str> = infos[0]
      .file_secrets
      .iter()
      .map(|s| s.name.as_str())
      .collect();
    assert_eq!(names, vec!["KUBECONFIG", "PROXY_CERT"]);
    assert_eq!(infos[0].file_secrets[0].path, None);
    assert_eq!(
      infos[0].file_secrets[1].path.as_deref(),
      Some("/etc/pki/ca-trust/source/anchors/proxy.crt")
    );
  }

  #[test]
  fn node_info_secrets_default_empty() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    assert!(infos[0].env_secrets.is_empty());
    assert!(infos[0].file_secrets.is_empty());
  }

  #[test]
  fn invalid_env_secret_name_rejected() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    secrets:
      env:
        - 1BAD
    steps:
      - cargo build
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(
      result,
      Err(PipelineError::InvalidSecret { ref node, ref name, .. })
        if node == "Build" && name == "1BAD"
    ));
  }

  #[test]
  fn invalid_file_secret_path_rejected() {
    for bad_path in [
      "relative/path",
      "/trailing/",
      "/has/../traverse",
      "/has//slash",
    ] {
      let yaml = format!(
        r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    secrets:
      files:
        - name: TOKEN
          path: {bad_path}
    steps:
      - cargo build
"#
      );
      let result = Pipeline::from_yaml(&yaml);
      assert!(
        matches!(result, Err(PipelineError::InvalidSecret { ref name, .. }) if name == "TOKEN"),
        "expected file path '{bad_path}' to be rejected"
      );
    }
  }

  #[test]
  fn valid_secret_name_forms_accepted() {
    for name in ["_FOO", "A1", "QUAY_USERNAME", "_", "aB_9"] {
      let yaml = format!(
        r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    secrets:
      env:
        - {name}
    steps:
      - cargo build
"#
      );
      assert!(
        Pipeline::from_yaml(&yaml).is_ok(),
        "expected secret name '{name}' to be accepted"
      );
    }
  }

  #[test]
  fn name_overlap_between_env_and_files_is_allowed() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    secrets:
      env:
        - SHARED_TOKEN
      files:
        - SHARED_TOKEN
    steps:
      - cargo build
"#;
    let infos = Pipeline::from_yaml(yaml).unwrap().node_info();
    assert_eq!(infos[0].env_secrets, vec!["SHARED_TOKEN".to_string()]);
    assert_eq!(infos[0].file_secrets.len(), 1);
    assert_eq!(infos[0].file_secrets[0].name, "SHARED_TOKEN");
  }

  #[test]
  fn pipeline_level_secret_invalid_name_rejected() {
    let yaml = r#"
name: Test Pipeline
secrets:
  env:
    - has-dash
nodes:
  - name: Build
    image: rust:latest
    steps:
      - cargo build
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(
      result,
      Err(PipelineError::InvalidSecret { ref name, .. }) if name == "has-dash"
    ));
  }

  #[test]
  fn test_duplicate_mount_paths_rejected() {
    let yaml = r#"
name: Test Pipeline
nodes:
  - name: Build
    image: rust:latest
    volumes:
      a: /data
      b: /data
    steps:
      - cargo build
"#;
    let result = Pipeline::from_yaml(yaml);
    assert!(matches!(result, Err(PipelineError::InvalidVolume { .. })));
  }
}
