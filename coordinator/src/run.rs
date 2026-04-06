use std::collections::{HashMap, HashSet};

use pipelines::NodeInfo;
pub use state_store::NodeStatus;

pub struct PipelineRun {
  statuses: HashMap<String, NodeStatus>,
  dependencies: HashMap<String, HashSet<String>>,
}

impl PipelineRun {
  pub fn new(nodes: &[NodeInfo]) -> Self {
    let statuses = nodes
      .iter()
      .map(|n| (n.name.clone(), NodeStatus::Pending))
      .collect();
    let dependencies = nodes
      .iter()
      .map(|n| (n.name.clone(), n.dependencies.iter().cloned().collect()))
      .collect();
    Self {
      statuses,
      dependencies,
    }
  }

  pub fn ready_nodes(&self) -> Vec<String> {
    self
      .statuses
      .iter()
      .filter(|(name, status)| **status == NodeStatus::Pending && self.deps_satisfied(name))
      .map(|(name, _)| name.clone())
      .collect()
  }

  fn deps_satisfied(&self, node: &str) -> bool {
    self
      .dependencies
      .get(node)
      .map(|deps| {
        deps
          .iter()
          .all(|dep| self.statuses.get(dep.as_str()) == Some(&NodeStatus::Success))
      })
      .unwrap_or(true)
  }

  pub fn mark_running(&mut self, node: &str) -> bool {
    if self.statuses.get(node) == Some(&NodeStatus::Pending) {
      self.statuses.insert(node.to_string(), NodeStatus::Running);
      true
    } else {
      false
    }
  }

  pub fn mark_success(&mut self, node: &str) -> bool {
    if self.statuses.get(node) == Some(&NodeStatus::Running) {
      self.statuses.insert(node.to_string(), NodeStatus::Success);
      true
    } else {
      false
    }
  }

  pub fn mark_failed(&mut self, node: &str) -> bool {
    if self.statuses.get(node) == Some(&NodeStatus::Running) {
      self.statuses.insert(node.to_string(), NodeStatus::Failed);
      true
    } else {
      false
    }
  }

  pub fn mark_dispatch_failed(&mut self, node: &str) -> bool {
    if self.statuses.get(node) == Some(&NodeStatus::Pending) {
      self.statuses.insert(node.to_string(), NodeStatus::Failed);
      true
    } else {
      false
    }
  }

  pub fn is_complete(&self) -> bool {
    let any_running = self.statuses.values().any(|s| *s == NodeStatus::Running);
    if any_running {
      return false;
    }
    self.ready_nodes().is_empty()
  }

  pub fn statuses(&self) -> &HashMap<String, NodeStatus> {
    &self.statuses
  }

  pub fn from_run_state(state: &state_store::RunState) -> Self {
    let statuses = state.statuses.clone();
    let dependencies = state
      .dependencies
      .iter()
      .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
      .collect();
    Self {
      statuses,
      dependencies,
    }
  }

  pub fn to_run_state(
    &self,
    version: u64,
    pipeline: std::sync::Arc<pipelines::Pipeline>,
  ) -> state_store::RunState {
    state_store::RunState {
      version,
      statuses: self.statuses.clone(),
      dependencies: self
        .dependencies
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
        .collect(),
      pipeline: (*pipeline).clone(),
    }
  }
}

#[cfg(test)]
mod tests {
  use pipelines::NodeInfo;

  use super::*;

  fn make_node(name: &str, deps: &[&str]) -> NodeInfo {
    NodeInfo {
      name: name.to_string(),
      image: String::new(),
      steps: vec![],
      dependencies: deps.iter().map(|d| d.to_string()).collect(),
      timeout_secs: None,
      checkout: false,
    }
  }

  fn make_run(nodes: &[(&str, &[&str])]) -> PipelineRun {
    let infos: Vec<NodeInfo> = nodes.iter().map(|(n, d)| make_node(n, d)).collect();
    PipelineRun::new(&infos)
  }

  #[test]
  fn test_initial_ready_nodes_no_deps() {
    let run = make_run(&[("build", &[]), ("test", &[])]);
    let mut ready = run.ready_nodes();
    ready.sort();
    assert_eq!(ready, vec!["build", "test"]);
  }

  #[test]
  fn test_dependent_node_not_ready_initially() {
    let run = make_run(&[("build", &[]), ("test", &["build"])]);
    assert_eq!(run.ready_nodes(), vec!["build"]);
  }

  #[test]
  fn test_node_unlocked_after_dep_succeeds() {
    let mut run = make_run(&[("build", &[]), ("test", &["build"])]);
    run.mark_running("build");
    run.mark_success("build");
    assert_eq!(run.ready_nodes(), vec!["test"]);
  }

  #[test]
  fn test_is_complete_when_all_succeed() {
    let mut run = make_run(&[("build", &[]), ("test", &["build"])]);
    run.mark_running("build");
    run.mark_success("build");
    run.mark_running("test");
    run.mark_success("test");
    assert!(run.is_complete());
  }

  #[test]
  fn test_is_complete_when_node_fails_and_downstream_blocked() {
    let mut run = make_run(&[("build", &[]), ("test", &["build"])]);
    run.mark_running("build");
    run.mark_failed("build");
    assert!(run.is_complete());
  }

  #[test]
  fn test_not_complete_while_running() {
    let mut run = make_run(&[("build", &[])]);
    run.mark_running("build");
    assert!(!run.is_complete());
  }

  #[test]
  fn test_mark_running_requires_pending() {
    let mut run = make_run(&[("build", &[])]);
    assert!(run.mark_running("build"));
    assert!(!run.mark_running("build"));
  }
}
