use crate::pod_watcher::InfraFailureReason;

#[derive(Debug)]
pub enum CoordinatorMessage {
  NodeTimedOut {
    node_name: String,
  },
  NodePodRunning {
    node_name: String,
  },
  NodeInfraFailed {
    node_name: String,
    reason: InfraFailureReason,
  },
  NodePodStartTimedOut {
    node_name: String,
  },
}
