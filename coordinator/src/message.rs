#[derive(Debug)]
pub enum CoordinatorMessage {
  NodeTimedOut { node_name: String },
}
