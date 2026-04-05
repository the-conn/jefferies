use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackplaneEvent {
  NodeCompleted { node_name: String, success: bool },
  Cancel,
}

#[derive(Debug, Error)]
pub enum BackplaneError {
  #[error("AMQP error: {0}")]
  Amqp(#[from] deadpool_lapin::lapin::Error),
  #[error("Pool error: {0}")]
  Pool(#[from] deadpool_lapin::PoolError),
  #[error("Serialization error: {0}")]
  Serialization(#[from] serde_json::Error),
  #[error("Backplane error: {0}")]
  Other(String),
}

#[async_trait]
pub trait RunSubscription: Send {
  async fn next_event(&mut self) -> Option<BackplaneEvent>;
}

#[async_trait]
pub trait Backplane: Send + Sync {
  async fn publish_node_completed(
    &self,
    run_id: &str,
    node_name: &str,
    success: bool,
  ) -> Result<(), BackplaneError>;

  async fn publish_cancel(&self, run_id: &str) -> Result<(), BackplaneError>;

  async fn subscribe_run(
    &self,
    run_id: &str,
  ) -> Result<Box<dyn RunSubscription + Send>, BackplaneError>;
}

const JEFFERIES_EXCHANGE: &str = "jefferies.events";

pub struct RabbitmqBackplane {
  pool: deadpool_lapin::Pool,
}

impl RabbitmqBackplane {
  pub fn new(url: &str, user: &str, password: &str) -> Result<Self, BackplaneError> {
    let amqp_url = format!(
      "amqp://{}:{}@{}",
      user,
      password,
      url.trim_start_matches("amqp://")
    );
    let cfg = deadpool_lapin::Config {
      url: Some(amqp_url),
      ..Default::default()
    };
    let pool = cfg
      .create_pool(Some(deadpool_lapin::Runtime::Tokio1))
      .map_err(|e| BackplaneError::Other(e.to_string()))?;
    Ok(Self { pool })
  }

  pub async fn ping(&self) -> Result<(), BackplaneError> {
    let _conn = self.pool.get().await?;
    Ok(())
  }

  async fn publish_event(
    &self,
    run_id: &str,
    event: &BackplaneEvent,
  ) -> Result<(), BackplaneError> {
    use deadpool_lapin::lapin::{BasicProperties, options::*};

    let conn = self.pool.get().await?;
    let channel = conn.create_channel().await?;

    channel
      .exchange_declare(
        JEFFERIES_EXCHANGE,
        deadpool_lapin::lapin::ExchangeKind::Topic,
        ExchangeDeclareOptions {
          durable: true,
          ..Default::default()
        },
        Default::default(),
      )
      .await?;

    let payload = serde_json::to_vec(event)?;
    let routing_key = format!("run.{run_id}");

    channel
      .basic_publish(
        JEFFERIES_EXCHANGE,
        &routing_key,
        BasicPublishOptions::default(),
        &payload,
        BasicProperties::default(),
      )
      .await?
      .await?;

    Ok(())
  }
}

#[async_trait]
impl Backplane for RabbitmqBackplane {
  async fn publish_node_completed(
    &self,
    run_id: &str,
    node_name: &str,
    success: bool,
  ) -> Result<(), BackplaneError> {
    let event = BackplaneEvent::NodeCompleted {
      node_name: node_name.to_string(),
      success,
    };
    self.publish_event(run_id, &event).await
  }

  async fn publish_cancel(&self, run_id: &str) -> Result<(), BackplaneError> {
    self.publish_event(run_id, &BackplaneEvent::Cancel).await
  }

  async fn subscribe_run(
    &self,
    run_id: &str,
  ) -> Result<Box<dyn RunSubscription + Send>, BackplaneError> {
    use deadpool_lapin::lapin::options::*;

    let conn = self.pool.get().await?;
    let channel = conn.create_channel().await?;

    channel
      .exchange_declare(
        JEFFERIES_EXCHANGE,
        deadpool_lapin::lapin::ExchangeKind::Topic,
        ExchangeDeclareOptions {
          durable: true,
          ..Default::default()
        },
        Default::default(),
      )
      .await?;

    let queue_name = format!("jefferies.coordinator.{run_id}");
    channel
      .queue_declare(
        &queue_name,
        QueueDeclareOptions {
          auto_delete: true,
          exclusive: true,
          ..Default::default()
        },
        Default::default(),
      )
      .await?;

    let routing_key = format!("run.{run_id}");
    channel
      .queue_bind(
        &queue_name,
        JEFFERIES_EXCHANGE,
        &routing_key,
        QueueBindOptions::default(),
        Default::default(),
      )
      .await?;

    let consumer = channel
      .basic_consume(
        &queue_name,
        "",
        BasicConsumeOptions::default(),
        Default::default(),
      )
      .await?;

    Ok(Box::new(RabbitmqSubscription { consumer }))
  }
}

struct RabbitmqSubscription {
  consumer: deadpool_lapin::lapin::Consumer,
}

#[async_trait]
impl RunSubscription for RabbitmqSubscription {
  async fn next_event(&mut self) -> Option<BackplaneEvent> {
    use futures_util::StreamExt;

    loop {
      match self.consumer.next().await {
        Some(Ok(delivery)) => {
          if let Ok(event) = serde_json::from_slice::<BackplaneEvent>(&delivery.data) {
            let _ = delivery
              .ack(deadpool_lapin::lapin::options::BasicAckOptions::default())
              .await;
            return Some(event);
          } else {
            warn!("Failed to deserialize backplane event");
            let _ = delivery
              .nack(deadpool_lapin::lapin::options::BasicNackOptions::default())
              .await;
          }
        }
        Some(Err(e)) => {
          warn!(error = %e, "Error receiving from backplane subscription");
          return None;
        }
        None => return None,
      }
    }
  }
}

pub struct InMemoryBackplane {
  channels: Mutex<HashMap<String, broadcast::Sender<BackplaneEvent>>>,
}

impl InMemoryBackplane {
  pub fn new() -> Arc<Self> {
    Arc::new(Self::default())
  }

  async fn publish_event(&self, run_id: &str, event: BackplaneEvent) -> Result<(), BackplaneError> {
    let mut channels = self.channels.lock().await;
    let sender = channels
      .entry(run_id.to_string())
      .or_insert_with(|| broadcast::channel(128).0);
    match sender.send(event) {
      Ok(_) => {}
      Err(_) => {
        debug!(run_id, "No subscribers for backplane event");
      }
    }
    Ok(())
  }
}

impl Default for InMemoryBackplane {
  fn default() -> Self {
    Self {
      channels: Mutex::new(HashMap::new()),
    }
  }
}

#[async_trait]
impl Backplane for InMemoryBackplane {
  async fn publish_node_completed(
    &self,
    run_id: &str,
    node_name: &str,
    success: bool,
  ) -> Result<(), BackplaneError> {
    let event = BackplaneEvent::NodeCompleted {
      node_name: node_name.to_string(),
      success,
    };
    self.publish_event(run_id, event).await
  }

  async fn publish_cancel(&self, run_id: &str) -> Result<(), BackplaneError> {
    self.publish_event(run_id, BackplaneEvent::Cancel).await
  }

  async fn subscribe_run(
    &self,
    run_id: &str,
  ) -> Result<Box<dyn RunSubscription + Send>, BackplaneError> {
    let mut channels = self.channels.lock().await;
    let sender = channels
      .entry(run_id.to_string())
      .or_insert_with(|| broadcast::channel(128).0);
    let receiver = sender.subscribe();
    Ok(Box::new(InMemorySubscription { receiver }))
  }
}

struct InMemorySubscription {
  receiver: broadcast::Receiver<BackplaneEvent>,
}

#[async_trait]
impl RunSubscription for InMemorySubscription {
  async fn next_event(&mut self) -> Option<BackplaneEvent> {
    loop {
      match self.receiver.recv().await {
        Ok(event) => return Some(event),
        Err(broadcast::error::RecvError::Lagged(n)) => {
          warn!(skipped = n, "InMemorySubscription lagged");
        }
        Err(broadcast::error::RecvError::Closed) => return None,
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_publish_and_subscribe() {
    let backplane = InMemoryBackplane::new();
    let mut sub = backplane.subscribe_run("run1").await.unwrap();

    backplane
      .publish_node_completed("run1", "build", true)
      .await
      .unwrap();

    let event = sub.next_event().await.unwrap();
    match event {
      BackplaneEvent::NodeCompleted { node_name, success } => {
        assert_eq!(node_name, "build");
        assert!(success);
      }
      _ => panic!("Unexpected event type"),
    }
  }

  #[tokio::test]
  async fn test_cancel_event() {
    let backplane = InMemoryBackplane::new();
    let mut sub = backplane.subscribe_run("run1").await.unwrap();

    backplane.publish_cancel("run1").await.unwrap();

    let event = sub.next_event().await.unwrap();
    assert!(matches!(event, BackplaneEvent::Cancel));
  }
}
