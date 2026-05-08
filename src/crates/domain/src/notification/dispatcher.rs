use async_trait::async_trait;
use super::entity::NotificationEvent;

#[async_trait]
pub trait NotificationDispatcher: Send + Sync {
    async fn dispatch_notification(&self, event: NotificationEvent) -> anyhow::Result<()>;
}
