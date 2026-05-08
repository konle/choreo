use async_trait::async_trait;
use crate::task::repository::RepositoryError;
use super::entity::{NotificationSubscription, NotificationRecord};

#[async_trait]
pub trait NotificationSubscriptionRepository: Send + Sync {
    async fn create(&self, sub: &NotificationSubscription) -> Result<(), RepositoryError>;
    async fn update(&self, sub: &NotificationSubscription) -> Result<(), RepositoryError>;
    async fn delete(&self, tenant_id: &str, subscription_id: &str) -> Result<(), RepositoryError>;
    async fn get_by_id(&self, tenant_id: &str, subscription_id: &str) -> Result<NotificationSubscription, RepositoryError>;
    async fn list_by_user(&self, tenant_id: &str, user_id: &str) -> Result<Vec<NotificationSubscription>, RepositoryError>;
    /// Find all enabled subscriptions in a tenant matching a given event type.
    async fn find_matching(
        &self,
        tenant_id: &str,
        event_type: &str,
    ) -> Result<Vec<NotificationSubscription>, RepositoryError>;
}

#[async_trait]
pub trait NotificationRecordRepository: Send + Sync {
    async fn create(&self, record: &NotificationRecord) -> Result<(), RepositoryError>;
    async fn list_by_user(
        &self,
        tenant_id: &str,
        user_id: &str,
        page: u64,
        page_size: u64,
    ) -> Result<(Vec<NotificationRecord>, u64), RepositoryError>;
    async fn unread_count(&self, tenant_id: &str, user_id: &str) -> Result<u64, RepositoryError>;
    async fn mark_read(&self, tenant_id: &str, user_id: &str, notification_id: &str) -> Result<(), RepositoryError>;
    async fn mark_all_read(&self, tenant_id: &str, user_id: &str) -> Result<u64, RepositoryError>;
}
