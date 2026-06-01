use std::sync::Arc;
use chrono::Utc;
use uuid::Uuid;
use super::entity::{
    ChannelDeliveryStatus, DeliveryStatus, NotificationChannel, NotificationRecord,
    NotificationSubscription, SubscriptionScope,
};
use super::error::RepositoryError;
use super::repository::{NotificationRecordRepository, NotificationSubscriptionRepository};

#[derive(Clone)]
pub struct NotificationService {
    pub sub_repo: Arc<dyn NotificationSubscriptionRepository>,
    pub record_repo: Arc<dyn NotificationRecordRepository>,
    pub frontend_base_url: String,
}

impl NotificationService {
    pub fn new(
        sub_repo: Arc<dyn NotificationSubscriptionRepository>,
        record_repo: Arc<dyn NotificationRecordRepository>,
        frontend_base_url: String,
    ) -> Self {
        Self { sub_repo, record_repo, frontend_base_url }
    }

    // ── Subscription CRUD ──

    pub async fn create_subscription(
        &self,
        tenant_id: &str,
        user_id: &str,
        scope: SubscriptionScope,
        resource_type: Option<String>,
        resource_id: Option<String>,
        event_types: Vec<String>,
        channels: Vec<NotificationChannel>,
    ) -> Result<NotificationSubscription, RepositoryError> {
        let now = Utc::now();
        let sub = NotificationSubscription {
            subscription_id: Uuid::new_v4().to_string(),
            tenant_id: tenant_id.to_string(),
            user_id: user_id.to_string(),
            scope,
            resource_type,
            resource_id,
            event_types,
            channels,
            enabled: true,
            created_at: now,
            updated_at: now,
        };
        self.sub_repo.create(&sub).await?;
        Ok(sub)
    }

    pub async fn update_subscription(
        &self,
        mut sub: NotificationSubscription,
    ) -> Result<NotificationSubscription, RepositoryError> {
        sub.updated_at = Utc::now();
        self.sub_repo.update(&sub).await?;
        Ok(sub)
    }

    pub async fn delete_subscription(
        &self,
        tenant_id: &str,
        subscription_id: &str,
    ) -> Result<(), RepositoryError> {
        self.sub_repo.delete(tenant_id, subscription_id).await
    }

    pub async fn get_subscription(
        &self,
        tenant_id: &str,
        subscription_id: &str,
    ) -> Result<NotificationSubscription, RepositoryError> {
        self.sub_repo.get_by_id(tenant_id, subscription_id).await
    }

    pub async fn list_subscriptions(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<NotificationSubscription>, RepositoryError> {
        self.sub_repo.list_by_user(tenant_id, user_id).await
    }

    // ── Subscription matching (§16.3.2) ──

    pub async fn find_recipients_for_event(
        &self,
        tenant_id: &str,
        event_type: &str,
        workflow_meta_id: Option<&str>,
    ) -> Result<Vec<(String, Vec<NotificationChannel>)>, RepositoryError> {
        let subs = self.sub_repo.find_matching(tenant_id, event_type).await?;

        let mut user_channels: std::collections::HashMap<String, Vec<NotificationChannel>> =
            std::collections::HashMap::new();

        let mut resource_hit: std::collections::HashSet<String> = std::collections::HashSet::new();

        if let Some(meta_id) = workflow_meta_id {
            for sub in &subs {
                if sub.scope == SubscriptionScope::Resource
                    && sub.resource_id.as_deref() == Some(meta_id)
                {
                    resource_hit.insert(sub.user_id.clone());
                    user_channels.insert(sub.user_id.clone(), sub.channels.clone());
                }
            }
        }

        for sub in &subs {
            if sub.scope == SubscriptionScope::Global && !resource_hit.contains(&sub.user_id) {
                user_channels.insert(sub.user_id.clone(), sub.channels.clone());
            }
        }

        Ok(user_channels.into_iter().collect())
    }

    // ── Notification records ──

    pub async fn create_in_app_record(
        &self,
        tenant_id: &str,
        user_id: &str,
        event_type: &str,
        payload: &serde_json::Value,
        source_type: &str,
        source_id: &str,
        workflow_meta_id: Option<&str>,
    ) -> Result<NotificationRecord, RepositoryError> {
        let url = self.build_url(payload);
        let record = NotificationRecord {
            notification_id: Uuid::new_v4().to_string(),
            tenant_id: tenant_id.to_string(),
            user_id: user_id.to_string(),
            event_type: event_type.to_string(),
            event_payload: payload.clone(),
            source_type: source_type.to_string(),
            source_id: source_id.to_string(),
            workflow_meta_id: workflow_meta_id.map(|s| s.to_string()),
            url,
            channel_statuses: vec![ChannelDeliveryStatus {
                channel: "in_app".to_string(),
                status: DeliveryStatus::Sent,
                sent_at: Some(Utc::now()),
                error: None,
            }],
            read: false,
            created_at: Utc::now(),
        };
        self.record_repo.create(&record).await?;
        Ok(record)
    }

    pub async fn list_notifications(
        &self,
        tenant_id: &str,
        user_id: &str,
        page: u64,
        page_size: u64,
    ) -> Result<(Vec<NotificationRecord>, u64), RepositoryError> {
        self.record_repo.list_by_user(tenant_id, user_id, page, page_size).await
    }

    pub async fn unread_count(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<u64, RepositoryError> {
        self.record_repo.unread_count(tenant_id, user_id).await
    }

    pub async fn mark_read(
        &self,
        tenant_id: &str,
        user_id: &str,
        notification_id: &str,
    ) -> Result<(), RepositoryError> {
        self.record_repo.mark_read(tenant_id, user_id, notification_id).await
    }

    pub async fn mark_all_read(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<u64, RepositoryError> {
        self.record_repo.mark_all_read(tenant_id, user_id).await
    }

    fn build_url(&self, payload: &serde_json::Value) -> Option<String> {
        let wf_id = payload
            .get("data")
            .and_then(|d| d.get("workflow_instance_id"))
            .and_then(|v| v.as_str());
        wf_id.map(|id| format!("{}/workflows/instances/{}", self.frontend_base_url, id))
    }
}
