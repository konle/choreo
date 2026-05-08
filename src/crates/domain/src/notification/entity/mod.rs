use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── Subscription ──

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionScope {
    Global,
    Resource,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NotificationChannel {
    InApp,
    Webhook {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        secret: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationSubscription {
    pub subscription_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub scope: SubscriptionScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    pub event_types: Vec<String>,
    pub channels: Vec<NotificationChannel>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ── Notification Record ──

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryStatus {
    Pending,
    Sent,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelDeliveryStatus {
    pub channel: String,
    pub status: DeliveryStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationRecord {
    pub notification_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub event_type: String,
    pub event_payload: serde_json::Value,
    pub source_type: String,
    pub source_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_meta_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub channel_statuses: Vec<ChannelDeliveryStatus>,
    pub read: bool,
    pub created_at: DateTime<Utc>,
}

// ── Notification Event (queued to apalis-redis) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationEvent {
    pub tenant_id: String,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_meta_id: Option<String>,
    /// When set, bypass subscription matching and push directly to these users (InApp).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_user_ids: Option<Vec<String>>,
    pub payload: serde_json::Value,
}
