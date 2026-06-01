use async_trait::async_trait;
use domain::notification::entity::{NotificationRecord, NotificationSubscription};
use domain::notification::error::RepositoryError;
use domain::notification::repository::{
    NotificationRecordRepository, NotificationSubscriptionRepository,
};
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Client, Collection, Database, IndexModel};
use mongodb::options::IndexOptions;

pub struct NotificationSubscriptionRepositoryImpl {
    collection: Collection<NotificationSubscription>,
}

impl NotificationSubscriptionRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database: Database = client.database("workflow");
        let collection = database.collection("notification_subscriptions");
        Self { collection }
    }

    pub async fn ensure_indexes(&self) -> Result<(), mongodb::error::Error> {
        let idx = IndexModel::builder()
            .keys(doc! {
                "tenant_id": 1,
                "user_id": 1,
                "scope": 1,
                "resource_type": 1,
                "resource_id": 1,
            })
            .options(
                IndexOptions::builder()
                    .name("uk_tenant_user_scope_resource".to_string())
                    .unique(true)
                    .build(),
            )
            .build();
        self.collection.create_index(idx).await?;

        let idx2 = IndexModel::builder()
            .keys(doc! { "tenant_id": 1, "event_types": 1, "enabled": 1 })
            .options(
                IndexOptions::builder()
                    .name("idx_tenant_event_enabled".to_string())
                    .build(),
            )
            .build();
        self.collection.create_index(idx2).await?;
        Ok(())
    }
}

#[async_trait]
impl NotificationSubscriptionRepository for NotificationSubscriptionRepositoryImpl {
    async fn create(&self, sub: &NotificationSubscription) -> Result<(), RepositoryError> {
        self.collection.insert_one(sub).await?;
        Ok(())
    }

    async fn update(&self, sub: &NotificationSubscription) -> Result<(), RepositoryError> {
        let filter = doc! {
            "tenant_id": &sub.tenant_id,
            "subscription_id": &sub.subscription_id,
        };
        self.collection.replace_one(filter, sub).await?;
        Ok(())
    }

    async fn delete(
        &self,
        tenant_id: &str,
        subscription_id: &str,
    ) -> Result<(), RepositoryError> {
        self.collection
            .delete_one(doc! {
                "tenant_id": tenant_id,
                "subscription_id": subscription_id,
            })
            .await?;
        Ok(())
    }

    async fn get_by_id(
        &self,
        tenant_id: &str,
        subscription_id: &str,
    ) -> Result<NotificationSubscription, RepositoryError> {
        self.collection
            .find_one(doc! {
                "tenant_id": tenant_id,
                "subscription_id": subscription_id,
            })
            .await?
            .ok_or_else(|| {
                RepositoryError::NotFound(format!(
                    "subscription not found: {}",
                    subscription_id
                ))
            })
    }

    async fn list_by_user(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<NotificationSubscription>, RepositoryError> {
        let cursor = self
            .collection
            .find(doc! {
                "tenant_id": tenant_id,
                "user_id": user_id,
            })
            .await?;
        Ok(cursor.try_collect().await?)
    }

    async fn find_matching(
        &self,
        tenant_id: &str,
        event_type: &str,
    ) -> Result<Vec<NotificationSubscription>, RepositoryError> {
        let cursor = self
            .collection
            .find(doc! {
                "tenant_id": tenant_id,
                "event_types": event_type,
                "enabled": true,
            })
            .await?;
        Ok(cursor.try_collect().await?)
    }
}

pub struct NotificationRecordRepositoryImpl {
    collection: Collection<NotificationRecord>,
}

impl NotificationRecordRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database: Database = client.database("workflow");
        let collection = database.collection("notification_records");
        Self { collection }
    }

    pub async fn ensure_indexes(&self) -> Result<(), mongodb::error::Error> {
        let idx1 = IndexModel::builder()
            .keys(doc! { "tenant_id": 1, "user_id": 1, "created_at": -1 })
            .options(
                IndexOptions::builder()
                    .name("idx_tenant_user_created".to_string())
                    .build(),
            )
            .build();
        self.collection.create_index(idx1).await?;

        let idx2 = IndexModel::builder()
            .keys(doc! { "tenant_id": 1, "user_id": 1, "read": 1 })
            .options(
                IndexOptions::builder()
                    .name("idx_tenant_user_read".to_string())
                    .build(),
            )
            .build();
        self.collection.create_index(idx2).await?;

        let idx3 = IndexModel::builder()
            .keys(doc! { "created_at": 1 })
            .options(
                IndexOptions::builder()
                    .name("idx_ttl_created_at".to_string())
                    .expire_after(std::time::Duration::from_secs(2592000))
                    .build(),
            )
            .build();
        self.collection.create_index(idx3).await?;
        Ok(())
    }
}

#[async_trait]
impl NotificationRecordRepository for NotificationRecordRepositoryImpl {
    async fn create(&self, record: &NotificationRecord) -> Result<(), RepositoryError> {
        self.collection.insert_one(record).await?;
        Ok(())
    }

    async fn list_by_user(
        &self,
        tenant_id: &str,
        user_id: &str,
        page: u64,
        page_size: u64,
    ) -> Result<(Vec<NotificationRecord>, u64), RepositoryError> {
        let filter = doc! {
            "tenant_id": tenant_id,
            "user_id": user_id,
        };
        let total = self.collection.count_documents(filter.clone()).await?;
        let skip = (page.saturating_sub(1)) * page_size;
        let cursor = self
            .collection
            .find(filter)
            .sort(doc! { "created_at": -1 })
            .skip(skip)
            .limit(page_size as i64)
            .await?;
        let records: Vec<NotificationRecord> = cursor.try_collect().await?;
        Ok((records, total))
    }

    async fn unread_count(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<u64, RepositoryError> {
        let count = self
            .collection
            .count_documents(doc! {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "read": false,
            })
            .await?;
        Ok(count)
    }

    async fn mark_read(
        &self,
        tenant_id: &str,
        user_id: &str,
        notification_id: &str,
    ) -> Result<(), RepositoryError> {
        self.collection
            .update_one(
                doc! {
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "notification_id": notification_id,
                },
                doc! { "$set": { "read": true } },
            )
            .await?;
        Ok(())
    }

    async fn mark_all_read(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<u64, RepositoryError> {
        let result = self
            .collection
            .update_many(
                doc! {
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "read": false,
                },
                doc! { "$set": { "read": true } },
            )
            .await?;
        Ok(result.modified_count)
    }
}
