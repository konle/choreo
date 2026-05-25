use async_trait::async_trait;
use domain::apikey::entity::ApiKeyEntity;
use domain::apikey::repository::{ApiKeyRepository, RepositoryError};
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Client, Collection, Database};

pub struct ApiKeyRepositoryImpl {
    collection: Collection<ApiKeyEntity>,
}

impl ApiKeyRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database: Database = client.database("workflow");
        let collection = database.collection("api_keys");
        Self { collection }
    }
}

#[async_trait]
impl ApiKeyRepository for ApiKeyRepositoryImpl {
    async fn create(&self, entity: &ApiKeyEntity) -> Result<ApiKeyEntity, RepositoryError> {
        self.collection.insert_one(entity).await?;
        Ok(entity.clone())
    }

    async fn get_by_id(&self, tenant_id: &str, id: &str) -> Result<ApiKeyEntity, RepositoryError> {
        self.collection
            .find_one(doc! { "tenant_id": tenant_id, "id": id })
            .await?
            .ok_or_else(|| format!("api key not found: {} in tenant {}", id, tenant_id).into())
    }

    async fn get_by_prefix(&self, key_prefix: &str) -> Result<ApiKeyEntity, RepositoryError> {
        self.collection
            .find_one(doc! { "key_prefix": key_prefix })
            .await?
            .ok_or_else(|| format!("api key not found for prefix {}", key_prefix).into())
    }

    async fn update(&self, entity: &ApiKeyEntity) -> Result<ApiKeyEntity, RepositoryError> {
        let filter = doc! { "tenant_id": &entity.tenant_id, "id": &entity.id };
        self.collection.replace_one(filter, entity).await?;
        Ok(entity.clone())
    }

    async fn list_by_tenant(&self, tenant_id: &str) -> Result<Vec<ApiKeyEntity>, RepositoryError> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .await?;
        Ok(cursor.try_collect().await?)
    }
}
