use async_trait::async_trait;
use domain::tenant::entity::TenantEntity;
use domain::tenant::repository::{RepositoryError, TenantRepository};
use mongodb::bson::doc;
use mongodb::{Client, Collection, Database};

pub struct TenantRepositoryImpl {
    collection: Collection<TenantEntity>,
}

impl TenantRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database: Database = client.database("workflow");
        let collection = database.collection("tenants");
        Self { collection }
    }
}

#[async_trait]
impl TenantRepository for TenantRepositoryImpl {
    async fn create(&self, entity: &TenantEntity) -> Result<TenantEntity, RepositoryError> {
        self.collection.insert_one(entity).await?;
        Ok(entity.clone())
    }

    async fn get_by_id(&self, tenant_id: &str) -> Result<TenantEntity, RepositoryError> {
        self.collection
            .find_one(doc! { "tenant_id": tenant_id })
            .await?
            .ok_or_else(|| format!("tenant not found: {}", tenant_id).into())
    }

    async fn list(&self) -> Result<Vec<TenantEntity>, RepositoryError> {
        use futures::TryStreamExt;
        let cursor = self.collection.find(doc! {}).await?;
        let results: Vec<TenantEntity> = cursor.try_collect().await?;
        Ok(results)
    }

    async fn update(&self, entity: &TenantEntity) -> Result<(), RepositoryError> {
        let filter = doc! { "tenant_id": &entity.tenant_id };
        self.collection.replace_one(filter, entity).await?;
        Ok(())
    }

    async fn delete(&self, tenant_id: &str) -> Result<(), RepositoryError> {
        self.collection
            .delete_one(doc! { "tenant_id": tenant_id })
            .await?;
        Ok(())
    }
}
