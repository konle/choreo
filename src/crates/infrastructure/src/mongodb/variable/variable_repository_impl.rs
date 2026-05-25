use async_trait::async_trait;
use domain::variable::entity::{VariableEntity, VariableScope};
use domain::variable::repository::{RepositoryError, VariableRepository};
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Client, Collection, Database};

pub struct VariableRepositoryImpl {
    pub client: Client,
    pub database: Database,
    pub collection: Collection<VariableEntity>,
}

impl VariableRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database = client.database("workflow");
        let collection = database.collection("variables");
        Self {
            client,
            database,
            collection,
        }
    }
}

fn scope_str(scope: &VariableScope) -> &'static str {
    match scope {
        VariableScope::Tenant => "Tenant",
        VariableScope::WorkflowMeta => "WorkflowMeta",
    }
}

#[async_trait]
impl VariableRepository for VariableRepositoryImpl {
    async fn create(&self, entity: &VariableEntity) -> Result<VariableEntity, RepositoryError> {
        self.collection.insert_one(entity).await?;
        Ok(entity.clone())
    }

    async fn get_by_id(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<VariableEntity, RepositoryError> {
        let entity = self
            .collection
            .find_one(doc! { "tenant_id": tenant_id, "id": id })
            .await?
            .ok_or_else(|| format!("variable not found: {} in tenant {}", id, tenant_id))?;
        Ok(entity)
    }

    async fn update(&self, entity: &VariableEntity) -> Result<VariableEntity, RepositoryError> {
        let filter = doc! { "tenant_id": &entity.tenant_id, "id": &entity.id };
        self.collection.replace_one(filter, entity).await?;
        Ok(entity.clone())
    }

    async fn delete(&self, tenant_id: &str, id: &str) -> Result<(), RepositoryError> {
        self.collection
            .delete_one(doc! { "tenant_id": tenant_id, "id": id })
            .await?;
        Ok(())
    }

    async fn list_by_scope(
        &self,
        tenant_id: &str,
        scope: &VariableScope,
        scope_id: &str,
    ) -> Result<Vec<VariableEntity>, RepositoryError> {
        let cursor = self
            .collection
            .find(doc! {
                "tenant_id": tenant_id,
                "scope": scope_str(scope),
                "scope_id": scope_id,
            })
            .await?;
        let results: Vec<VariableEntity> = cursor.try_collect().await?;
        Ok(results)
    }

    async fn get_by_key(
        &self,
        tenant_id: &str,
        scope: &VariableScope,
        scope_id: &str,
        key: &str,
    ) -> Result<Option<VariableEntity>, RepositoryError> {
        let entity = self
            .collection
            .find_one(doc! {
                "tenant_id": tenant_id,
                "scope": scope_str(scope),
                "scope_id": scope_id,
                "key": key,
            })
            .await?;
        Ok(entity)
    }
}
