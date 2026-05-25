use async_trait::async_trait;
use domain::approval::entity::ApprovalInstanceEntity;
use domain::approval::repository::{ApprovalRepository, RepositoryError};
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::{Client, Collection, Database};

pub struct ApprovalRepositoryImpl {
    collection: Collection<ApprovalInstanceEntity>,
}

impl ApprovalRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database: Database = client.database("workflow");
        let collection = database.collection("approval_instances");
        Self { collection }
    }
}

#[async_trait]
impl ApprovalRepository for ApprovalRepositoryImpl {
    async fn create(
        &self,
        entity: &ApprovalInstanceEntity,
    ) -> Result<ApprovalInstanceEntity, RepositoryError> {
        self.collection.insert_one(entity).await?;
        Ok(entity.clone())
    }

    async fn get_by_id(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<ApprovalInstanceEntity, RepositoryError> {
        self.collection
            .find_one(doc! { "tenant_id": tenant_id, "id": id })
            .await?
            .ok_or_else(|| format!("approval not found: {} in tenant {}", id, tenant_id).into())
    }

    async fn update(
        &self,
        entity: &ApprovalInstanceEntity,
    ) -> Result<ApprovalInstanceEntity, RepositoryError> {
        let filter = doc! { "tenant_id": &entity.tenant_id, "id": &entity.id };
        self.collection.replace_one(filter, entity).await?;
        Ok(entity.clone())
    }

    async fn find_by_workflow_and_node(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        node_id: &str,
    ) -> Result<Option<ApprovalInstanceEntity>, RepositoryError> {
        let entity = self
            .collection
            .find_one(doc! {
                "tenant_id": tenant_id,
                "workflow_instance_id": workflow_instance_id,
                "node_id": node_id,
            })
            .await?;
        Ok(entity)
    }

    async fn list_pending_by_approver(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
        let cursor = self
            .collection
            .find(doc! {
                "tenant_id": tenant_id,
                "status": "Pending",
                "approvers": user_id,
            })
            .await?;
        Ok(cursor.try_collect().await?)
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .await?;
        Ok(cursor.try_collect().await?)
    }

    async fn scan_expired_approvals(
        &self,
        limit: u32,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
        let now = chrono::Utc::now().to_rfc3339();
        let filter = doc! {
            "status": "Pending",
            "expires_at": { "$ne": mongodb::bson::Bson::Null, "$lt": &now },
        };
        let cursor = self.collection.find(filter).limit(limit as i64).await?;
        Ok(cursor.try_collect().await?)
    }
}
