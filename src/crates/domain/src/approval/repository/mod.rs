use crate::approval::entity::ApprovalInstanceEntity;
use async_trait::async_trait;
use std::error::Error;

pub type RepositoryError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait ApprovalRepository: Send + Sync {
    async fn create(
        &self,
        entity: &ApprovalInstanceEntity,
    ) -> Result<ApprovalInstanceEntity, RepositoryError>;
    async fn get_by_id(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<ApprovalInstanceEntity, RepositoryError>;
    async fn update(
        &self,
        entity: &ApprovalInstanceEntity,
    ) -> Result<ApprovalInstanceEntity, RepositoryError>;

    async fn find_by_workflow_and_node(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        node_id: &str,
    ) -> Result<Option<ApprovalInstanceEntity>, RepositoryError>;

    async fn list_pending_by_approver(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError>;

    async fn list_by_tenant(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError>;

    /// Scan pending approval instances whose `expires_at` has passed.
    async fn scan_expired_approvals(
        &self,
        limit: u32,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError>;
}
