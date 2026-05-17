use async_trait::async_trait;
use std::error::Error;
use common::pagination::PaginatedData;
use crate::shared::workflow::{WorkflowInstanceStatus, WorkflowStatus};
use crate::workflow::entity::workflow_definition::{WorkflowEntity, WorkflowInstanceEntity};
use crate::workflow::entity::workflow_definition::WorkflowMetaEntity;
use crate::workflow::entity::query::WorkflowInstanceQuery;
pub type RepositoryError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait WorkflowDefinitionRepository: Send + Sync {
    async fn get_workflow_entity(&self, workflow_meta_id: String, version: u32) -> Result<WorkflowEntity, RepositoryError>;
    async fn list_workflow_entities(&self, workflow_meta_id: &str) -> Result<Vec<WorkflowEntity>, RepositoryError>;
    async fn save_workflow_entity(&self, entity: &WorkflowEntity) -> Result<(), RepositoryError>;
    //async fn publish_workflow_entity(&self, workflow_meta_id: &str, version: u32) -> Result<(), RepositoryError>;
    //async fn delete_workflow_entity(&self, workflow_meta_id: String, version: u32) -> Result<(), RepositoryError>;
    async fn max_version(&self, workflow_meta_id: String) -> Result<u32, RepositoryError>;
    async fn transition_status(&self, workflow_meta_id: String, version: u32, from_status: &WorkflowStatus, to_status: &WorkflowStatus) -> Result<(), RepositoryError>;

    async fn get_workflow_meta_entity(&self, workflow_meta_id: String) -> Result<WorkflowMetaEntity, RepositoryError>;
    async fn get_workflow_meta_entity_scoped(&self, tenant_id: &str, workflow_meta_id: &str) -> Result<WorkflowMetaEntity, RepositoryError>;
    async fn list_workflow_meta_entities(&self, tenant_id: &str) -> Result<Vec<WorkflowMetaEntity>, RepositoryError>;
    async fn save_workflow_meta_entity(&self, entity: &WorkflowMetaEntity) -> Result<(), RepositoryError>;
    async fn delete_workflow_meta_entity(&self, tenant_id: &str, workflow_meta_id: &str) -> Result<(), RepositoryError>;
    async fn create_workflow_meta_entity(&self, workflow_meta_entity: &WorkflowMetaEntity) -> Result<WorkflowMetaEntity, RepositoryError>;
}

#[async_trait]
pub trait WorkflowInstanceRepository: Send + Sync {
    async fn get_workflow_instance(&self, id: String) -> Result<WorkflowInstanceEntity, RepositoryError>;
    async fn get_workflow_instance_scoped(&self, tenant_id: &str, id: &str) -> Result<WorkflowInstanceEntity, RepositoryError>;
    async fn list_workflow_instances(
        &self,
        tenant_id: &str,
        query: &WorkflowInstanceQuery,
    ) -> Result<PaginatedData<WorkflowInstanceEntity>, RepositoryError>;

    async fn transfer_status(
        &self,
        workflow_instance_id: &str,
        from_status: &WorkflowInstanceStatus,
        to_status: &WorkflowInstanceStatus,
    ) -> Result<WorkflowInstanceEntity, RepositoryError>;

    async fn acquire_lock(
        &self,
        workflow_instance_id: &str,
        worker_id: &str,
        duration_ms: u64,
    ) -> Result<WorkflowInstanceEntity, RepositoryError>;

    async fn release_lock(
        &self,
        workflow_instance_id: &str,
        worker_id: &str,
    ) -> Result<(), RepositoryError>;

    async fn create_workflow_instance(
        &self,
        instance: &WorkflowInstanceEntity,
    ) -> Result<WorkflowInstanceEntity, RepositoryError>;

    async fn save_workflow_instance(
        &self,
        instance: &WorkflowInstanceEntity,
    ) -> Result<(), RepositoryError>;

    /// Scan zombie instances: status in {Running, Await} and lease expired or absent.
    async fn scan_zombie_instances(
        &self,
        limit: u32,
    ) -> Result<Vec<WorkflowInstanceEntity>, RepositoryError>;

    /// CAS-based lock clearing: only succeeds if epoch matches.
    async fn force_clear_lock(
        &self,
        workflow_instance_id: &str,
        expected_epoch: u64,
    ) -> Result<(), RepositoryError>;

    /// Scan instances by status (for sweeper use).
    async fn scan_instances_by_status(
        &self,
        status: &WorkflowInstanceStatus,
        limit: u32,
    ) -> Result<Vec<WorkflowInstanceEntity>, RepositoryError>;
}
