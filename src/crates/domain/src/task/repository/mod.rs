use crate::shared::workflow::TaskInstanceStatus;
use crate::task::entity::query::TaskInstanceQuery;
use crate::task::entity::task_definition::{TaskEntity, TaskInstanceEntity, TaskTransitionFields};
use async_trait::async_trait;
use common::pagination::PaginatedData;
use std::error::Error;

pub type RepositoryError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait TaskEntityRepository: Send + Sync {
    async fn create_task_entity(
        &self,
        task_entity: TaskEntity,
    ) -> Result<TaskEntity, RepositoryError>;
    async fn get_task_entity(&self, id: String) -> Result<TaskEntity, RepositoryError>;
    async fn get_task_entity_scoped(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<TaskEntity, RepositoryError>;
    async fn list_task_entities(&self, tenant_id: &str)
    -> Result<Vec<TaskEntity>, RepositoryError>;
    async fn list_task_entities_by_type(
        &self,
        tenant_id: &str,
        task_type: &str,
    ) -> Result<Vec<TaskEntity>, RepositoryError>;
    async fn update_task_entity(
        &self,
        task_entity: TaskEntity,
    ) -> Result<TaskEntity, RepositoryError>;
    async fn delete_task_entity(&self, tenant_id: &str, id: &str) -> Result<(), RepositoryError>;
}

#[async_trait]
pub trait TaskInstanceEntityRepository: Send + Sync {
    async fn create_task_instance_entity(
        &self,
        task_instance_entity: TaskInstanceEntity,
    ) -> Result<TaskInstanceEntity, RepositoryError>;
    async fn get_task_instance_entity(
        &self,
        id: String,
    ) -> Result<TaskInstanceEntity, RepositoryError>;
    async fn get_task_instance_entity_scoped(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<TaskInstanceEntity, RepositoryError>;
    async fn list_task_instance_entities(
        &self,
        query: &TaskInstanceQuery,
    ) -> Result<PaginatedData<TaskInstanceEntity>, RepositoryError>;
    async fn update_task_instance_entity(
        &self,
        task_instance_entity: TaskInstanceEntity,
    ) -> Result<TaskInstanceEntity, RepositoryError>;

    /// CAS-style atomic status transition with optional extra fields set in the same operation.
    /// `fields` carries Optional fields (output, input, error_message) to be $set alongside
    /// the status change; None values are simply omitted from the update (not unset).
    async fn transfer_status_with_fields(
        &self,
        task_instance_id: &str,
        from_status: &TaskInstanceStatus,
        to_status: &TaskInstanceStatus,
        fields: TaskTransitionFields,
    ) -> Result<TaskInstanceEntity, RepositoryError>;
}
