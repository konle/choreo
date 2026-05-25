use crate::shared::workflow::{TaskInstanceStatus, TaskStatus, TaskType};
use crate::task::entity::query::TaskInstanceQuery;
use crate::task::entity::task_definition::{
    TaskEntity, TaskInstanceEntity, TaskTemplate, TaskTransitionFields,
};
use crate::task::repository::{
    RepositoryError, TaskEntityRepository, TaskInstanceEntityRepository,
};
use chrono::Utc;
use common::pagination::PaginatedData;
use std::sync::Arc;
use uuid::Uuid;

pub struct CreateTaskCommand {
    pub name: String,
    pub task_type: TaskType,
    pub task_template: TaskTemplate,
    pub description: String,
    pub status: TaskStatus,
}

pub struct UpdateTaskCommand {
    pub name: String,
    pub task_type: TaskType,
    pub task_template: TaskTemplate,
    pub description: String,
    pub status: TaskStatus,
}

#[derive(Clone)]
pub struct TaskService {
    pub task_entity_repository: Arc<dyn TaskEntityRepository>,
}

impl TaskService {
    pub fn new(task_entity_repository: Arc<dyn TaskEntityRepository>) -> Self {
        Self {
            task_entity_repository,
        }
    }

    pub async fn create_task(
        &self,
        tenant_id: String,
        cmd: CreateTaskCommand,
    ) -> Result<TaskEntity, RepositoryError> {
        let now = Utc::now();
        let entity = TaskEntity::new(
            Uuid::new_v4().to_string(),
            tenant_id,
            cmd.name,
            cmd.task_type,
            cmd.task_template,
            cmd.description,
            cmd.status,
            now,
            now,
            None,
        );
        self.task_entity_repository.create_task_entity(entity).await
    }

    pub async fn update_task(
        &self,
        tenant_id: &str,
        id: &str,
        cmd: UpdateTaskCommand,
    ) -> Result<TaskEntity, RepositoryError> {
        let existing = self.get_task_entity_scoped(tenant_id, id).await?;
        let now = Utc::now();
        let entity = TaskEntity::new(
            id.to_string(),
            tenant_id.to_string(),
            cmd.name,
            cmd.task_type,
            cmd.task_template,
            cmd.description,
            cmd.status,
            existing.created_at,
            now,
            existing.deleted_at,
        );
        self.task_entity_repository.update_task_entity(entity).await
    }

    pub async fn get_task_entity(&self, id: String) -> Result<TaskEntity, RepositoryError> {
        self.task_entity_repository.get_task_entity(id).await
    }

    pub async fn get_task_entity_scoped(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<TaskEntity, RepositoryError> {
        self.task_entity_repository
            .get_task_entity_scoped(tenant_id, id)
            .await
    }

    pub async fn list_task_entities(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<TaskEntity>, RepositoryError> {
        self.task_entity_repository
            .list_task_entities(tenant_id)
            .await
    }

    pub async fn list_task_entities_by_type(
        &self,
        tenant_id: &str,
        task_type: &str,
    ) -> Result<Vec<TaskEntity>, RepositoryError> {
        self.task_entity_repository
            .list_task_entities_by_type(tenant_id, task_type)
            .await
    }

    pub async fn update_task_entity(
        &self,
        task_entity: TaskEntity,
    ) -> Result<TaskEntity, RepositoryError> {
        self.task_entity_repository
            .update_task_entity(task_entity)
            .await
    }

    pub async fn delete_task_entity(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<(), RepositoryError> {
        self.task_entity_repository
            .delete_task_entity(tenant_id, id)
            .await
    }
}

#[derive(Clone)]
pub struct TaskInstanceService {
    pub task_instance_entity_repository: Arc<dyn TaskInstanceEntityRepository>,
}

impl TaskInstanceService {
    pub fn new(task_instance_entity_repository: Arc<dyn TaskInstanceEntityRepository>) -> Self {
        Self {
            task_instance_entity_repository,
        }
    }

    pub async fn create_task_instance_entity(
        &self,
        task_instance_entity: TaskInstanceEntity,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.task_instance_entity_repository
            .create_task_instance_entity(task_instance_entity)
            .await
    }

    pub async fn get_task_instance_entity(
        &self,
        id: String,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.task_instance_entity_repository
            .get_task_instance_entity(id)
            .await
    }

    pub async fn get_task_instance_entity_scoped(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.task_instance_entity_repository
            .get_task_instance_entity_scoped(tenant_id, id)
            .await
    }

    pub async fn list_task_instance_entities(
        &self,
        query: &TaskInstanceQuery,
    ) -> Result<PaginatedData<TaskInstanceEntity>, RepositoryError> {
        self.task_instance_entity_repository
            .list_task_instance_entities(query)
            .await
    }

    pub async fn update_task_instance_entity(
        &self,
        task_instance_entity: TaskInstanceEntity,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.task_instance_entity_repository
            .update_task_instance_entity(task_instance_entity)
            .await
    }

    async fn transfer_status_with_fields(
        &self,
        task_instance_id: &str,
        from: &TaskInstanceStatus,
        to: &TaskInstanceStatus,
        fields: TaskTransitionFields,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        if !from.can_transition_to(to) {
            return Err(
                format!("invalid task instance state transition: {} -> {}", from, to).into(),
            );
        }
        self.task_instance_entity_repository
            .transfer_status_with_fields(task_instance_id, from, to, fields)
            .await
    }

    /// Pending -> Running
    pub async fn submit_instance(
        &self,
        task_instance_id: &str,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.transfer_status_with_fields(
            task_instance_id,
            &TaskInstanceStatus::Pending,
            &TaskInstanceStatus::Running,
            TaskTransitionFields::default(),
        )
        .await
    }

    /// Running -> Completed
    pub async fn complete_instance(
        &self,
        task_instance_id: &str,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.transfer_status_with_fields(
            task_instance_id,
            &TaskInstanceStatus::Running,
            &TaskInstanceStatus::Completed,
            TaskTransitionFields::default(),
        )
        .await
    }

    /// Running -> Failed
    pub async fn fail_instance(
        &self,
        task_instance_id: &str,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.transfer_status_with_fields(
            task_instance_id,
            &TaskInstanceStatus::Running,
            &TaskInstanceStatus::Failed,
            TaskTransitionFields::default(),
        )
        .await
    }

    /// Running -> Completed with output and input set atomically.
    pub async fn complete_with_output(
        &self,
        task_instance_id: &str,
        output: Option<serde_json::Value>,
        input: Option<serde_json::Value>,
        execution_duration: Option<u64>,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.transfer_status_with_fields(
            task_instance_id,
            &TaskInstanceStatus::Running,
            &TaskInstanceStatus::Completed,
            TaskTransitionFields {
                output,
                input,
                error_message: None,
                execution_duration,
            },
        )
        .await
    }

    /// Running -> Failed with error_message set atomically.
    pub async fn fail_with_error(
        &self,
        task_instance_id: &str,
        error_message: String,
        execution_duration: Option<u64>,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.transfer_status_with_fields(
            task_instance_id,
            &TaskInstanceStatus::Running,
            &TaskInstanceStatus::Failed,
            TaskTransitionFields {
                output: None,
                input: None,
                error_message: Some(error_message),
                execution_duration,
            },
        )
        .await
    }

    /// Failed -> Pending
    pub async fn retry_instance(
        &self,
        task_instance_id: &str,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.transfer_status_with_fields(
            task_instance_id,
            &TaskInstanceStatus::Failed,
            &TaskInstanceStatus::Pending,
            TaskTransitionFields::default(),
        )
        .await
    }

    /// Pending | Failed -> Canceled
    pub async fn cancel_instance(
        &self,
        task_instance_id: &str,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        let instance = self
            .get_task_instance_entity(task_instance_id.to_string())
            .await?;

        if instance.task_status.is_terminal() {
            return Err(format!(
                "cannot cancel task instance in terminal state: {}",
                instance.task_status
            )
            .into());
        }

        match instance.task_status {
            TaskInstanceStatus::Pending | TaskInstanceStatus::Failed => {
                self.transfer_status_with_fields(
                    task_instance_id,
                    &instance.task_status,
                    &TaskInstanceStatus::Canceled,
                    TaskTransitionFields::default(),
                )
                .await
            }
            other => Err(format!(
                "cannot cancel task instance in state: {}, only Pending or Failed can be canceled",
                other
            )
            .into()),
        }
    }
}
