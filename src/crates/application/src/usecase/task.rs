use crate::auth::context::AuthContext;
use crate::auth::service::AuthService;
use common::pagination::PaginatedData;
use domain::shared::job::{ExecuteTaskJob, TaskDispatcher};
use domain::task::entity::query::TaskInstanceQuery;
use domain::shared::workflow::TaskInstanceStatus;
use domain::task::entity::task_definition::TaskInstanceEntity;
use domain::task::service::{TaskInstanceService, TaskService};
use domain::user::entity::Permission;
use domain::variable::service::VariableService;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

pub struct TaskUsecase {
    task_service: TaskService,
    task_instance_service: TaskInstanceService,
    variable_service: VariableService,
    auth_service: AuthService,
    dispatcher: Arc<dyn TaskDispatcher>,
}

impl TaskUsecase {
    pub fn new(
        task_service: TaskService,
        task_instance_service: TaskInstanceService,
        variable_service: VariableService,
        auth_service: AuthService,
        dispatcher: Arc<dyn TaskDispatcher>,
    ) -> Self {
        Self {
            task_service,
            task_instance_service,
            variable_service,
            auth_service,
            dispatcher,
        }
    }

    pub async fn list_instances(
        &self,
        auth: &AuthContext,
        query: TaskInstanceQuery,
    ) -> Result<PaginatedData<TaskInstanceEntity>, String> {
        self.auth_service.authorize(auth, &Permission::ReadOnly)?;
        self.task_instance_service
            .list_task_instance_entities(&query)
            .await
            .map_err(|e| format!("failed to list task instances: {}", e))
    }

    pub async fn get_instance(
        &self,
        auth: &AuthContext,
        id: &str,
    ) -> Result<TaskInstanceEntity, String> {
        self.auth_service.authorize(auth, &Permission::ReadOnly)?;
        self.task_instance_service
            .get_task_instance_entity_scoped(&auth.tenant_id, id)
            .await
            .map_err(|e| format!("failed to get task instance: {}", e))
    }

    pub async fn retry_instance(
        &self,
        auth: &AuthContext,
        id: &str,
    ) -> Result<TaskInstanceEntity, String> {
        self.auth_service
            .authorize(auth, &Permission::InstanceExecute)?;
        self.task_instance_service
            .retry_instance(id)
            .await
            .map_err(|e| format!("failed to retry task instance: {}", e))
    }

    pub async fn execute_task_by_name(
        &self,
        auth: &AuthContext,
        task_name: &str,
        context: Option<JsonValue>,
    ) -> Result<TaskInstanceEntity, String> {
        self.auth_service
            .authorize(auth, &Permission::InstanceExecute)?;

        let tasks = self
            .task_service
            .list_task_entities(&auth.tenant_id)
            .await
            .map_err(|e| format!("failed to list tasks: {}", e))?;

        let task = tasks
            .into_iter()
            .find(|t| t.name == task_name)
            .ok_or_else(|| format!("task not found: {}", task_name))?;

        // Resolve tenant-level variables into context
        let user_ctx = context.unwrap_or(serde_json::json!({}));
        let resolved_ctx = self
            .variable_service
            .resolve_standalone_context(&auth.tenant_id, &user_ctx)
            .await
            .unwrap_or(user_ctx);

        let now = chrono::Utc::now();
        let instance_id = Uuid::new_v4().to_string();
        let instance = TaskInstanceEntity {
            id: Uuid::new_v4().to_string(),
            tenant_id: auth.tenant_id.clone(),
            task_id: task.id.clone(),
            task_name: task.name.clone(),
            task_type: task.task_type.clone(),
            task_template: task.task_template.clone(),
            task_status: TaskInstanceStatus::Pending,
            task_instance_id: instance_id.clone(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
            input: Some(resolved_ctx),
            output: None,
            error_message: None,
            execution_duration: None,
            caller_context: None,
        };

        self.task_instance_service
            .create_task_instance_entity(instance)
            .await
            .map_err(|e| format!("failed to create instance: {}", e))?;

        self.dispatcher
            .dispatch_task(ExecuteTaskJob {
                task_instance_id: instance_id.clone(),
                tenant_id: auth.tenant_id.clone(),
                caller_context: None,
            })
            .await
            .map_err(|e| format!("failed to dispatch task: {}", e))?;

        self.task_instance_service
            .get_task_instance_entity(instance_id)
            .await
            .map_err(|e| format!("failed to get created instance: {}", e))
    }
}
