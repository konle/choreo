use crate::auth::context::AuthContext;
use crate::auth::service::AuthService;
use common::pagination::PaginatedData;
use domain::task::entity::query::TaskInstanceQuery;
use domain::task::entity::task_definition::TaskInstanceEntity;
use domain::task::service::TaskInstanceService;
use domain::user::entity::Permission;

pub struct TaskUsecase {
    task_instance_service: TaskInstanceService,
    auth_service: AuthService,
}

impl TaskUsecase {
    pub fn new(task_instance_service: TaskInstanceService, auth_service: AuthService) -> Self {
        Self {
            task_instance_service,
            auth_service,
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
}
