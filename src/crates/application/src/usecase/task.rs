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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token::TokenService;
    use common::pagination::PaginatedData;
    use domain::shared::job::{ExecuteTaskJob, TaskDispatcher};
    use domain::shared::workflow::{TaskInstanceStatus as TIS, TaskStatus, TaskType};
    use domain::task::entity::query::TaskInstanceQuery;
    use domain::task::entity::task_definition::{TaskEntity, TaskTemplate, TaskTransitionFields};
    use domain::task::repository::{RepositoryError, TaskEntityRepository, TaskInstanceEntityRepository};
    use domain::user::entity::TenantRole;
    use domain::variable::entity::{VariableEntity, VariableScope};
    use domain::variable::repository::VariableRepository;
    use std::sync::Mutex;
    use async_trait::async_trait;

    struct MockTaskRepo {
        tasks: Mutex<Vec<TaskEntity>>,
    }

    #[async_trait]
    impl TaskEntityRepository for MockTaskRepo {
        async fn create_task_entity(&self, e: TaskEntity) -> Result<TaskEntity, RepositoryError> {
            self.tasks.lock().unwrap().push(e.clone());
            Ok(e)
        }
        async fn get_task_entity(&self, _: String) -> Result<TaskEntity, RepositoryError> { unimplemented!() }
        async fn get_task_entity_scoped(&self, _: &str, _: &str) -> Result<TaskEntity, RepositoryError> { unimplemented!() }
        async fn list_task_entities(&self, _: &str) -> Result<Vec<TaskEntity>, RepositoryError> {
            Ok(self.tasks.lock().unwrap().clone())
        }
        async fn list_task_entities_by_type(&self, _: &str, _: &str) -> Result<Vec<TaskEntity>, RepositoryError> { unimplemented!() }
        async fn update_task_entity(&self, _: TaskEntity) -> Result<TaskEntity, RepositoryError> { unimplemented!() }
        async fn delete_task_entity(&self, _: &str, _: &str) -> Result<(), RepositoryError> { unimplemented!() }
    }

    impl MockTaskRepo {
        fn new(tasks: Vec<TaskEntity>) -> Self { Self { tasks: Mutex::new(tasks) } }
    }

    struct MockTaskInstanceRepo {
        instances: Mutex<Vec<TaskInstanceEntity>>,
    }

    #[async_trait]
    impl TaskInstanceEntityRepository for MockTaskInstanceRepo {
        async fn create_task_instance_entity(&self, inst: TaskInstanceEntity) -> Result<TaskInstanceEntity, RepositoryError> {
            self.instances.lock().unwrap().push(inst.clone());
            Ok(inst)
        }
        async fn get_task_instance_entity(&self, id: String) -> Result<TaskInstanceEntity, RepositoryError> {
            self.instances.lock().unwrap().iter().find(|i| i.task_instance_id == id).cloned().ok_or_else(|| "not found".into())
        }
        async fn get_task_instance_entity_scoped(&self, _: &str, _: &str) -> Result<TaskInstanceEntity, RepositoryError> { unimplemented!() }
        async fn list_task_instance_entities(&self, _: &TaskInstanceQuery) -> Result<PaginatedData<TaskInstanceEntity>, RepositoryError> { unimplemented!() }
        async fn update_task_instance_entity(&self, _: TaskInstanceEntity) -> Result<TaskInstanceEntity, RepositoryError> { unimplemented!() }
        async fn transfer_status_with_fields(&self, _: &str, _: &TIS, _: &TIS, _: TaskTransitionFields) -> Result<TaskInstanceEntity, RepositoryError> { unimplemented!() }
    }

    impl MockTaskInstanceRepo {
        fn new() -> Self { Self { instances: Mutex::new(vec![]) } }
    }

    #[derive(Clone)]
    struct MockVariableRepo;

    #[async_trait]
    impl VariableRepository for MockVariableRepo {
        async fn create(&self, _: &VariableEntity) -> Result<VariableEntity, RepositoryError> { unimplemented!() }
        async fn get_by_id(&self, _: &str, _: &str) -> Result<VariableEntity, RepositoryError> { unimplemented!() }
        async fn update(&self, _: &VariableEntity) -> Result<VariableEntity, RepositoryError> { unimplemented!() }
        async fn delete(&self, _: &str, _: &str) -> Result<(), RepositoryError> { unimplemented!() }
        async fn list_by_scope(&self, _: &str, _: &VariableScope, _: &str) -> Result<Vec<VariableEntity>, RepositoryError> { Ok(vec![]) }
        async fn get_by_key(&self, _: &str, _: &VariableScope, _: &str, _: &str) -> Result<Option<VariableEntity>, RepositoryError> { unimplemented!() }
    }

    #[derive(Clone)]
    struct MockDispatcher {
        dispatched_tasks: Arc<Mutex<Vec<ExecuteTaskJob>>>,
    }

    #[async_trait]
    impl TaskDispatcher for MockDispatcher {
        async fn dispatch_task(&self, job: ExecuteTaskJob) -> anyhow::Result<()> {
            self.dispatched_tasks.lock().unwrap().push(job);
            Ok(())
        }
        async fn dispatch_workflow(&self, _: domain::shared::job::ExecuteWorkflowJob) -> anyhow::Result<()> { unimplemented!() }
    }

    impl MockDispatcher {
        fn new() -> Self { Self { dispatched_tasks: Arc::new(Mutex::new(vec![])) } }
    }

    fn make_auth_developer() -> AuthContext {
        AuthContext {
            user_id: "u1".into(), username: "dev".into(),
            is_super_admin: false, tenant_id: "t1".into(),
            role: Some(TenantRole::Developer),
        }
    }

    fn make_auth_viewer() -> AuthContext {
        AuthContext {
            user_id: "u2".into(), username: "viewer".into(),
            is_super_admin: false, tenant_id: "t1".into(),
            role: Some(TenantRole::Viewer),
        }
    }

    fn make_test_task(name: &str, tenant_id: &str) -> TaskEntity {
        TaskEntity::new(
            format!("task-{}", name), tenant_id.into(), name.into(),
            TaskType::Http, TaskTemplate::Grpc,
            "desc".into(), TaskStatus::Published,
            chrono::Utc::now(), chrono::Utc::now(), None,
        )
    }

    fn make_usecase() -> (TaskUsecase, MockDispatcher, Arc<MockTaskRepo>) {
        let task_repo = Arc::new(MockTaskRepo::new(vec![]));
        let ti_repo: Arc<dyn TaskInstanceEntityRepository> = Arc::new(MockTaskInstanceRepo::new());
        let var_repo: Arc<dyn VariableRepository> = Arc::new(MockVariableRepo);
        let auth_service = AuthService::new(TokenService::new("test_secret".into()));
        let dispatcher = MockDispatcher::new();
        let usecase = TaskUsecase::new(
            TaskService::new(task_repo.clone()),
            TaskInstanceService::new(ti_repo),
            VariableService::new(var_repo, "enc_key_32_bytes_long_enough!!".into()),
            auth_service,
            Arc::new(dispatcher.clone()),
        );
        (usecase, dispatcher, task_repo)
    }

    #[tokio::test]
    async fn execute_task_by_name_success() {
        let (usecase, dispatcher, task_repo) = make_usecase();
        task_repo.tasks.lock().unwrap().push(make_test_task("myhttp", "t1"));

        let result = usecase.execute_task_by_name(&make_auth_developer(), "myhttp", None).await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
        let instance = result.unwrap();
        assert_eq!(instance.task_name, "myhttp");
        assert_eq!(instance.task_status, TIS::Pending);
        assert_eq!(dispatcher.dispatched_tasks.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn execute_task_by_name_permission_denied() {
        let (usecase, _dispatcher, _repo) = make_usecase();
        let result = usecase.execute_task_by_name(&make_auth_viewer(), "myhttp", None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("insufficient permissions"));
    }

    #[tokio::test]
    async fn execute_task_by_name_not_found() {
        let (usecase, _dispatcher, _repo) = make_usecase();
        let result = usecase.execute_task_by_name(&make_auth_developer(), "nonexistent", None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("task not found"));
    }
}
