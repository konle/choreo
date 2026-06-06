use crate::auth::context::AuthContext;
use crate::auth::service::AuthService;
use domain::shared::job::TaskDispatcher;
use domain::user::entity::Permission;
use domain::workflow::entity::query::WorkflowInstanceQuery;
use domain::workflow::entity::workflow_definition::WorkflowInstanceEntity;
use domain::workflow::service::{WorkflowDefinitionService, WorkflowInstanceService};
use common::pagination::PaginatedData;
use std::sync::Arc;

pub struct WorkflowUsecase {
    #[allow(dead_code)]
    definition_service: WorkflowDefinitionService,
    instance_service: WorkflowInstanceService,
    #[allow(dead_code)]
    dispatcher: Arc<dyn TaskDispatcher>,
    auth_service: AuthService,
}

impl WorkflowUsecase {
    pub fn new(
        definition_service: WorkflowDefinitionService,
        instance_service: WorkflowInstanceService,
        dispatcher: Arc<dyn TaskDispatcher>,
        auth_service: AuthService,
    ) -> Self {
        Self {
            definition_service,
            instance_service,
            dispatcher,
            auth_service,
        }
    }

    pub async fn list_instances(
        &self,
        auth: &AuthContext,
        query: WorkflowInstanceQuery,
    ) -> Result<PaginatedData<WorkflowInstanceEntity>, String> {
        self.auth_service.authorize(auth, &Permission::ReadOnly)?;
        self.instance_service
            .list_workflow_instances(&auth.tenant_id, &query)
            .await
            .map_err(|e| format!("failed to list instances: {}", e))
    }
}
