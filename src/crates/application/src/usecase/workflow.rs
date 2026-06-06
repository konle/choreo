#![allow(deprecated)]

use crate::auth::context::AuthContext;
use crate::auth::service::AuthService;
use domain::shared::job::{ExecuteWorkflowJob, TaskDispatcher, WorkflowEvent};
use domain::user::entity::Permission;
use domain::workflow::entity::query::WorkflowInstanceQuery;
use domain::workflow::entity::workflow_definition::{
    NodeExecutionStatus, WorkflowInstanceEntity, WorkflowMetaEntity,
};
use domain::workflow::service::{WorkflowDefinitionService, WorkflowInstanceService};
use domain::workflow::service::node_callback_child_task_id;
use common::pagination::PaginatedData;
use serde_json::Value as JsonValue;
use std::sync::Arc;

pub struct WorkflowUsecase {
    definition_service: WorkflowDefinitionService,
    instance_service: WorkflowInstanceService,
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

    // ── Read operations ──

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

    pub async fn get_instance(
        &self,
        auth: &AuthContext,
        instance_id: &str,
    ) -> Result<WorkflowInstanceEntity, String> {
        self.auth_service.authorize(auth, &Permission::ReadOnly)?;
        self.instance_service
            .get_workflow_instance_scoped(&auth.tenant_id, instance_id)
            .await
            .map_err(|e| format!("failed to get instance: {}", e))
    }

    pub async fn list_definitions(
        &self,
        auth: &AuthContext,
    ) -> Result<Vec<WorkflowMetaEntity>, String> {
        self.auth_service.authorize(auth, &Permission::ReadOnly)?;
        self.definition_service
            .list_workflow_meta_entities(&auth.tenant_id)
            .await
            .map_err(|e| format!("failed to list definitions: {}", e))
    }

    pub async fn get_definition(
        &self,
        auth: &AuthContext,
        meta_id: &str,
    ) -> Result<WorkflowMetaEntity, String> {
        self.auth_service.authorize(auth, &Permission::ReadOnly)?;
        self.definition_service
            .get_workflow_meta_entity_scoped(&auth.tenant_id, meta_id)
            .await
            .map_err(|e| format!("failed to get definition: {}", e))
    }

    // ── Write operations ──

    pub async fn execute_instance(
        &self,
        auth: &AuthContext,
        instance_id: &str,
    ) -> Result<WorkflowInstanceEntity, String> {
        self.auth_service
            .authorize(auth, &Permission::InstanceExecute)?;
        let instance = self
            .instance_service
            .start_instance(instance_id)
            .await
            .map_err(|e| format!("failed to execute instance: {}", e))?;
        self.dispatcher
            .dispatch_workflow(ExecuteWorkflowJob {
                workflow_instance_id: instance.workflow_instance_id.clone(),
                tenant_id: instance.tenant_id.clone(),
                event: WorkflowEvent::Start,
            })
            .await
            .map_err(|e| format!("failed to dispatch: {}", e))?;
        Ok(instance)
    }

    pub async fn cancel_instance(
        &self,
        auth: &AuthContext,
        instance_id: &str,
    ) -> Result<WorkflowInstanceEntity, String> {
        self.auth_service
            .authorize(auth, &Permission::InstanceExecute)?;
        self.instance_service
            .cancel_instance(instance_id)
            .await
            .map_err(|e| format!("failed to cancel instance: {}", e))
    }

    #[allow(deprecated)]
    pub async fn retry_instance(
        &self,
        auth: &AuthContext,
        instance_id: &str,
    ) -> Result<WorkflowInstanceEntity, String> {
        self.auth_service
            .authorize(auth, &Permission::InstanceExecute)?;
        self.instance_service
            .retry_instance(instance_id)
            // Note: retry_instance is deprecated in favor of retry_workflow_node,
            // but we intentionally use the instance-level retry here
            .await
            .map_err(|e| format!("failed to retry instance: {}", e))
    }

    pub async fn skip_node(
        &self,
        auth: &AuthContext,
        instance_id: &str,
        node_id: &str,
        output: JsonValue,
    ) -> Result<WorkflowInstanceEntity, String> {
        self.auth_service
            .authorize(auth, &Permission::InstanceExecute)?;

        let instance = self
            .instance_service
            .skip_workflow_node(&auth.tenant_id, instance_id, node_id, None, output.clone())
            .await
            .map_err(|e| format!("failed to skip node: {}", e))?;

        let node = instance.nodes.iter().find(|n| n.node_id == node_id)
            .ok_or_else(|| "node not found in instance".to_string())?;
        let child_task_id = node_callback_child_task_id(&instance, node);

        self.dispatcher
            .dispatch_workflow(ExecuteWorkflowJob {
                workflow_instance_id: instance.workflow_instance_id.clone(),
                tenant_id: instance.tenant_id.clone(),
                event: WorkflowEvent::NodeCallback {
                    node_id: node_id.to_string(),
                    child_task_id,
                    status: NodeExecutionStatus::Skipped,
                    output: Some(output),
                    error_message: None,
                    input: None,
                },
            })
            .await
            .map_err(|e| format!("failed to dispatch callback: {}", e))?;

        Ok(instance)
    }
}
