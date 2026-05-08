//! Workflow execution is split across [`workflow`], [`apply_exec`], and [`ensure_task_job`];
//! this file holds construction, plugin registration, and the [`PluginExecutor`] bridge.

mod apply_exec;
mod ensure_task_job;
mod loop_action;
mod workflow;

pub use workflow::resolved_llm_request_snapshot;

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::workflow::TaskType;
use crate::task::service::TaskInstanceService;
use crate::variable::service::VariableService;
use crate::workflow::entity::workflow_definition::{WorkflowInstanceEntity, WorkflowNodeInstanceEntity};
use crate::workflow::service::WorkflowInstanceService;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

pub struct PluginManager {
    pub(super) plugins: HashMap<TaskType, Box<dyn PluginInterface>>,
    pub(super) workflow_instance_svc: Arc<WorkflowInstanceService>,
    pub(super) task_instance_svc: Option<Arc<TaskInstanceService>>,
    pub(super) variable_svc: Option<VariableService>,
    pub(super) dispatcher: Arc<dyn crate::shared::job::TaskDispatcher>,
}

impl PluginManager {
    pub fn new(
        workflow_instance_svc: Arc<WorkflowInstanceService>,
        dispatcher: Arc<dyn crate::shared::job::TaskDispatcher>,
    ) -> Self {
        Self {
            plugins: HashMap::new(),
            workflow_instance_svc,
            task_instance_svc: None,
            variable_svc: None,
            dispatcher,
        }
    }

    pub fn with_variable_service(mut self, svc: VariableService) -> Self {
        self.variable_svc = Some(svc);
        self
    }

    pub fn with_task_instance_service(mut self, svc: Arc<TaskInstanceService>) -> Self {
        self.task_instance_svc = Some(svc);
        self
    }

    pub fn workflow_instance_svc(&self) -> &WorkflowInstanceService {
        &self.workflow_instance_svc
    }

    pub fn dispatcher(&self) -> Arc<dyn crate::shared::job::TaskDispatcher> {
        self.dispatcher.clone()
    }

    pub fn register(&mut self, plugin: Box<dyn PluginInterface>) {
        let task_type = plugin.plugin_type();
        self.plugins.insert(task_type, plugin);
    }
}

#[async_trait]
impl PluginExecutor for PluginManager {
    async fn execute_node_instance(
        &self,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let plugin = self.plugins.get(&node_instance.node_type).ok_or_else(|| {
            error!(
                node_type = ?node_instance.node_type,
                node_id = %node_instance.node_id,
                "no plugin registered for node type"
            );
            anyhow::anyhow!(
                "no plugin registered for task type: {:?}",
                node_instance.node_type
            )
        })?;

        plugin.execute(self, node_instance, workflow_instance).await
    }

    async fn handle_node_callback(
        &self,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
        child_task_id: &str,
        status: &crate::workflow::entity::workflow_definition::NodeExecutionStatus,
        output: &Option<serde_json::Value>,
        error_message: &Option<String>,
        input: &Option<serde_json::Value>,
    ) -> anyhow::Result<ExecutionResult> {
        let plugin = self.plugins.get(&node_instance.node_type).ok_or_else(|| {
            error!(
                node_type = ?node_instance.node_type,
                node_id = %node_instance.node_id,
                "no plugin registered for callback"
            );
            anyhow::anyhow!(
                "no plugin registered for task type: {:?}",
                node_instance.node_type
            )
        })?;

        plugin
            .handle_callback(
                self,
                node_instance,
                workflow_instance,
                child_task_id,
                status,
                output,
                error_message,
                input,
            )
            .await
    }

    async fn is_task_still_failed(&self, task_instance_id: &str) -> bool {
        let Some(ref task_svc) = self.task_instance_svc else {
            return true; // conservative: no service available
        };
        match task_svc.get_task_instance_entity(task_instance_id.to_string()).await {
            Ok(task) => {
                matches!(
                    task.task_status,
                    crate::shared::workflow::TaskInstanceStatus::Failed
                )
            }
            Err(_) => true,
        }
    }
}
