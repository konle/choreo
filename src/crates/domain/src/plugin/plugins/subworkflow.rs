use async_trait::async_trait;
use tracing::{info, error};

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::job::{ExecuteWorkflowJob, WorkflowCallerContext, WorkflowEvent};
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::TaskTemplate;
use crate::workflow::entity::workflow_definition::{WorkflowInstanceEntity, WorkflowNodeInstanceEntity};
use crate::workflow::service::{WorkflowDefinitionService, WorkflowInstanceService};

const MAX_SUB_WORKFLOW_DEPTH: u32 = 10;

pub struct SubWorkflowPlugin {
    definition_svc: WorkflowDefinitionService,
    instance_svc: WorkflowInstanceService,
}

impl SubWorkflowPlugin {
    pub fn new(
        definition_svc: WorkflowDefinitionService,
        instance_svc: WorkflowInstanceService,
    ) -> Self {
        Self { definition_svc, instance_svc }
    }
}

#[async_trait]
impl PluginInterface for SubWorkflowPlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::SubWorkflow(t) => t,
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for SubWorkflowPlugin");
                return Err(anyhow::anyhow!("Invalid task template for SubWorkflowPlugin"));
            }
        };

        // Re-evaluation: check if a child workflow instance already exists
        if let Some(existing_child_id) = node_instance
            .task_instance
            .output
            .as_ref()
            .and_then(|o| o.get("child_workflow_instance_id"))
            .and_then(|v| v.as_str())
        {
            match self.instance_svc.get_workflow_instance(existing_child_id.to_string()).await {
                Ok(child) => {
                    use crate::shared::workflow::WorkflowInstanceStatus;
                    use crate::workflow::entity::workflow_definition::NodeExecutionStatus;
                    return match child.status {
                        WorkflowInstanceStatus::Completed => {
                            info!(
                                parent_workflow_id = %workflow_instance.workflow_instance_id,
                                child_workflow_id = %existing_child_id,
                                "sub-workflow re-evaluation: child already completed"
                            );
                            node_instance.task_instance.output = Some(serde_json::json!({
                                "child_workflow_instance_id": existing_child_id,
                            }));
                            Ok(ExecutionResult::success(None))
                        }
                        WorkflowInstanceStatus::Failed => {
                            info!(
                                parent_workflow_id = %workflow_instance.workflow_instance_id,
                                child_workflow_id = %existing_child_id,
                                "sub-workflow re-evaluation: child still failed"
                            );
                            Ok(ExecutionResult::failed())
                        }
                        _ => {
                            info!(
                                parent_workflow_id = %workflow_instance.workflow_instance_id,
                                child_workflow_id = %existing_child_id,
                                child_status = ?child.status,
                                "sub-workflow re-evaluation: child still running, awaiting callback"
                            );
                            Ok(ExecutionResult {
                                status: NodeExecutionStatus::Await,
                                dispatch_jobs: vec![],
                                dispatch_workflow_jobs: vec![],
                                jump_to_node: None,
                            })
                        }
                    };
                }
                Err(_) => {
                    // Child instance not found — fall through to create a new one
                }
            }
        }

        let child_depth = workflow_instance.depth + 1;
        if child_depth > MAX_SUB_WORKFLOW_DEPTH {
            error!(
                workflow_instance_id = %workflow_instance.workflow_instance_id,
                depth = child_depth,
                max = MAX_SUB_WORKFLOW_DEPTH,
                "sub-workflow nesting depth exceeded"
            );
            return Err(anyhow::anyhow!(
                "Sub-workflow nesting depth exceeded maximum ({}), possible circular reference",
                MAX_SUB_WORKFLOW_DEPTH
            ));
        }

        let workflow_entity = self.definition_svc
            .get_workflow_entity(template.workflow_meta_id.clone(), template.workflow_version)
            .await
            .map_err(|e| {
                error!(
                    workflow_meta_id = %template.workflow_meta_id,
                    version = template.workflow_version,
                    error = %e,
                    "failed to load sub-workflow template"
                );
                anyhow::anyhow!("Failed to load sub-workflow template: {}", e)
            })?;

        let mut child_context = workflow_instance.context.clone();
        if !template.form.is_empty() {
            if let serde_json::Value::Object(ref mut ctx) = child_context {
                for field in &template.form {
                    ctx.insert(field.key.clone(), serde_json::to_value(&field.value).unwrap_or_default());
                }
            }
        }

        let child_context_snapshot = child_context.clone();

        let parent_ctx = WorkflowCallerContext {
            workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
            node_id: node_instance.node_id.clone(),
            parent_task_instance_id: None,
            item_index: None,
        };

        let child_instance = self.instance_svc
            .create_instance(&workflow_instance.tenant_id, &workflow_entity, child_context, Some(parent_ctx), child_depth, workflow_instance.created_by.clone())
            .await
            .map_err(|e| {
                error!(
                    parent_workflow_id = %workflow_instance.workflow_instance_id,
                    error = %e,
                    "failed to create sub-workflow instance"
                );
                anyhow::anyhow!("Failed to create sub-workflow instance: {}", e)
            })?;

        info!(
            parent_workflow_id = %workflow_instance.workflow_instance_id,
            child_workflow_id = %child_instance.workflow_instance_id,
            depth = child_depth,
            "sub-workflow created"
        );

        node_instance.task_instance.input = Some(serde_json::json!({
            "workflow_meta_id": template.workflow_meta_id,
            "workflow_version": template.workflow_version,
            "child_context": child_context_snapshot,
        }));

        node_instance.task_instance.output = Some(serde_json::json!({
            "child_workflow_instance_id": child_instance.workflow_instance_id,
        }));

        let job = ExecuteWorkflowJob {
            workflow_instance_id: child_instance.workflow_instance_id,
            tenant_id: workflow_instance.tenant_id.clone(),
            event: WorkflowEvent::Start,
        };

        Ok(ExecutionResult::async_dispatch_workflow(job))
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::SubWorkflow
    }
}
