use async_trait::async_trait;
use rhai::Scope;
use tracing::{debug, error};

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::plugin::rhai_engine;
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::TaskTemplate;
use crate::workflow::entity::workflow_definition::{WorkflowInstanceEntity, WorkflowNodeInstanceEntity};

pub struct IfConditionPlugin {}

impl IfConditionPlugin {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl PluginInterface for IfConditionPlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::IfCondition(t) => t,
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for IfConditionPlugin");
                return Err(anyhow::anyhow!("Invalid task template for IfConditionPlugin"));
            }
        };

        let engine = rhai_engine::create_engine();
        let mut scope = Scope::new();

        // `node_instance.context` is resolve_variables + `nodes` (see `run_node`); single source of truth.
        rhai_engine::inject_context_flat(&mut scope, &node_instance.context);

        let result: bool = engine.eval_with_scope(&mut scope, &template.condition)
            .map_err(|e| {
                error!(
                    workflow_instance_id = %workflow_instance.workflow_instance_id,
                    node_id = %node_instance.node_id,
                    condition = %template.condition,
                    error = %e,
                    "failed to evaluate IfCondition"
                );
                anyhow::anyhow!("Failed to evaluate IfCondition: {}", e)
            })?;

        let next_node = if result {
            template.then_task.clone()
        } else {
            template.else_task.clone()
        };
        let out_data = serde_json::json!({
            "if_condition_result": result,
            "next_node": next_node,
            "then_task": template.then_task.clone(),
            "else_task": template.else_task.clone(),
            "condition": template.condition.clone(),
        });
        node_instance.task_instance.input = Some(serde_json::json!({
            "condition": template.condition.clone(),
            "name": template.name.clone(),
        }));
        node_instance.task_instance.output = Some(out_data);

        debug!(
            node_id = %node_instance.node_id,
            condition = %template.condition,
            result = result,
            next_node = ?next_node,
            "IfCondition evaluated"
        );

        Ok(ExecutionResult::success(next_node))
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::IfCondition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::PluginInterface;
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{IfConditionTemplate, TaskInstanceEntity, TaskTemplate};
    use crate::workflow::entity::workflow_definition::{NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity};
    use chrono::Utc;

    struct StubExecutor;

    #[async_trait::async_trait]
    impl PluginExecutor for StubExecutor {
        async fn execute_node_instance(&self, _: &mut WorkflowNodeInstanceEntity, _: &mut WorkflowInstanceEntity) -> anyhow::Result<ExecutionResult> { unreachable!() }
        async fn handle_node_callback(&self, _: &mut WorkflowNodeInstanceEntity, _: &mut WorkflowInstanceEntity, _: &str, _: &NodeExecutionStatus, _: &Option<serde_json::Value>, _: &Option<String>, _: &Option<serde_json::Value>) -> anyhow::Result<ExecutionResult> { unreachable!() }
        async fn resolve_child_status(&self, _: &str, _: &TaskTemplate) -> crate::plugin::interface::ChildStatus { unreachable!() }
    }

    fn make_node(plugin: &IfConditionPlugin, wf: &WorkflowInstanceEntity, node_id: &str, condition: &str, then_task: Option<String>, else_task: Option<String>, node_ctx: serde_json::Value) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.to_string(),
            node_type: TaskType::IfCondition,
            task_instance: TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: wf.tenant_id.clone(),
                task_id: "".to_string(), task_name: "ifcond".to_string(), task_type: plugin.plugin_type(),
                task_template: TaskTemplate::IfCondition(IfConditionTemplate {
                    name: "check".into(), condition: condition.into(),
                    then_task, else_task,
                }),
                task_status: TaskInstanceStatus::Pending,
                task_instance_id: format!("{}-{}", wf.workflow_instance_id, node_id),
                created_at: now, updated_at: now, deleted_at: None,
                input: None, output: None, error_message: None, execution_duration: None,
                caller_context: None,
            },
            context: node_ctx, next_node: None,
            status: NodeExecutionStatus::Pending,
            error_message: None, created_at: now, updated_at: now,
        }
    }

    fn make_instance() -> WorkflowInstanceEntity {
        let now = Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: "wf1".into(), tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(), workflow_version: 1,
            status: WorkflowInstanceStatus::Running,
            created_at: now, updated_at: now, deleted_at: None,
            context: serde_json::json!({"score": 80}), entry_node: "c1".into(), current_node: "c1".into(),
            nodes: vec![], epoch: 0,
            locked_by: None, locked_duration: None, locked_at: None,
            parent_context: None, depth: 0, created_by: None,
        }
    }

    #[tokio::test]
    async fn true_condition_jumps_to_then_task() {
        let plugin = IfConditionPlugin::new();
        let mut wf = make_instance();
        let mut node = make_node(&plugin, &wf, "c1", "score > 75", Some("approved".into()), Some("rejected".into()), serde_json::json!({"score": 80}));

        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        assert_eq!(result.jump_to_node, Some("approved".to_string()));
        let output = node.task_instance.output.as_ref().unwrap();
        assert_eq!(output["if_condition_result"], true);
        assert_eq!(output["next_node"], "approved");
    }

    #[tokio::test]
    async fn false_condition_jumps_to_else_task() {
        let plugin = IfConditionPlugin::new();
        let mut wf = make_instance();
        let mut node = make_node(&plugin, &wf, "c2", "score > 90", Some("approved".into()), Some("rejected".into()), serde_json::json!({"score": 80}));

        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        assert_eq!(result.jump_to_node, Some("rejected".to_string()));
        assert_eq!(node.task_instance.output.as_ref().unwrap()["if_condition_result"], false);
    }

    #[tokio::test]
    async fn missing_variable_evaluates_to_false() {
        let plugin = IfConditionPlugin::new();
        let mut wf = make_instance();
        let mut node = make_node(&plugin, &wf, "c3", "nonexistent > 0", Some("t".into()), Some("e".into()), serde_json::json!({"score": 80}));

        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await;

        assert!(result.is_err(), "expected error for missing variable");
    }

    #[tokio::test]
    async fn invalid_template_returns_error() {
        let plugin = IfConditionPlugin::new();
        let mut wf = make_instance();
        let mut node = WorkflowNodeInstanceEntity {
            node_id: "bad".into(),
            node_type: TaskType::IfCondition,
            task_instance: TaskInstanceEntity {
                id: "ti-bad".into(), tenant_id: "t1".into(),
                task_id: "".into(), task_name: "bad".into(), task_type: TaskType::IfCondition,
                task_template: TaskTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                    url: "/x".into(), method: crate::task::entity::task_definition::HttpMethod::Get,
                    headers: vec![], body: vec![], form: vec![],
                    retry_count: 0, retry_delay: 0, timeout: 30, success_condition: None,
                }),
                task_status: TaskInstanceStatus::Pending,
                task_instance_id: "bad".into(),
                created_at: Utc::now(), updated_at: Utc::now(), deleted_at: None,
                input: None, output: None, error_message: None, execution_duration: None,
                caller_context: None,
            },
            context: serde_json::json!({}), next_node: None,
            status: NodeExecutionStatus::Pending,
            error_message: None, created_at: Utc::now(), updated_at: Utc::now(),
        };

        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await;
        assert!(result.is_err());
    }

    #[test]
    fn plugin_type_is_ifcondition() {
        assert_eq!(IfConditionPlugin::new().plugin_type(), TaskType::IfCondition);
    }
}
