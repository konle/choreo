use async_trait::async_trait;

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::job::{ExecuteTaskJob, WorkflowCallerContext};
use crate::shared::workflow::TaskType;
use crate::workflow::entity::workflow_definition::{
    WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};

pub struct LlmPlugin {}

impl LlmPlugin {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl PluginInterface for LlmPlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let job = ExecuteTaskJob {
            task_instance_id: format!(
                "{}-{}",
                workflow_instance.workflow_instance_id, node_instance.node_id
            ),
            tenant_id: workflow_instance.tenant_id.clone(),
            caller_context: Some(WorkflowCallerContext {
                workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
                node_id: node_instance.node_id.clone(),
                parent_task_instance_id: None,
                item_index: None,
            }),
        };

        Ok(ExecutionResult::async_dispatch(job))
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::Llm
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::PluginExecutor;
    use crate::plugin::interface::PluginInterface;
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::TaskTemplate;
    use crate::workflow::entity::workflow_definition::NodeExecutionStatus;
    use chrono::Utc;

    struct StubExecutor;

    #[async_trait::async_trait]
    impl PluginExecutor for StubExecutor {
        async fn execute_node_instance(
            &self,
            _: &mut WorkflowNodeInstanceEntity,
            _: &mut WorkflowInstanceEntity,
        ) -> anyhow::Result<ExecutionResult> {
            unreachable!()
        }
        async fn handle_node_callback(
            &self,
            _: &mut WorkflowNodeInstanceEntity,
            _: &mut WorkflowInstanceEntity,
            _: &str,
            _: &NodeExecutionStatus,
            _: &Option<serde_json::Value>,
            _: &Option<String>,
            _: &Option<serde_json::Value>,
        ) -> anyhow::Result<ExecutionResult> {
            unreachable!()
        }
        async fn resolve_child_status(
            &self,
            _: &str,
            _: &TaskTemplate,
        ) -> crate::plugin::interface::ChildStatus {
            unreachable!()
        }
    }

    fn make_node(
        plugin: &LlmPlugin,
        wf: &WorkflowInstanceEntity,
        node_id: &str,
    ) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.to_string(),
            node_type: TaskType::Llm,
            task_instance: crate::task::entity::task_definition::TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: wf.tenant_id.clone(),
                task_id: "".to_string(),
                task_name: "llm".to_string(),
                task_type: plugin.plugin_type(),
                task_template: TaskTemplate::Llm(
                    crate::task::entity::task_definition::LlmTemplate {
                        base_url: "https://api.openai.com/v1".into(),
                        model: "gpt-4o".into(),
                        api_key_ref: "OPENAI_KEY".into(),
                        system_prompt: Some("classify".into()),
                        user_prompt: "text: {{input}}".into(),
                        temperature: None,
                        max_tokens: None,
                        timeout: 30,
                        retry_count: 0,
                        retry_delay: 0,
                        response_format: None,
                        form: vec![],
                    },
                ),
                task_status: TaskInstanceStatus::Pending,
                task_instance_id: format!("{}-{}", wf.workflow_instance_id, node_id),
                created_at: now,
                updated_at: now,
                deleted_at: None,
                input: None,
                output: None,
                error_message: None,
                execution_duration: None,
                caller_context: None,
            },
            context: serde_json::json!({}),
            next_node: None,
            status: NodeExecutionStatus::Pending,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_instance() -> WorkflowInstanceEntity {
        let now = Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: "wf1".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status: WorkflowInstanceStatus::Running,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "llm1".into(),
            current_node: "llm1".into(),
            nodes: vec![],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 0,
            created_by: None,
        }
    }

    #[tokio::test]
    async fn execute_dispatches_async_job() {
        let plugin = LlmPlugin::new();
        let mut wf = make_instance();
        let mut node = make_node(&plugin, &wf, "llm1");

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert_eq!(result.dispatch_jobs.len(), 1);
        assert_eq!(result.dispatch_jobs[0].task_instance_id, "wf1-llm1");
    }

    #[tokio::test]
    async fn handle_callback_success_copies_output() {
        let plugin = LlmPlugin::new();
        let mut node = make_node(&plugin, &make_instance(), "llm1");
        let mut wf = make_instance();
        let output = serde_json::json!({"content": "classified", "usage": {"total_tokens": 50}});

        let result = plugin
            .handle_callback(
                &StubExecutor,
                &mut node,
                &mut wf,
                "wf1-llm1",
                &NodeExecutionStatus::Success,
                &Some(output.clone()),
                &None,
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        assert_eq!(node.task_instance.output, Some(output));
    }

    #[tokio::test]
    async fn handle_callback_failed_sets_error() {
        let plugin = LlmPlugin::new();
        let mut node = make_node(&plugin, &make_instance(), "llm1");
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &StubExecutor,
                &mut node,
                &mut wf,
                "wf1-llm1",
                &NodeExecutionStatus::Failed,
                &None,
                &Some("rate limit".into()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
        assert_eq!(node.error_message, Some("rate limit".to_string()));
    }

    #[test]
    fn plugin_type_is_llm() {
        assert_eq!(LlmPlugin::new().plugin_type(), TaskType::Llm);
    }
}
