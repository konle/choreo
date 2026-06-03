use async_trait::async_trait;
use chrono::Utc;
use tracing::info;

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::TaskTemplate;
use crate::workflow::entity::workflow_definition::{
    WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};

#[derive(Default)]
pub struct PausePlugin;

impl PausePlugin {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PluginInterface for PausePlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::Pause(t) => t.clone(),
            other => {
                return Err(anyhow::anyhow!(
                    "Invalid template for PausePlugin: {:?}",
                    other
                ));
            }
        };

        let resume_at = Utc::now() + chrono::Duration::seconds(template.wait_seconds as i64);

        node_instance.task_instance.output = Some(serde_json::json!({
            "mode": format!("{:?}", template.mode),
            "wait_seconds": template.wait_seconds,
            "resume_at": resume_at.to_rfc3339(),
        }));

        info!(
            workflow_instance_id = %workflow_instance.workflow_instance_id,
            node_id = %node_instance.node_id,
            mode = ?template.mode,
            wait_seconds = template.wait_seconds,
            resume_at = %resume_at.to_rfc3339(),
            "pause node suspended"
        );

        Ok(ExecutionResult::suspended())
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::Pause
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::PluginExecutor;
    use crate::plugin::interface::PluginInterface;
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{PauseMode, PauseTemplate};
    use crate::workflow::entity::workflow_definition::NodeExecutionStatus;
    use chrono::{DateTime, Utc};

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

    fn make_node(template: TaskTemplate, node_id: &str) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.to_string(),
            node_type: TaskType::Pause,
            task_instance: crate::task::entity::task_definition::TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: "t1".into(),
                task_id: "".into(),
                task_name: "pause".into(),
                task_type: TaskType::Pause,
                task_template: template,
                task_status: TaskInstanceStatus::Pending,
                task_instance_id: format!("wf1-{}", node_id),
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
            entry_node: "p1".into(),
            current_node: "p1".into(),
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
    async fn execute_auto_mode_sets_suspended_with_resume_at() {
        let plugin = PausePlugin::new();
        let template = TaskTemplate::Pause(PauseTemplate {
            wait_seconds: 60,
            mode: PauseMode::Auto,
        });
        let mut node = make_node(template, "p1");
        let mut wf = make_instance();

        let before = Utc::now();
        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();
        let after = Utc::now();

        assert_eq!(result.status, NodeExecutionStatus::Suspended);
        let output = node.task_instance.output.unwrap();
        assert_eq!(output["mode"], "Auto");
        assert_eq!(output["wait_seconds"], 60);
        let resume_at_str = output["resume_at"].as_str().unwrap();
        let resume_at: DateTime<Utc> = resume_at_str.parse().unwrap();
        assert!(resume_at >= before + chrono::Duration::seconds(59));
        assert!(resume_at <= after + chrono::Duration::seconds(61));
    }

    #[tokio::test]
    async fn execute_manual_mode_sets_suspended() {
        let plugin = PausePlugin::new();
        let template = TaskTemplate::Pause(PauseTemplate {
            wait_seconds: 120,
            mode: PauseMode::Manual,
        });
        let mut node = make_node(template, "p2");
        let mut wf = make_instance();

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Suspended);
        let output = node.task_instance.output.unwrap();
        assert_eq!(output["mode"], "Manual");
        assert_eq!(output["wait_seconds"], 120);
    }

    #[tokio::test]
    async fn invalid_template_returns_error() {
        let plugin = PausePlugin::new();
        let mut node = make_node(
            TaskTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                url: "/x".into(),
                method: crate::task::entity::task_definition::HttpMethod::Get,
                headers: vec![],
                body: vec![],
                form: vec![],
                retry_count: 0,
                retry_delay: 0,
                timeout: 30,
                success_condition: None,
            }),
            "p_bad",
        );
        let mut wf = make_instance();

        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await;
        assert!(result.is_err());
    }

    #[test]
    fn plugin_type_is_pause() {
        assert_eq!(PausePlugin::new().plugin_type(), TaskType::Pause);
    }
}
