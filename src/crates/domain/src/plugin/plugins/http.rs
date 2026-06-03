use async_trait::async_trait;

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::job::{ExecuteTaskJob, WorkflowCallerContext};
use crate::shared::workflow::TaskType;
use crate::workflow::entity::workflow_definition::{
    WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};

#[derive(Default)]
pub struct HttpPlugin {}

impl HttpPlugin {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PluginInterface for HttpPlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        // 构造异步任务
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

        // 返回 AsyncDispatch，让 Manager 挂起工作流并投递任务
        Ok(ExecutionResult::async_dispatch(job))
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::Http
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
            _ni: &mut WorkflowNodeInstanceEntity,
            _wi: &mut WorkflowInstanceEntity,
        ) -> anyhow::Result<ExecutionResult> {
            unreachable!()
        }
        async fn handle_node_callback(
            &self,
            _ni: &mut WorkflowNodeInstanceEntity,
            _wi: &mut WorkflowInstanceEntity,
            _cid: &str,
            _st: &NodeExecutionStatus,
            _out: &Option<serde_json::Value>,
            _err: &Option<String>,
            _inp: &Option<serde_json::Value>,
        ) -> anyhow::Result<ExecutionResult> {
            unreachable!()
        }
        async fn resolve_child_status(
            &self,
            _child_task_instance_id: &str,
            _task_template: &TaskTemplate,
        ) -> crate::plugin::interface::ChildStatus {
            unreachable!()
        }
    }

    fn make_node(
        plugin: &HttpPlugin,
        wf: &WorkflowInstanceEntity,
        node_id: &str,
    ) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.to_string(),
            node_type: TaskType::Http,
            task_instance: crate::task::entity::task_definition::TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: wf.tenant_id.clone(),
                task_id: "".to_string(),
                task_name: "http".to_string(),
                task_type: plugin.plugin_type(),
                task_template: TaskTemplate::Http(
                    crate::task::entity::task_definition::TaskHttpTemplate {
                        url: "/test".into(),
                        method: crate::task::entity::task_definition::HttpMethod::Get,
                        headers: vec![],
                        body: vec![],
                        form: vec![],
                        retry_count: 0,
                        retry_delay: 0,
                        timeout: 30,
                        success_condition: None,
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
            entry_node: "h1".into(),
            current_node: "h1".into(),
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
        let plugin = HttpPlugin::new();
        let mut wf = make_instance();
        let mut node = make_node(&plugin, &wf, "h1");

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert_eq!(result.dispatch_jobs.len(), 1);
        let job = &result.dispatch_jobs[0];
        assert_eq!(job.tenant_id, "t1");
        assert_eq!(job.task_instance_id, "wf1-h1");
        assert!(job.caller_context.is_some());
        let ctx = job.caller_context.as_ref().unwrap();
        assert_eq!(ctx.workflow_instance_id, "wf1");
        assert_eq!(ctx.node_id, "h1");
    }

    #[tokio::test]
    async fn handle_callback_success_copies_output() {
        let plugin = HttpPlugin::new();
        let mut node = make_node(&plugin, &make_instance(), "h1");
        let mut wf = make_instance();

        let input = serde_json::json!({"url": "http://example.com"});
        let output = serde_json::json!({"status": 200, "body": "ok"});

        let result = plugin
            .handle_callback(
                &StubExecutor,
                &mut node,
                &mut wf,
                "wf1-h1",
                &NodeExecutionStatus::Success,
                &Some(output.clone()),
                &None,
                &Some(input.clone()),
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        assert_eq!(node.task_instance.input, Some(input));
        assert_eq!(node.task_instance.output, Some(output));
    }

    #[tokio::test]
    async fn handle_callback_failed_sets_error() {
        let plugin = HttpPlugin::new();
        let mut node = make_node(&plugin, &make_instance(), "h1");
        let mut wf = make_instance();

        let err = "connection timeout".to_string();
        let result = plugin
            .handle_callback(
                &StubExecutor,
                &mut node,
                &mut wf,
                "wf1-h1",
                &NodeExecutionStatus::Failed,
                &None,
                &Some(err.clone()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
        assert_eq!(node.error_message, Some(err.clone()));
        assert_eq!(node.task_instance.error_message, Some(err));
    }

    #[test]
    fn plugin_type_is_http() {
        assert_eq!(HttpPlugin::new().plugin_type(), TaskType::Http);
    }
}
