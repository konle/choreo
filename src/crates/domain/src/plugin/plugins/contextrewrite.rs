use async_trait::async_trait;
use rhai::Scope;
use tracing::{debug, error};

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::plugin::rhai_engine;
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::{MergeMode, TaskTemplate};
use crate::workflow::entity::workflow_definition::{
    WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};

pub struct ContextRewritePlugin {}

impl ContextRewritePlugin {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl PluginInterface for ContextRewritePlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::ContextRewrite(t) => t,
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ContextRewritePlugin");
                return Err(anyhow::anyhow!(
                    "Invalid task template for ContextRewritePlugin"
                ));
            }
        };

        let engine = rhai_engine::create_engine();
        let ast = rhai_engine::compile_script(&engine, &template.script).map_err(|e| {
            error!(
                workflow_instance_id = %workflow_instance.workflow_instance_id,
                node_id = %node_instance.node_id,
                error = %e,
                "failed to compile ContextRewrite script"
            );
            e
        })?;

        let mut scope = Scope::new();
        rhai_engine::inject_context(&mut scope, &node_instance.context);

        let result = engine
            .eval_ast_with_scope::<rhai::Dynamic>(&mut scope, &ast)
            .map_err(|e| {
                error!(
                    workflow_instance_id = %workflow_instance.workflow_instance_id,
                    node_id = %node_instance.node_id,
                    error = %e,
                    "ContextRewrite script execution error"
                );
                anyhow::anyhow!("ContextRewrite script error: {}", e)
            })?;

        let result_map = rhai_engine::rhai_map_to_json(result)?;

        match template.merge_mode {
            MergeMode::Merge => {
                if let Some(ctx_obj) = workflow_instance.context.as_object_mut() {
                    for (k, v) in result_map {
                        ctx_obj.insert(k, v);
                    }
                } else {
                    workflow_instance.context = serde_json::Value::Object(result_map);
                }
            }
            MergeMode::Replace => {
                workflow_instance.context = serde_json::Value::Object(result_map);
            }
        }

        debug!(
            node_id = %node_instance.node_id,
            merge_mode = ?template.merge_mode,
            "ContextRewrite applied"
        );

        node_instance.task_instance.input = Some(serde_json::json!({
            "name": template.name.clone(),
            "script": template.script.clone(),
            "merge_mode": format!("{:?}", template.merge_mode),
        }));

        node_instance.task_instance.output = Some(serde_json::json!({
            "rewritten_keys": workflow_instance.context.as_object()
                .map(|o| o.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default(),
        }));

        Ok(ExecutionResult::success(None))
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::ContextRewrite
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::PluginInterface;
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{
        ContextRewriteTemplate, TaskInstanceEntity, TaskTemplate,
    };
    use crate::workflow::entity::workflow_definition::{
        NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
    };
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
        plugin: &ContextRewritePlugin,
        wf: &WorkflowInstanceEntity,
        node_id: &str,
        script: &str,
        merge_mode: MergeMode,
    ) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.to_string(),
            node_type: TaskType::ContextRewrite,
            task_instance: TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: wf.tenant_id.clone(),
                task_id: "".into(),
                task_name: "rewrite".to_string(),
                task_type: plugin.plugin_type(),
                task_template: TaskTemplate::ContextRewrite(ContextRewriteTemplate {
                    name: "rewrite".into(),
                    script: script.into(),
                    merge_mode,
                }),
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

    fn make_instance(initial_ctx: serde_json::Value) -> WorkflowInstanceEntity {
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
            context: initial_ctx,
            entry_node: "r1".into(),
            current_node: "r1".into(),
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
    async fn merge_mode_merges_new_keys() {
        let plugin = ContextRewritePlugin::new();
        let mut wf = make_instance(serde_json::json!({"existing_key": "old_value"}));
        let mut node = make_node(
            &plugin,
            &wf,
            "r1",
            "#{new_key: \"new_value\"}",
            MergeMode::Merge,
        );

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        assert_eq!(wf.context["existing_key"], "old_value");
        assert_eq!(wf.context["new_key"], "new_value");
    }

    #[tokio::test]
    async fn merge_mode_existing_keys_overwritten() {
        let plugin = ContextRewritePlugin::new();
        let mut wf = make_instance(serde_json::json!({"key": "old_value"}));
        let mut node = make_node(
            &plugin,
            &wf,
            "r2",
            "#{key: \"new_value\"}",
            MergeMode::Merge,
        );

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        assert_eq!(wf.context["key"], "new_value");
    }

    #[tokio::test]
    async fn replace_mode_replaces_all_keys() {
        let plugin = ContextRewritePlugin::new();
        let mut wf = make_instance(serde_json::json!({"existing_key": "old_value"}));
        let mut node = make_node(&plugin, &wf, "r3", "#{only_new: 42}", MergeMode::Replace);

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        assert_eq!(wf.context.get("existing_key"), None);
        assert_eq!(wf.context["only_new"], 42);
    }

    #[tokio::test]
    async fn empty_script_returns_empty_map() {
        let plugin = ContextRewritePlugin::new();
        let mut wf = make_instance(serde_json::json!({"existing": "value"}));
        let script = "#{}";
        let mut node = make_node(&plugin, &wf, "r5", script, MergeMode::Merge);

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        assert_eq!(wf.context["existing"], "value");
    }

    #[tokio::test]
    async fn invalid_template_returns_error() {
        let plugin = ContextRewritePlugin::new();
        let mut wf = make_instance(serde_json::json!({}));
        let mut node = WorkflowNodeInstanceEntity {
            node_id: "bad".into(),
            node_type: TaskType::ContextRewrite,
            task_instance: TaskInstanceEntity {
                id: "ti-bad".into(),
                tenant_id: "t1".into(),
                task_id: "".into(),
                task_name: "bad".into(),
                task_type: TaskType::ContextRewrite,
                task_template: TaskTemplate::Http(
                    crate::task::entity::task_definition::TaskHttpTemplate {
                        url: "/x".into(),
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
                task_instance_id: "bad".into(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
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
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn non_map_script_returns_error() {
        let plugin = ContextRewritePlugin::new();
        let mut wf = make_instance(serde_json::json!({}));
        let mut node = make_node(&plugin, &wf, "r5", "42", MergeMode::Merge);

        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await;
        assert!(result.is_err());
    }

    #[test]
    fn plugin_type_is_context_rewrite() {
        assert_eq!(
            ContextRewritePlugin::new().plugin_type(),
            TaskType::ContextRewrite
        );
    }

    #[tokio::test]
    async fn merge_mode_with_non_object_context_replaces() {
        let plugin = ContextRewritePlugin::new();
        let mut wf = make_instance(serde_json::json!("not_an_object"));
        let mut node = make_node(
            &plugin,
            &wf,
            "r6",
            r#"#{"key": "from_script"}"#,
            MergeMode::Merge,
        );
        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await;
        assert!(result.is_ok());
        let obj = wf.context.as_object().unwrap();
        assert_eq!(obj["key"], "from_script");
    }

    #[tokio::test]
    async fn compile_error_returns_err() {
        let plugin = ContextRewritePlugin::new();
        let mut wf = make_instance(serde_json::json!({}));
        let mut node = make_node(&plugin, &wf, "r7", "{{broken syntax", MergeMode::Merge);
        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await;
        assert!(result.is_err());
    }
}
