use async_trait::async_trait;
use serde_json::{Value as JsonValue, json};
use tracing::{debug, error};

use crate::plugin::interface::{
    ChildStatus, ContainerDecision, ContainerGatherResult, ContainerOutcome, ExecutionResult,
    PluginExecutor, PluginInterface, should_abort,
};
use crate::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob, WorkflowCallerContext};
use crate::task::entity::task_definition::{ParallelMode, ParallelTemplate, TaskTemplate};
use crate::workflow::entity::workflow_definition::{
    NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};

#[derive(Default)]
pub struct ParallelPlugin {}

impl ParallelPlugin {
    pub fn new() -> Self {
        Self::default()
    }

    fn resolve_items(
        template: &ParallelTemplate,
        node_instance: &WorkflowNodeInstanceEntity,
        workflow_instance: &WorkflowInstanceEntity,
    ) -> anyhow::Result<Vec<JsonValue>> {
        let pointer_path = if template.items_path.starts_with('/') {
            template.items_path.clone()
        } else {
            format!("/{}", template.items_path.replace('.', "/"))
        };

        let items_val = workflow_instance
            .context
            .pointer(&pointer_path)
            .or_else(|| node_instance.context.pointer(&pointer_path));

        match items_val {
            Some(JsonValue::Array(arr)) => Ok(arr.clone()),
            _ => {
                error!(
                    node_id = %node_instance.node_id,
                    items_path = %template.items_path,
                    "items path did not resolve to a JSON array"
                );
                Err(anyhow::anyhow!(
                    "Items path '{}' did not resolve to a JSON array",
                    template.items_path
                ))
            }
        }
    }

    fn diagnose(
        total_items: u64,
        max_failures: Option<u32>,
        gather: &ContainerGatherResult,
    ) -> ContainerDecision {
        if should_abort(max_failures, gather.failed_count) {
            return ContainerDecision::EarlyAbort;
        }

        let terminal = gather.terminal_count();

        if terminal == total_items {
            let outcome = if gather.failed_count > 0 {
                ContainerOutcome::Failed
            } else {
                ContainerOutcome::Success
            };
            return ContainerDecision::AllDone(outcome);
        }

        if gather.not_found_count == 0 && terminal + gather.running_count == total_items {
            return ContainerDecision::AllDispatched;
        }

        ContainerDecision::NeedDispatch
    }

    async fn gather_child_status(
        executor: &dyn PluginExecutor,
        inner_template: &TaskTemplate,
        total_items: usize,
        node_instance: &WorkflowNodeInstanceEntity,
        workflow_instance: &WorkflowInstanceEntity,
    ) -> anyhow::Result<ContainerGatherResult> {
        let mut result = ContainerGatherResult::default();

        for index in 0..total_items {
            let child_task_id = format!(
                "{}-{}-{}",
                workflow_instance.workflow_instance_id, node_instance.node_id, index
            );

            let child_status = executor
                .resolve_child_status(&child_task_id, inner_template)
                .await;

            result.record(child_task_id.clone(), child_status);
        }

        Ok(result)
    }

    async fn calc_dispatch_indices(
        executor: &dyn PluginExecutor,
        inner_template: &TaskTemplate,
        template: &ParallelTemplate,
        total_items: usize,
        workflow_instance: &WorkflowInstanceEntity,
        node_instance: &WorkflowNodeInstanceEntity,
        dispatched_count: u64,
        gather: &ContainerGatherResult,
    ) -> Vec<usize> {
        let concurrency = template.concurrency as usize;
        let slots_available = concurrency.saturating_sub(gather.running_count as usize);

        if slots_available == 0 {
            return Vec::new();
        }

        if template.mode == ParallelMode::Batch && gather.running_count > 0 {
            return Vec::new();
        }

        let mut indices = Vec::new();
        for index in dispatched_count as usize..total_items {
            if indices.len() >= slots_available {
                break;
            }
            let child_id = format!(
                "{}-{}-{}",
                workflow_instance.workflow_instance_id, node_instance.node_id, index
            );
            let child_status = executor
                .resolve_child_status(&child_id, inner_template)
                .await;
            if matches!(child_status, ChildStatus::NotFound) {
                indices.push(index);
            }
        }
        indices
    }

    fn build_dispatch_jobs(
        inner_template: &TaskTemplate,
        workflow_instance: &WorkflowInstanceEntity,
        node_instance: &WorkflowNodeInstanceEntity,
        indices: &[usize],
    ) -> (Vec<ExecuteTaskJob>, Vec<ExecuteWorkflowJob>) {
        let mut dispatch_jobs = Vec::new();
        let mut dispatch_workflow_jobs = Vec::new();

        for &index in indices {
            let child_id = format!(
                "{}-{}-{}",
                workflow_instance.workflow_instance_id, node_instance.node_id, index
            );
            let caller_context = WorkflowCallerContext {
                workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
                node_id: node_instance.node_id.clone(),
                parent_task_instance_id: Some(node_instance.task_instance.id.clone()),
                item_index: Some(index),
            };

            match inner_template {
                TaskTemplate::SubWorkflow(_) => {
                    dispatch_workflow_jobs.push(ExecuteWorkflowJob {
                        workflow_instance_id: child_id,
                        tenant_id: workflow_instance.tenant_id.clone(),
                        event: crate::shared::job::WorkflowEvent::Start,
                    });
                }
                _ => {
                    dispatch_jobs.push(ExecuteTaskJob {
                        task_instance_id: child_id,
                        tenant_id: workflow_instance.tenant_id.clone(),
                        caller_context: Some(caller_context),
                    });
                }
            }
        }

        (dispatch_jobs, dispatch_workflow_jobs)
    }

    fn write_output(
        node_instance: &mut WorkflowNodeInstanceEntity,
        total_items: u64,
        dispatched_count: u64,
        results_map: serde_json::Map<String, JsonValue>,
    ) {
        node_instance.task_instance.output = Some(json!({
            "total_items": total_items,
            "dispatched_count": dispatched_count,
            "results": results_map,
        }));
    }
}

/*
同构并发插件：execute 和 handle_callback 统一通过全表扫描获取子任务真实状态，
不再依赖增量计数器（success_count/failed_count/processed_callbacks）。
回调仅做"唤醒父工作流"，状态判断由扫表驱动。
*/
#[async_trait]
impl PluginInterface for ParallelPlugin {
    async fn execute(
        &self,
        executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::Parallel(t) => t.clone(),
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ParallelPlugin");
                return Err(anyhow::anyhow!("Invalid task template for ParallelPlugin"));
            }
        };

        let items = Self::resolve_items(&template, node_instance, workflow_instance)?;

        if items.is_empty() {
            debug!(node_id = %node_instance.node_id, "parallel: empty items, completing immediately");
            return Ok(ExecutionResult::success(None));
        }

        let total_items = items.len() as u64;
        let inner_template = &template.task_template;

        let gather = Self::gather_child_status(
            executor,
            inner_template,
            items.len(),
            node_instance,
            workflow_instance,
        )
        .await?;

        match Self::diagnose(total_items, template.max_failures, &gather) {
            ContainerDecision::AllDone(ContainerOutcome::Success) => {
                Self::write_output(node_instance, total_items, total_items, gather.results_map);
                Ok(ExecutionResult::success(None))
            }
            ContainerDecision::AllDone(ContainerOutcome::Failed)
            | ContainerDecision::EarlyAbort => {
                node_instance.error_message = Some(format!(
                    "Parallel aborted: {} failures out of {} items",
                    gather.failed_count, total_items
                ));
                Self::write_output(node_instance, total_items, total_items, gather.results_map);
                Ok(ExecutionResult::failed())
            }
            ContainerDecision::AllDispatched => {
                Self::write_output(node_instance, total_items, total_items, gather.results_map);
                Ok(ExecutionResult {
                    status: NodeExecutionStatus::Await,
                    dispatch_jobs: vec![],
                    dispatch_workflow_jobs: vec![],
                    jump_to_node: None,
                })
            }
            ContainerDecision::NeedDispatch => {
                let dispatched_count = node_instance
                    .task_instance
                    .output
                    .as_ref()
                    .and_then(|s| s.get("dispatched_count"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                let indices = Self::calc_dispatch_indices(
                    executor,
                    inner_template,
                    &template,
                    items.len(),
                    workflow_instance,
                    node_instance,
                    dispatched_count,
                    &gather,
                )
                .await;

                node_instance.task_instance.input = Some(json!({
                    "items_path": template.items_path,
                    "item_alias": template.item_alias,
                    "concurrency": template.concurrency,
                    "mode": format!("{:?}", template.mode),
                    "max_failures": template.max_failures,
                }));

                let (dispatch_jobs, dispatch_workflow_jobs) = Self::build_dispatch_jobs(
                    inner_template,
                    workflow_instance,
                    node_instance,
                    &indices,
                );

                let new_dispatched = std::cmp::max(
                    dispatched_count,
                    indices
                        .last()
                        .map(|&i| i as u64 + 1)
                        .unwrap_or(dispatched_count),
                );

                Self::write_output(
                    node_instance,
                    total_items,
                    new_dispatched,
                    gather.results_map,
                );

                Ok(ExecutionResult {
                    status: NodeExecutionStatus::Await,
                    dispatch_jobs,
                    dispatch_workflow_jobs,
                    jump_to_node: None,
                })
            }
        }
    }

    async fn handle_callback(
        &self,
        executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
        _child_task_id: &str,
        _status: &NodeExecutionStatus,
        _output: &Option<serde_json::Value>,
        _error_message: &Option<String>,
        _input: &Option<serde_json::Value>,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::Parallel(t) => t.clone(),
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ParallelPlugin callback");
                return Err(anyhow::anyhow!("Invalid task template for ParallelPlugin"));
            }
        };

        let items = Self::resolve_items(&template, node_instance, workflow_instance)?;

        if items.is_empty() {
            return Ok(ExecutionResult::success(None));
        }

        let total_items = items.len() as u64;
        let inner_template = &template.task_template;

        let gather = Self::gather_child_status(
            executor,
            inner_template,
            items.len(),
            node_instance,
            workflow_instance,
        )
        .await?;

        match Self::diagnose(total_items, template.max_failures, &gather) {
            ContainerDecision::AllDone(ContainerOutcome::Success) => {
                Self::write_output(node_instance, total_items, total_items, gather.results_map);
                Ok(ExecutionResult::success(None))
            }
            ContainerDecision::AllDone(ContainerOutcome::Failed)
            | ContainerDecision::EarlyAbort => {
                node_instance.error_message = Some(format!(
                    "Parallel aborted: {} failures out of {} items",
                    gather.failed_count, total_items
                ));
                Self::write_output(node_instance, total_items, total_items, gather.results_map);
                Ok(ExecutionResult::failed())
            }
            ContainerDecision::AllDispatched => {
                Self::write_output(node_instance, total_items, total_items, gather.results_map);
                Ok(ExecutionResult {
                    status: NodeExecutionStatus::Await,
                    dispatch_jobs: vec![],
                    dispatch_workflow_jobs: vec![],
                    jump_to_node: None,
                })
            }
            ContainerDecision::NeedDispatch => {
                let dispatched_count = node_instance
                    .task_instance
                    .output
                    .as_ref()
                    .and_then(|s| s.get("dispatched_count"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                let indices = Self::calc_dispatch_indices(
                    executor,
                    inner_template,
                    &template,
                    items.len(),
                    workflow_instance,
                    node_instance,
                    dispatched_count,
                    &gather,
                )
                .await;

                let (dispatch_jobs, dispatch_workflow_jobs) = Self::build_dispatch_jobs(
                    inner_template,
                    workflow_instance,
                    node_instance,
                    &indices,
                );

                let new_dispatched = std::cmp::max(
                    dispatched_count,
                    indices
                        .last()
                        .map(|&i| i as u64 + 1)
                        .unwrap_or(dispatched_count),
                );

                Self::write_output(
                    node_instance,
                    total_items,
                    new_dispatched,
                    gather.results_map,
                );

                Ok(ExecutionResult {
                    status: NodeExecutionStatus::Await,
                    dispatch_jobs,
                    dispatch_workflow_jobs,
                    jump_to_node: None,
                })
            }
        }
    }

    fn plugin_type(&self) -> crate::shared::workflow::TaskType {
        crate::shared::workflow::TaskType::Parallel
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::{ChildStatus, PluginExecutor, PluginInterface};
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{HttpMethod, TaskHttpTemplate};
    use chrono::Utc;
    use std::collections::HashMap;

    struct StubExecutor {
        child_statuses: HashMap<String, ChildStatus>,
    }

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
            child_task_instance_id: &str,
            _task_template: &TaskTemplate,
        ) -> ChildStatus {
            self.child_statuses
                .get(child_task_instance_id)
                .cloned()
                .unwrap_or(ChildStatus::NotFound)
        }
    }

    fn http_template() -> TaskHttpTemplate {
        TaskHttpTemplate {
            url: "/test".into(),
            method: HttpMethod::Get,
            headers: vec![],
            body: vec![],
            form: vec![],
            retry_count: 0,
            retry_delay: 0,
            timeout: 0,
            success_condition: None,
        }
    }

    fn parallel_template(
        concurrency: u32,
        mode: ParallelMode,
        max_failures: Option<u32>,
    ) -> TaskTemplate {
        TaskTemplate::Parallel(ParallelTemplate {
            items_path: "items".into(),
            item_alias: "item".into(),
            task_template: Box::new(TaskTemplate::Http(http_template())),
            concurrency,
            mode,
            max_failures,
        })
    }

    fn make_node(
        node_id: &str,
        _total: usize,
        template: TaskTemplate,
        output: Option<JsonValue>,
    ) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.into(),
            node_type: crate::shared::workflow::TaskType::Parallel,
            task_instance: crate::task::entity::task_definition::TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: "t1".into(),
                task_id: "".into(),
                task_name: "parallel".into(),
                task_type: crate::shared::workflow::TaskType::Parallel,
                task_template: template,
                task_status: TaskInstanceStatus::Running,
                task_instance_id: format!("ti-{}", node_id),
                created_at: now,
                updated_at: now,
                deleted_at: None,
                input: None,
                output,
                error_message: None,
                execution_duration: None,
                caller_context: None,
            },
            context: serde_json::json!({}),
            next_node: None,
            status: NodeExecutionStatus::Await,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_instance_with_items(items: Vec<JsonValue>) -> WorkflowInstanceEntity {
        let now = Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: "wf1".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status: WorkflowInstanceStatus::Await,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            context: serde_json::json!({ "items": items }),
            entry_node: "p".into(),
            current_node: "p".into(),
            nodes: vec![],
            parent_context: None,
            depth: 0,
            created_by: None,
            epoch: 0,
            locked_by: None,
            locked_at: None,
            locked_duration: None,
        }
    }

    fn make_instance() -> WorkflowInstanceEntity {
        make_instance_with_items(vec![json!(1), json!(2), json!(3)])
    }

    #[tokio::test]
    async fn all_not_found_dispatches_initial_batch() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            3,
            parallel_template(3, ParallelMode::Rolling, None),
            None,
        );
        let mut wf = make_instance();

        let exec = StubExecutor {
            child_statuses: HashMap::new(),
        };

        let result = plugin.execute(&exec, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert_eq!(result.dispatch_jobs.len(), 3);
        let output = node.task_instance.output.unwrap();
        assert_eq!(output["dispatched_count"], 3);
    }

    #[tokio::test]
    async fn all_completed_returns_success() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            3,
            parallel_template(3, ParallelMode::Rolling, None),
            None,
        );
        let mut wf = make_instance();

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Completed(Some(json!(1))));
        statuses.insert("wf1-p-1".into(), ChildStatus::Completed(Some(json!(2))));
        statuses.insert("wf1-p-2".into(), ChildStatus::Completed(Some(json!(3))));
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin.execute(&exec, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
    }

    #[tokio::test]
    async fn one_failure_with_max_failures_zero_returns_failed() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            3,
            parallel_template(3, ParallelMode::Rolling, Some(0)),
            None,
        );
        let mut wf = make_instance();

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Completed(None));
        statuses.insert(
            "wf1-p-1".into(),
            ChildStatus::Failed(None, Some("err".into())),
        );
        statuses.insert("wf1-p-2".into(), ChildStatus::Completed(None));
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin.execute(&exec, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn all_running_returns_await() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            3,
            parallel_template(3, ParallelMode::Rolling, None),
            None,
        );
        let mut wf = make_instance();

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Running);
        statuses.insert("wf1-p-1".into(), ChildStatus::Running);
        statuses.insert("wf1-p-2".into(), ChildStatus::Running);
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin.execute(&exec, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert!(result.dispatch_jobs.is_empty());
        assert!(result.dispatch_workflow_jobs.is_empty());
    }

    #[tokio::test]
    async fn skipped_counts_as_completed() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            2,
            parallel_template(2, ParallelMode::Rolling, None),
            None,
        );
        let mut wf = make_instance_with_items(vec![json!(1), json!(2)]);

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-p-1".into(), ChildStatus::Skipped(None));
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin.execute(&exec, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
    }

    #[tokio::test]
    async fn all_failed_no_max_failures_returns_failed() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            2,
            parallel_template(2, ParallelMode::Rolling, None),
            None,
        );
        let mut wf = make_instance_with_items(vec![json!(1), json!(2)]);

        let mut statuses = HashMap::new();
        statuses.insert(
            "wf1-p-0".into(),
            ChildStatus::Failed(None, Some("err".into())),
        );
        statuses.insert(
            "wf1-p-1".into(),
            ChildStatus::Failed(None, Some("err".into())),
        );
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin.execute(&exec, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn handle_callback_mixed_running_dispatches_more() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            3,
            parallel_template(2, ParallelMode::Rolling, None),
            Some(json!({
                "total_items": 3,
                "dispatched_count": 2,
                "results": {},
            })),
        );
        let mut wf = make_instance();

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-p-1".into(), ChildStatus::NotFound);
        statuses.insert("wf1-p-2".into(), ChildStatus::NotFound);
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-0",
                &NodeExecutionStatus::Success,
                &None,
                &None,
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert!(result.dispatch_jobs.len() >= 1 || result.dispatch_workflow_jobs.len() >= 1);
    }

    #[tokio::test]
    async fn handle_callback_all_terminal_returns_failed() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            2,
            parallel_template(2, ParallelMode::Rolling, None),
            None,
        );
        let mut wf = make_instance_with_items(vec![json!(1), json!(2)]);

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Completed(None));
        statuses.insert(
            "wf1-p-1".into(),
            ChildStatus::Failed(None, Some("err".into())),
        );
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-1",
                &NodeExecutionStatus::Failed,
                &None,
                &Some("err".into()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn batch_mode_waits_for_current_batch() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            5,
            parallel_template(2, ParallelMode::Batch, None),
            Some(json!({
                "total_items": 5,
                "dispatched_count": 2,
                "results": {},
            })),
        );
        let mut wf =
            make_instance_with_items(vec![json!(1), json!(2), json!(3), json!(4), json!(5)]);

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Running);
        statuses.insert("wf1-p-1".into(), ChildStatus::Running);
        statuses.insert("wf1-p-2".into(), ChildStatus::NotFound);
        statuses.insert("wf1-p-3".into(), ChildStatus::NotFound);
        statuses.insert("wf1-p-4".into(), ChildStatus::NotFound);
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-0",
                &NodeExecutionStatus::Success,
                &None,
                &None,
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert!(result.dispatch_jobs.is_empty());
        assert!(result.dispatch_workflow_jobs.is_empty());
    }

    #[tokio::test]
    async fn batch_mode_dispatches_next_batch_when_current_completes() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            5,
            parallel_template(2, ParallelMode::Batch, None),
            Some(json!({
                "total_items": 5,
                "dispatched_count": 2,
                "results": {},
            })),
        );
        let mut wf =
            make_instance_with_items(vec![json!(1), json!(2), json!(3), json!(4), json!(5)]);

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-p-1".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-p-2".into(), ChildStatus::NotFound);
        statuses.insert("wf1-p-3".into(), ChildStatus::NotFound);
        statuses.insert("wf1-p-4".into(), ChildStatus::NotFound);
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-1",
                &NodeExecutionStatus::Success,
                &None,
                &None,
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert!(result.dispatch_jobs.len() >= 2 || result.dispatch_workflow_jobs.len() >= 2);
    }

    #[tokio::test]
    async fn empty_items_completes_immediately() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            3,
            parallel_template(3, ParallelMode::Rolling, None),
            None,
        );
        let mut wf = make_instance();
        wf.context = serde_json::json!({"items": []});

        let exec = StubExecutor {
            child_statuses: HashMap::new(),
        };

        let result = plugin.execute(&exec, &mut node, &mut wf).await.unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
    }

    #[tokio::test]
    async fn failures_exceeding_max_triggers_early_abort() {
        let plugin = ParallelPlugin::new();
        let mut node = make_node(
            "p",
            5,
            parallel_template(5, ParallelMode::Rolling, Some(1)),
            Some(json!({
                "total_items": 5,
                "dispatched_count": 5,
                "results": {},
            })),
        );
        let mut wf =
            make_instance_with_items(vec![json!(1), json!(2), json!(3), json!(4), json!(5)]);

        let mut statuses = HashMap::new();
        statuses.insert("wf1-p-0".into(), ChildStatus::Completed(None));
        statuses.insert(
            "wf1-p-1".into(),
            ChildStatus::Failed(None, Some("err".into())),
        );
        statuses.insert(
            "wf1-p-2".into(),
            ChildStatus::Failed(None, Some("err".into())),
        );
        statuses.insert("wf1-p-3".into(), ChildStatus::Running);
        statuses.insert("wf1-p-4".into(), ChildStatus::Running);
        let exec = StubExecutor {
            child_statuses: statuses,
        };

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-2",
                &NodeExecutionStatus::Failed,
                &None,
                &Some("err".into()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }
}
