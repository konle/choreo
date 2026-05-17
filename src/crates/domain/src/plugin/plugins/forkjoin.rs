use async_trait::async_trait;
use serde_json::{Value as JsonValue, json};
use tracing::{debug, error};

use crate::plugin::interface::{ChildStatus, ContainerGatherResult, ExecutionResult, PluginExecutor, PluginInterface, should_abort};
use crate::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob, WorkflowCallerContext};
use crate::task::entity::task_definition::{ParallelMode, TaskTemplate};
use crate::workflow::entity::workflow_definition::{
    NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};

pub struct ForkJoinPlugin {}

enum ForkJoinDecision {
    AllDone(ForkJoinOutcome),
    AllDispatched,
    NeedDispatch,
    EarlyAbort,
}

enum ForkJoinOutcome {
    Success,
    Failed,
}

impl ForkJoinPlugin {
    pub fn new() -> Self {
        Self {}
    }

    fn diagnose(
        total_tasks: u64,
        max_failures: Option<u32>,
        gather: &ContainerGatherResult,
    ) -> ForkJoinDecision {
        if should_abort(max_failures, gather.failed_count) {
            return ForkJoinDecision::EarlyAbort;
        }

        let terminal = gather.terminal_count();

        if terminal == total_tasks {
            let outcome = if gather.failed_count > 0 {
                ForkJoinOutcome::Failed
            } else {
                ForkJoinOutcome::Success
            };
            return ForkJoinDecision::AllDone(outcome);
        }

        if gather.not_found_count == 0 && terminal + gather.running_count == total_tasks {
            return ForkJoinDecision::AllDispatched;
        }

        ForkJoinDecision::NeedDispatch
    }

    async fn gather_child_task_status(
        executor: &dyn PluginExecutor,
        template: &crate::task::entity::task_definition::ForkJoinTemplate,
        node_instance: &WorkflowNodeInstanceEntity,
        workflow_instance: &WorkflowInstanceEntity,
    ) -> anyhow::Result<ContainerGatherResult> {
        let mut result = ContainerGatherResult::new();

        for (index, item) in template.tasks.iter().enumerate() {
            let child_task_id = format!(
                "{}-{}-{}",
                workflow_instance.workflow_instance_id,
                node_instance.node_id,
                index as u64
            );

            let child_task_status = executor
                .resolve_child_status(&child_task_id, &item.task_template)
                .await;

            result.record(item.task_key.clone(), child_task_status);
        }

        Ok(result)
    }

    async fn calc_dispatch_indices(
        executor: &dyn PluginExecutor,
        template: &crate::task::entity::task_definition::ForkJoinTemplate,
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
        for index in dispatched_count as usize..template.tasks.len() {
            if indices.len() >= slots_available {
                break;
            }
            let child_id = format!(
                "{}-{}-{}",
                workflow_instance.workflow_instance_id,
                node_instance.node_id,
                index
            );
            let child_status = executor
                .resolve_child_status(&child_id, &template.tasks[index].task_template)
                .await;
            if matches!(child_status, ChildStatus::NotFound) {
                indices.push(index);
            }
        }
        indices
    }

    fn build_dispatch_jobs(
        template: &crate::task::entity::task_definition::ForkJoinTemplate,
        workflow_instance: &WorkflowInstanceEntity,
        node_instance: &WorkflowNodeInstanceEntity,
        indices: &[usize],
    ) -> (Vec<ExecuteTaskJob>, Vec<ExecuteWorkflowJob>) {
        let mut dispatch_jobs = Vec::new();
        let mut dispatch_workflow_jobs = Vec::new();

        for &index in indices {
            let child_id = format!(
                "{}-{}-{}",
                workflow_instance.workflow_instance_id,
                node_instance.node_id,
                index
            );
            let caller_context = WorkflowCallerContext {
                workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
                node_id: node_instance.node_id.clone(),
                parent_task_instance_id: Some(node_instance.task_instance.id.clone()),
                item_index: Some(index),
            };

            match &template.tasks[index].task_template {
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
        total_tasks: u64,
        dispatched_count: u64,
        results_map: serde_json::Map<String, JsonValue>,
    ) {
        node_instance.task_instance.output = Some(json!({
            "total_tasks": total_tasks,
            "dispatched_count": dispatched_count,
            "results": results_map,
        }));
    }
}

/*
异构并发插件：execute 和 handle_callback 统一通过全表扫描获取子任务真实状态，
不再依赖增量计数器（success_count/failed_count/processed_callbacks）。
回调仅做"唤醒父工作流"，状态判断由扫表驱动。
*/
#[async_trait]
impl PluginInterface for ForkJoinPlugin {
    async fn execute(
        &self,
        executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::ForkJoin(t) => t.clone(),
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ForkJoinPlugin");
                return Err(anyhow::anyhow!("Invalid task template for ForkJoinPlugin"));
            }
        };

        if template.tasks.is_empty() {
            debug!(node_id = %node_instance.node_id, "forkjoin: empty tasks, completing immediately");
            return Ok(ExecutionResult::success(None));
        }

        let total_tasks = template.tasks.len() as u64;
        let gather = Self::gather_child_task_status(
            executor, &template, node_instance, workflow_instance,
        ).await?;

        match Self::diagnose(total_tasks, template.max_failures, &gather) {
            ForkJoinDecision::AllDone(ForkJoinOutcome::Success) => {
                Self::write_output(node_instance, total_tasks, total_tasks, gather.results_map);
                Ok(ExecutionResult::success(None))
            }
            ForkJoinDecision::AllDone(ForkJoinOutcome::Failed) | ForkJoinDecision::EarlyAbort => {
                node_instance.error_message = Some(format!(
                    "ForkJoin aborted: {} failures out of {} tasks",
                    gather.failed_count, total_tasks
                ));
                Self::write_output(node_instance, total_tasks, total_tasks, gather.results_map);
                Ok(ExecutionResult::failed())
            }
            ForkJoinDecision::AllDispatched => {
                Self::write_output(node_instance, total_tasks, total_tasks, gather.results_map);
                Ok(ExecutionResult {
                    status: NodeExecutionStatus::Await,
                    dispatch_jobs: vec![],
                    dispatch_workflow_jobs: vec![],
                    jump_to_node: None,
                })
            }
            ForkJoinDecision::NeedDispatch => {
                let dispatched_count = node_instance
                    .task_instance
                    .output
                    .as_ref()
                    .and_then(|s| s.get("dispatched_count"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                let indices = Self::calc_dispatch_indices(
                    executor, &template, workflow_instance, node_instance,
                    dispatched_count, &gather,
                ).await;

                node_instance.task_instance.input = Some(json!({
                    "task_keys": template.tasks.iter().map(|t| t.task_key.clone()).collect::<Vec<_>>(),
                    "concurrency": template.concurrency,
                    "mode": format!("{:?}", template.mode),
                    "max_failures": template.max_failures,
                }));

                let (dispatch_jobs, dispatch_workflow_jobs) = Self::build_dispatch_jobs(
                    &template, workflow_instance, node_instance, &indices,
                );

                let new_dispatched = std::cmp::max(
                    dispatched_count,
                    indices.last().map(|&i| i as u64 + 1).unwrap_or(dispatched_count),
                );

                Self::write_output(node_instance, total_tasks, new_dispatched, gather.results_map);

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
            TaskTemplate::ForkJoin(t) => t.clone(),
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ForkJoinPlugin callback");
                return Err(anyhow::anyhow!("Invalid task template for ForkJoinPlugin"));
            }
        };

if template.tasks.is_empty() {
            return Ok(ExecutionResult::success(None));
        }

        let total_tasks = template.tasks.len() as u64;
        let gather = Self::gather_child_task_status(
            executor, &template, node_instance, workflow_instance,
        ).await?;

        match Self::diagnose(total_tasks, template.max_failures, &gather) {
            ForkJoinDecision::AllDone(ForkJoinOutcome::Success) => {
                Self::write_output(node_instance, total_tasks, total_tasks, gather.results_map);
                Ok(ExecutionResult::success(None))
            }
            ForkJoinDecision::AllDone(ForkJoinOutcome::Failed) | ForkJoinDecision::EarlyAbort => {
                node_instance.error_message = Some(format!(
                    "ForkJoin aborted: {} failures out of {} tasks",
                    gather.failed_count, total_tasks
                ));
                Self::write_output(node_instance, total_tasks, total_tasks, gather.results_map);
                Ok(ExecutionResult::failed())
            }
            ForkJoinDecision::AllDispatched => {
                Self::write_output(node_instance, total_tasks, total_tasks, gather.results_map);
                Ok(ExecutionResult {
                    status: NodeExecutionStatus::Await,
                    dispatch_jobs: vec![],
                    dispatch_workflow_jobs: vec![],
                    jump_to_node: None,
                })
            }
            ForkJoinDecision::NeedDispatch => {
                let dispatched_count = node_instance
                    .task_instance
                    .output
                    .as_ref()
                    .and_then(|s| s.get("dispatched_count"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                let indices = Self::calc_dispatch_indices(
                    executor, &template, workflow_instance, node_instance,
                    dispatched_count, &gather,
                ).await;

                let (dispatch_jobs, dispatch_workflow_jobs) = Self::build_dispatch_jobs(
                    &template, workflow_instance, node_instance, &indices,
                );

                let new_dispatched = std::cmp::max(
                    dispatched_count,
                    indices.last().map(|&i| i as u64 + 1).unwrap_or(dispatched_count),
                );

                Self::write_output(node_instance, total_tasks, new_dispatched, gather.results_map);

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
        crate::shared::workflow::TaskType::ForkJoin
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::{ChildStatus, PluginExecutor, PluginInterface};
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{
        ForkJoinTaskItem, ForkJoinTemplate, HttpMethod, ParallelMode, TaskHttpTemplate,
    };
    use chrono::Utc;

    struct StubExecutor {
        child_statuses: std::collections::HashMap<String, ChildStatus>,
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

    fn forkjoin_template(keys: &[&str]) -> TaskTemplate {
        TaskTemplate::ForkJoin(ForkJoinTemplate {
            tasks: keys
                .iter()
                .map(|k| ForkJoinTaskItem {
                    task_key: k.to_string(),
                    task_id: None,
                    name: k.to_string(),
                    task_template: TaskTemplate::Http(http_template()),
                })
                .collect(),
            concurrency: keys.len() as u32,
            mode: ParallelMode::Rolling,
            max_failures: None,
        })
    }

    fn make_node(node_id: &str, keys: &[&str]) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.into(),
            node_type: crate::shared::workflow::TaskType::ForkJoin,
            task_instance: crate::task::entity::task_definition::TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: "t1".into(),
                task_id: "".into(),
                task_name: "forkjoin".into(),
                task_type: crate::shared::workflow::TaskType::ForkJoin,
                task_template: forkjoin_template(keys),
                task_status: TaskInstanceStatus::Running,
                task_instance_id: format!("ti-{}", node_id),
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
            entry_node: "fj".into(),
            current_node: "fj".into(),
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

    #[tokio::test]
    async fn all_tasks_not_found_dispatches_initial_batch() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b", "c"];
        let mut node = make_node("fj", keys);
        let mut wf = make_instance();

        let exec = StubExecutor {
            child_statuses: std::collections::HashMap::new(),
        };

        let result = plugin
            .execute(&exec, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert_eq!(result.dispatch_jobs.len(), 3);
    }

    #[tokio::test]
    async fn all_tasks_completed_returns_success() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b"];
        let mut node = make_node("fj", keys);
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Completed(Some(json!({"result": 1}))));
        statuses.insert("wf1-fj-1".into(), ChildStatus::Completed(Some(json!({"result": 2}))));
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .execute(&exec, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
    }

    #[tokio::test]
    async fn one_failure_with_max_failures_zero_returns_failed() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b"];
        let mut node = make_node("fj", keys);
        if let TaskTemplate::ForkJoin(ref mut t) = node.task_instance.task_template {
            t.max_failures = Some(0);
        }
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-fj-1".into(), ChildStatus::Failed(None, Some("err".into())));
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .execute(&exec, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn mixed_running_and_not_found_dispatches_new_tasks() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b", "c"];
        let mut node = make_node("fj", keys);
        node.task_instance.output = Some(json!({
            "total_tasks": 3,
            "dispatched_count": 1,
            "results": { "a": null },
        }));
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Running);
        statuses.insert("wf1-fj-1".into(), ChildStatus::NotFound);
        statuses.insert("wf1-fj-2".into(), ChildStatus::NotFound);
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .execute(&exec, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert!(result.dispatch_jobs.len() >= 1 || result.dispatch_workflow_jobs.len() >= 1);
    }

    #[tokio::test]
    async fn all_running_pure_await() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b"];
        let mut node = make_node("fj", keys);
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Running);
        statuses.insert("wf1-fj-1".into(), ChildStatus::Running);
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .execute(&exec, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert!(result.dispatch_jobs.is_empty());
        assert!(result.dispatch_workflow_jobs.is_empty());
    }

    #[tokio::test]
    async fn skipped_counts_as_completed() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b"];
        let mut node = make_node("fj", keys);
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-fj-1".into(), ChildStatus::Skipped(None));
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .execute(&exec, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
    }

    #[tokio::test]
    async fn all_failed_no_max_failures_returns_failed() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b"];
        let mut node = make_node("fj", keys);
        if let TaskTemplate::ForkJoin(ref mut t) = node.task_instance.task_template {
            t.max_failures = None;
        }
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Failed(None, Some("err".into())));
        statuses.insert("wf1-fj-1".into(), ChildStatus::Failed(None, Some("err".into())));
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .execute(&exec, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn handle_callback_mixed_running_dispatches_more() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b", "c"];
        let mut node = make_node("fj", keys);
        node.task_instance.output = Some(json!({
            "total_tasks": 3,
            "dispatched_count": 2,
            "results": {},
        }));
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-fj-1".into(), ChildStatus::NotFound);
        statuses.insert("wf1-fj-2".into(), ChildStatus::NotFound);
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .handle_callback(&exec, &mut node, &mut wf, "wf1-fj-0", &NodeExecutionStatus::Success, &None, &None, &None)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
    }

    #[tokio::test]
    async fn handle_callback_all_terminal_returns_failed() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b"];
        let mut node = make_node("fj", keys);
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-fj-1".into(), ChildStatus::Failed(None, Some("err".into())));
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .handle_callback(&exec, &mut node, &mut wf, "wf1-fj-1", &NodeExecutionStatus::Failed, &None, &Some("err".into()), &None)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn batch_mode_waits_for_current_batch() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b", "c", "d", "e"];
        let mut node = make_node("fj", keys);
        if let TaskTemplate::ForkJoin(ref mut t) = node.task_instance.task_template {
            t.concurrency = 2;
            t.mode = ParallelMode::Batch;
        }
        node.task_instance.output = Some(json!({
            "total_tasks": 5,
            "dispatched_count": 2,
            "results": {},
        }));
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Running);
        statuses.insert("wf1-fj-1".into(), ChildStatus::Running);
        statuses.insert("wf1-fj-2".into(), ChildStatus::NotFound);
        statuses.insert("wf1-fj-3".into(), ChildStatus::NotFound);
        statuses.insert("wf1-fj-4".into(), ChildStatus::NotFound);
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .handle_callback(&exec, &mut node, &mut wf, "wf1-fj-0", &NodeExecutionStatus::Success, &None, &None, &None)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert!(result.dispatch_jobs.is_empty());
        assert!(result.dispatch_workflow_jobs.is_empty());
    }

    #[tokio::test]
    async fn batch_mode_dispatches_next_batch_when_current_completes() {
        let plugin = ForkJoinPlugin::new();
        let keys = &["a", "b", "c", "d", "e"];
        let mut node = make_node("fj", keys);
        if let TaskTemplate::ForkJoin(ref mut t) = node.task_instance.task_template {
            t.concurrency = 2;
            t.mode = ParallelMode::Batch;
        }
        node.task_instance.output = Some(json!({
            "total_tasks": 5,
            "dispatched_count": 2,
            "results": {},
        }));
        let mut wf = make_instance();

        let mut statuses = std::collections::HashMap::new();
        statuses.insert("wf1-fj-0".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-fj-1".into(), ChildStatus::Completed(None));
        statuses.insert("wf1-fj-2".into(), ChildStatus::NotFound);
        statuses.insert("wf1-fj-3".into(), ChildStatus::NotFound);
        statuses.insert("wf1-fj-4".into(), ChildStatus::NotFound);
        let exec = StubExecutor { child_statuses: statuses };

        let result = plugin
            .handle_callback(&exec, &mut node, &mut wf, "wf1-fj-1", &NodeExecutionStatus::Success, &None, &None, &None)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert!(result.dispatch_jobs.len() >= 2 || result.dispatch_workflow_jobs.len() >= 2);
    }
}