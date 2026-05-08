use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tracing::{debug, warn, error};

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::workflow::TaskType;
use crate::shared::job::{ExecuteTaskJob, WorkflowCallerContext};
use crate::workflow::entity::workflow_definition::{
    NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};
use crate::task::entity::task_definition::TaskTemplate;

pub struct ParallelPlugin {}

impl ParallelPlugin {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl PluginInterface for ParallelPlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::Parallel(t) => t,
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ParallelPlugin");
                return Err(anyhow::anyhow!("Invalid task template for ParallelPlugin"));
            }
        };

        // Re-evaluation: if state already exists (dispatched_count > 0), this is a re-entry
        if let Some(ref state) = node_instance.task_instance.output {
            let dispatched = state.get("dispatched_count").and_then(|v| v.as_u64()).unwrap_or(0);
            if dispatched > 0 {
                debug!(
                    node_id = %node_instance.node_id,
                    "parallel: re-evaluation — children already dispatched, returning Await"
                );
                return Ok(ExecutionResult {
                    status: NodeExecutionStatus::Await,
                    dispatch_jobs: vec![],
                    dispatch_workflow_jobs: vec![],
                    jump_to_node: None,
                });
            }
        }

        let items_path = &template.items_path;
        
        let pointer_path = if items_path.starts_with('/') {
            items_path.clone()
        } else {
            format!("/{}", items_path.replace(".", "/"))
        };

        let items_val = workflow_instance.context.pointer(&pointer_path)
            .or_else(|| node_instance.context.pointer(&pointer_path));

        let items = match items_val {
            Some(JsonValue::Array(arr)) => arr,
            _ => {
                error!(
                    node_id = %node_instance.node_id,
                    items_path = %items_path,
                    "items path did not resolve to a JSON array"
                );
                return Err(anyhow::anyhow!("Items path '{}' did not resolve to a JSON array", items_path));
            }
        };

        if items.is_empty() {
            debug!(node_id = %node_instance.node_id, "parallel: empty items, completing immediately");
            return Ok(ExecutionResult::success(None));
        }

        let total_items = items.len();
        let concurrency = template.concurrency as usize;
        let initial_dispatch_count = std::cmp::min(total_items, concurrency);

        debug!(
            node_id = %node_instance.node_id,
            total_items = total_items,
            concurrency = concurrency,
            initial_dispatch = initial_dispatch_count,
            "parallel: dispatching initial batch"
        );

        node_instance.task_instance.input = Some(serde_json::json!({
            "items_path": template.items_path,
            "item_alias": template.item_alias,
            "concurrency": template.concurrency,
            "mode": format!("{:?}", template.mode),
            "max_failures": template.max_failures,
        }));

        let mut results_map = serde_json::Map::new();
        for index in 0..total_items {
            let child_id = format!("{}-{}-{}", workflow_instance.workflow_instance_id, node_instance.node_id, index);
            results_map.insert(child_id, JsonValue::Null);
        }

        let state = serde_json::json!({
            "total_items": total_items,
            "dispatched_count": initial_dispatch_count,
            "success_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "results": results_map,
        });
        node_instance.task_instance.output = Some(state);

        let mut jobs = Vec::new();
        for index in 0..initial_dispatch_count {
            let caller_context = WorkflowCallerContext {
                workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
                node_id: node_instance.node_id.clone(),
                parent_task_instance_id: Some(node_instance.task_instance.id.clone()),
                item_index: Some(index),
            };

            let job = ExecuteTaskJob {
                task_instance_id: format!("{}-{}-{}", workflow_instance.workflow_instance_id, node_instance.node_id, index),
                tenant_id: workflow_instance.tenant_id.clone(),
                caller_context: Some(caller_context),
            };
            jobs.push(job);
        }

        Ok(ExecutionResult::async_dispatch_multiple(jobs))
    }

    async fn handle_callback(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
        child_task_id: &str,
        status: &NodeExecutionStatus,
        _output: &Option<serde_json::Value>,
        _error_message: &Option<String>,
        _input: &Option<serde_json::Value>,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::Parallel(t) => t,
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ParallelPlugin callback");
                return Err(anyhow::anyhow!("Invalid task template for ParallelPlugin"));
            }
        };

        let mut state = node_instance.task_instance.output.clone().unwrap_or(serde_json::json!({}));

        let processed: Vec<String> = state
            .get("processed_callbacks")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let is_duplicate = processed.contains(&child_task_id.to_string());

        if is_duplicate && *status != NodeExecutionStatus::Skipped {
            warn!(
                node_id = %node_instance.node_id,
                child_task_id = %child_task_id,
                "parallel: duplicate callback ignored"
            );
            return Ok(ExecutionResult::async_dispatch_multiple(vec![]));
        }

        let mut success_count = state["success_count"].as_u64().unwrap_or(0);
        let mut failed_count = state["failed_count"].as_u64().unwrap_or(0);
        let mut skipped_count = state["skipped_count"].as_u64().unwrap_or(0);
        let total_items = state["total_items"].as_u64().unwrap_or(0);
        let mut dispatched_count = state["dispatched_count"].as_u64().unwrap_or(0);

        if is_duplicate && *status == NodeExecutionStatus::Skipped {
            let prev_status = state.get("results")
                .and_then(|r| r.get(child_task_id))
                .and_then(|e| e.get("status"))
                .and_then(|s| s.as_str())
                .unwrap_or("");
            match prev_status {
                "Failed" => { failed_count = failed_count.saturating_sub(1); }
                "Success" => { success_count = success_count.saturating_sub(1); }
                _ => {}
            }
            success_count += 1;
            skipped_count += 1;
            debug!(
                node_id = %node_instance.node_id,
                child_task_id = %child_task_id,
                prev_status = %prev_status,
                "parallel: skip overriding previous callback"
            );
        } else if *status == NodeExecutionStatus::Success {
            success_count += 1;
        } else if *status == NodeExecutionStatus::Skipped {
            success_count += 1;
            skipped_count += 1;
        } else if *status == NodeExecutionStatus::Failed {
            failed_count += 1;
        }

        let result_entry = serde_json::json!({
            "status": format!("{:?}", status),
            "output": _output,
            "error": _error_message,
        });
        if let Some(results) = state.get_mut("results").and_then(|r| r.as_object_mut()) {
            results.insert(child_task_id.to_string(), result_entry);
        }

        let concurrency = template.concurrency as u64;
        let mode = template.mode.clone();
        let max_failures = template.max_failures;

        // Stale Failure Check: verify previously-failed children are actually still failed.
        // If any have been retried (no longer Failed), reconcile counters.
        if failed_count > 0 {
            if let Some(results_obj) = state.get("results").and_then(|r| r.as_object()) {
                let failed_ids: Vec<String> = results_obj
                    .iter()
                    .filter(|(k, v)| {
                        *k != child_task_id
                            && v.get("status")
                                .and_then(|s| s.as_str())
                                .map(|s| s == "Failed")
                                .unwrap_or(false)
                    })
                    .map(|(k, _)| k.clone())
                    .collect();

                let mut stale_count = 0u64;
                for fid in &failed_ids {
                    if !_executor.is_task_still_failed(fid).await {
                        stale_count += 1;
                        // Remove from processed_callbacks
                        if let Some(arr) = state.get_mut("processed_callbacks").and_then(|v| v.as_array_mut()) {
                            arr.retain(|v| v.as_str() != Some(fid.as_str()));
                        }
                        // Reset results entry
                        if let Some(results) = state.get_mut("results").and_then(|r| r.as_object_mut()) {
                            results.insert(fid.clone(), serde_json::Value::Null);
                        }
                    }
                }
                if stale_count > 0 {
                    failed_count = failed_count.saturating_sub(stale_count);
                    debug!(
                        node_id = %node_instance.node_id,
                        stale_count = stale_count,
                        new_failed_count = failed_count,
                        "parallel: stale failure check reconciled counters"
                    );
                }
            }
        }

        // max_failures: early-abort threshold only.
        //   Some(n) → abort as soon as failed_count >= n (don't waste resources on remaining items)
        //   None    → never abort early; wait for all items to finish
        // At completion: failed_count > 0 always means Failed.
        let early_abort = match max_failures {
            Some(max) => failed_count >= max as u64,
            None => false,
        };

        let all_done = success_count + failed_count == total_items;

        let exec_result = if early_abort {
            warn!(
                node_id = %node_instance.node_id,
                failed_count = failed_count,
                max_failures = ?max_failures,
                "parallel: max_failures reached, aborting"
            );
            node_instance.error_message = Some(format!("Parallel aborted: {} failures reached max_failures={}", failed_count, max_failures.unwrap_or(0)));
            ExecutionResult::failed()
        } else if all_done && failed_count > 0 {
            warn!(
                node_id = %node_instance.node_id,
                failed_count = failed_count,
                success_count = success_count,
                "parallel: completed with failures"
            );
            node_instance.error_message = Some(format!("Parallel completed with {} failures out of {} items", failed_count, total_items));
            ExecutionResult::failed()
        } else if all_done {
            debug!(
                node_id = %node_instance.node_id,
                success_count = success_count,
                "parallel: all items completed successfully"
            );
            ExecutionResult::success(None)
        } else {
            let mut jobs_to_dispatch = Vec::new();
            
            if mode == crate::task::entity::task_definition::ParallelMode::Rolling {
                if dispatched_count < total_items {
                    jobs_to_dispatch.push(dispatched_count);
                    dispatched_count += 1;
                }
            } else if mode == crate::task::entity::task_definition::ParallelMode::Batch {
                if success_count + failed_count == dispatched_count {
                    let end = std::cmp::min(dispatched_count + concurrency, total_items);
                    for i in dispatched_count..end {
                        jobs_to_dispatch.push(i);
                    }
                    dispatched_count = end;
                }
            }

            let mut new_jobs = Vec::new();
            for idx in jobs_to_dispatch {
                let caller_context = WorkflowCallerContext {
                    workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
                    node_id: node_instance.node_id.clone(),
                    parent_task_instance_id: Some(node_instance.task_instance.id.clone()),
                    item_index: Some(idx as usize),
                };
                let child_job = ExecuteTaskJob {
                    task_instance_id: format!("{}-{}-{}", workflow_instance.workflow_instance_id, node_instance.node_id, idx),
                    tenant_id: workflow_instance.tenant_id.clone(),
                    caller_context: Some(caller_context),
                };
                new_jobs.push(child_job);
            }
            ExecutionResult::async_dispatch_multiple(new_jobs)
        };

        state["success_count"] = serde_json::json!(success_count);
        state["failed_count"] = serde_json::json!(failed_count);
        state["skipped_count"] = serde_json::json!(skipped_count);
        state["dispatched_count"] = serde_json::json!(dispatched_count);
        let mut updated_processed = processed;
        updated_processed.push(child_task_id.to_string());
        state["processed_callbacks"] = serde_json::json!(updated_processed);
        node_instance.task_instance.output = Some(state);

        Ok(exec_result)
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::Parallel
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::{PluginExecutor, PluginInterface};
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{
        HttpMethod, ParallelMode, ParallelTemplate, TaskHttpTemplate, TaskTemplate,
    };
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

    fn parallel_template(total: usize) -> TaskTemplate {
        TaskTemplate::Parallel(ParallelTemplate {
            items_path: "items".into(),
            item_alias: "item".into(),
            task_template: Box::new(TaskTemplate::Http(http_template())),
            concurrency: total as u32,
            mode: ParallelMode::Rolling,
            max_failures: None,
        })
    }

    fn make_node(node_id: &str, total: usize, dispatched: usize, success: u64, failed: u64, skipped: u64, processed: Vec<String>) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        let mut results_map = serde_json::Map::new();
        for i in 0..total {
            let child_id = format!("wf1-{}-{}", node_id, i);
            results_map.insert(child_id, JsonValue::Null);
        }
        let state = serde_json::json!({
            "total_items": total,
            "dispatched_count": dispatched,
            "success_count": success,
            "failed_count": failed,
            "skipped_count": skipped,
            "processed_callbacks": processed,
            "results": results_map,
        });
        WorkflowNodeInstanceEntity {
            node_id: node_id.into(),
            node_type: TaskType::Parallel,
            task_instance: crate::task::entity::task_definition::TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: "t1".into(),
                task_id: "".into(),
                task_name: "parallel".into(),
                task_type: TaskType::Parallel,
                task_template: parallel_template(total),
                task_status: TaskInstanceStatus::Running,
                task_instance_id: format!("ti-{}", node_id),
                created_at: now,
                updated_at: now,
                deleted_at: None,
                input: None,
                output: Some(state),
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

    fn make_instance() -> WorkflowInstanceEntity {
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
            context: serde_json::json!({"items": [1,2,3]}),
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

    #[tokio::test]
    async fn skipped_callback_counts_as_success() {
        let plugin = ParallelPlugin::new();
        let exec = StubExecutor;
        let mut node = make_node("p", 3, 3, 1, 0, 0, vec!["wf1-p-0".into()]);
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-1",
                &NodeExecutionStatus::Skipped,
                &Some(serde_json::json!({})),
                &None,
                &None,
            )
            .await
            .unwrap();

        let state = node.task_instance.output.unwrap();
        assert_eq!(state["success_count"], 2);
        assert_eq!(state["skipped_count"], 1);
        assert_eq!(state["failed_count"], 0);
        assert_eq!(result.status, NodeExecutionStatus::Await);
    }

    #[tokio::test]
    async fn all_skipped_completes_parallel() {
        let plugin = ParallelPlugin::new();
        let exec = StubExecutor;
        let mut node = make_node("p", 2, 2, 0, 0, 1, vec!["wf1-p-0".into()]);
        {
            let s = node.task_instance.output.as_mut().unwrap();
            s["success_count"] = serde_json::json!(1);
        }
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-1",
                &NodeExecutionStatus::Skipped,
                &Some(serde_json::json!({})),
                &None,
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
        let state = node.task_instance.output.unwrap();
        assert_eq!(state["success_count"], 2);
        assert_eq!(state["skipped_count"], 2);
    }

    #[tokio::test]
    async fn duplicate_non_skip_callback_ignored() {
        let plugin = ParallelPlugin::new();
        let exec = StubExecutor;
        let mut node = make_node("p", 2, 2, 1, 0, 0, vec!["wf1-p-0".into()]);

        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-0",
                &NodeExecutionStatus::Success,
                &Some(serde_json::json!({})),
                &None,
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        let state = node.task_instance.output.unwrap();
        assert_eq!(state["success_count"], 1);
    }

    #[tokio::test]
    async fn skip_overrides_previous_failed_callback() {
        let plugin = ParallelPlugin::new();
        let exec = StubExecutor;
        // total=2, dispatched=2, success=1, failed=1
        // Both already processed, node_6-0 was Failed, node_6-1 was Success
        let mut node = make_node("p", 2, 2, 1, 1, 0, vec!["wf1-p-0".into(), "wf1-p-1".into()]);
        {
            let s = node.task_instance.output.as_mut().unwrap();
            if let Some(results) = s.get_mut("results").and_then(|r| r.as_object_mut()) {
                results.insert("wf1-p-0".into(), serde_json::json!({"status": "Failed", "output": null, "error": "err"}));
                results.insert("wf1-p-1".into(), serde_json::json!({"status": "Success", "output": {}, "error": null}));
            }
        }
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-0",
                &NodeExecutionStatus::Skipped,
                &Some(serde_json::json!({})),
                &None,
                &None,
            )
            .await
            .unwrap();

        // Skip overrides Failed → now success_count=2, failed_count=0 → all done → Success
        assert_eq!(result.status, NodeExecutionStatus::Success);
        let state = node.task_instance.output.unwrap();
        assert_eq!(state["success_count"], 2);
        assert_eq!(state["failed_count"], 0);
        assert_eq!(state["skipped_count"], 1);
        assert_eq!(state["results"]["wf1-p-0"]["status"].as_str().unwrap(), "Skipped");
    }

    #[tokio::test]
    async fn all_failed_no_max_failures_returns_failed() {
        let plugin = ParallelPlugin::new();
        let exec = StubExecutor;
        // total=2, dispatched=2, success=0, failed=1, 1 already processed
        // max_failures is None (default in template) → any failure means container fails
        let mut node = make_node("p", 2, 2, 0, 1, 0, vec!["wf1-p-0".into()]);
        // Override template to have max_failures = None
        if let TaskTemplate::Parallel(ref mut t) = node.task_instance.task_template {
            t.max_failures = None;
        }
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-1",
                &NodeExecutionStatus::Failed,
                &None,
                &Some("error".into()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn all_failed_with_max_failures_equal_returns_failed() {
        let plugin = ParallelPlugin::new();
        let exec = StubExecutor;
        // total=2, max_failures=Some(2), 1 already failed, this is the 2nd failure
        // max_failures is an early-abort threshold (>=), so failed_count(2) >= max(2) → abort
        let mut node = make_node("p", 2, 2, 0, 1, 0, vec!["wf1-p-0".into()]);
        if let TaskTemplate::Parallel(ref mut t) = node.task_instance.task_template {
            t.max_failures = Some(2);
        }
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-1",
                &NodeExecutionStatus::Failed,
                &None,
                &Some("error".into()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn some_failures_at_completion_still_fails() {
        let plugin = ParallelPlugin::new();
        let exec = StubExecutor;
        // total=3, max_failures=None, success=2, failed=0, now 3rd fails
        // No early abort (None), but at completion: failed_count(1) > 0 → Failed
        let mut node = make_node("p", 3, 3, 2, 0, 0, vec!["wf1-p-0".into(), "wf1-p-1".into()]);
        if let TaskTemplate::Parallel(ref mut t) = node.task_instance.task_template {
            t.max_failures = None;
        }
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-2",
                &NodeExecutionStatus::Failed,
                &None,
                &Some("error".into()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn failures_exceeding_max_triggers_early_abort() {
        let plugin = ParallelPlugin::new();
        let exec = StubExecutor;
        // total=5, dispatched=5, success=1, failed=1, max_failures=Some(1)
        // Now 3rd callback is Failed → failed becomes 2, exceeds max(1) → abort
        let mut node = make_node("p", 5, 5, 1, 1, 0, vec!["wf1-p-0".into(), "wf1-p-1".into()]);
        if let TaskTemplate::Parallel(ref mut t) = node.task_instance.task_template {
            t.max_failures = Some(1);
        }
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-p-2",
                &NodeExecutionStatus::Failed,
                &None,
                &Some("error".into()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }
}
