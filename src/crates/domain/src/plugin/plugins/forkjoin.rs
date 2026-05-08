use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tracing::{debug, warn, error};

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::job::{ExecuteTaskJob, WorkflowCallerContext};
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::{ParallelMode, TaskTemplate};
use crate::workflow::entity::workflow_definition::{
    NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};

pub struct ForkJoinPlugin {}

impl ForkJoinPlugin {
    pub fn new() -> Self {
        Self {}
    }

    fn resolve_task_key_by_child_id<'a>(
        template: &'a crate::task::entity::task_definition::ForkJoinTemplate,
        child_task_id: &str,
    ) -> Option<&'a str> {
        let suffix = child_task_id.rsplit('-').next()?;
        let index: usize = suffix.parse().ok()?;
        template.tasks.get(index).map(|item| item.task_key.as_str())
    }
}

#[async_trait]
impl PluginInterface for ForkJoinPlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::ForkJoin(t) => t,
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ForkJoinPlugin");
                return Err(anyhow::anyhow!("Invalid task template for ForkJoinPlugin"));
            }
        };

        // Re-evaluation: if state already exists (dispatched_count > 0), this is a re-entry
        if let Some(ref state) = node_instance.task_instance.output {
            let dispatched = state.get("dispatched_count").and_then(|v| v.as_u64()).unwrap_or(0);
            if dispatched > 0 {
                debug!(
                    node_id = %node_instance.node_id,
                    "forkjoin: re-evaluation — children already dispatched, returning Await"
                );
                return Ok(ExecutionResult {
                    status: NodeExecutionStatus::Await,
                    dispatch_jobs: vec![],
                    dispatch_workflow_jobs: vec![],
                    jump_to_node: None,
                });
            }
        }

        if template.tasks.is_empty() {
            debug!(node_id = %node_instance.node_id, "forkjoin: empty tasks, completing immediately");
            return Ok(ExecutionResult::success(None));
        }

        let total_tasks = template.tasks.len();
        let concurrency = template.concurrency as usize;
        let initial_dispatch = std::cmp::min(total_tasks, concurrency);

        debug!(
            node_id = %node_instance.node_id,
            total_tasks = total_tasks,
            concurrency = concurrency,
            initial_dispatch = initial_dispatch,
            "forkjoin: dispatching initial batch"
        );

        let mut results_map = serde_json::Map::new();
        for item in &template.tasks {
            results_map.insert(item.task_key.clone(), JsonValue::Null);
        }

        node_instance.task_instance.input = Some(serde_json::json!({
            "task_keys": template.tasks.iter().map(|t| t.task_key.clone()).collect::<Vec<_>>(),
            "concurrency": template.concurrency,
            "mode": format!("{:?}", template.mode),
            "max_failures": template.max_failures,
        }));

        let state = serde_json::json!({
            "total_tasks": total_tasks,
            "dispatched_count": initial_dispatch,
            "success_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "results": results_map,
        });
        node_instance.task_instance.output = Some(state);

        let mut jobs = Vec::with_capacity(initial_dispatch);
        for index in 0..initial_dispatch {
            let caller_context = WorkflowCallerContext {
                workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
                node_id: node_instance.node_id.clone(),
                parent_task_instance_id: Some(node_instance.task_instance.id.clone()),
                item_index: Some(index),
            };

            let job = ExecuteTaskJob {
                task_instance_id: format!(
                    "{}-{}-{}",
                    workflow_instance.workflow_instance_id,
                    node_instance.node_id,
                    index
                ),
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
        output: &Option<serde_json::Value>,
        error_message: &Option<String>,
        _input: &Option<serde_json::Value>,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::ForkJoin(t) => t.clone(),
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ForkJoinPlugin callback");
                return Err(anyhow::anyhow!("Invalid task template for ForkJoinPlugin"));
            }
        };

        let mut state = node_instance
            .task_instance
            .output
            .clone()
            .unwrap_or(serde_json::json!({}));

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
                "forkjoin: duplicate callback ignored"
            );
            return Ok(ExecutionResult::async_dispatch_multiple(vec![]));
        }

        let mut success_count = state["success_count"].as_u64().unwrap_or(0);
        let mut failed_count = state["failed_count"].as_u64().unwrap_or(0);
        let mut skipped_count = state["skipped_count"].as_u64().unwrap_or(0);
        let total_tasks = state["total_tasks"].as_u64().unwrap_or(0);
        let mut dispatched_count = state["dispatched_count"].as_u64().unwrap_or(0);

        if is_duplicate && *status == NodeExecutionStatus::Skipped {
            let prev_status = if let Some(task_key) = Self::resolve_task_key_by_child_id(&template, child_task_id) {
                state.get("results")
                    .and_then(|r| r.get(task_key))
                    .and_then(|e| e.get("status"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string()
            } else {
                String::new()
            };
            match prev_status.as_str() {
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
                "forkjoin: skip overriding previous callback"
            );
        } else if *status == NodeExecutionStatus::Success {
            success_count += 1;
        } else if *status == NodeExecutionStatus::Skipped {
            success_count += 1;
            skipped_count += 1;
        } else if *status == NodeExecutionStatus::Failed {
            failed_count += 1;
        }

        if let Some(task_key) = Self::resolve_task_key_by_child_id(&template, child_task_id) {
            let result_entry = serde_json::json!({
                "status": format!("{:?}", status),
                "output": output,
                "error": error_message,
            });
            if let Some(results) = state.get_mut("results").and_then(|r| r.as_object_mut()) {
                results.insert(task_key.to_string(), result_entry);
            }
        }

        let concurrency = template.concurrency as u64;
        let mode = template.mode.clone();

        let max_failures = template.max_failures;

        // Stale Failure Check: verify previously-failed children are actually still failed.
        if failed_count > 0 {
            if let Some(results_obj) = state.get("results").and_then(|r| r.as_object()) {
                let failed_task_ids: Vec<(String, String)> = results_obj
                    .iter()
                    .filter(|(_, v)| {
                        v.get("status")
                            .and_then(|s| s.as_str())
                            .map(|s| s == "Failed")
                            .unwrap_or(false)
                    })
                    .filter_map(|(task_key, _)| {
                        // Resolve task_key back to child_task_id
                        template.tasks.iter().position(|t| t.task_key == *task_key).map(|idx| {
                            let cid = format!(
                                "{}-{}-{}",
                                workflow_instance.workflow_instance_id,
                                node_instance.node_id,
                                idx
                            );
                            (task_key.clone(), cid)
                        })
                    })
                    .filter(|(_, cid)| cid != child_task_id)
                    .collect();

                let mut stale_count = 0u64;
                for (task_key, cid) in &failed_task_ids {
                    if !_executor.is_task_still_failed(cid).await {
                        stale_count += 1;
                        if let Some(arr) = state.get_mut("processed_callbacks").and_then(|v| v.as_array_mut()) {
                            arr.retain(|v| v.as_str() != Some(cid.as_str()));
                        }
                        if let Some(results) = state.get_mut("results").and_then(|r| r.as_object_mut()) {
                            results.insert(task_key.clone(), serde_json::Value::Null);
                        }
                    }
                }
                if stale_count > 0 {
                    failed_count = failed_count.saturating_sub(stale_count);
                    debug!(
                        node_id = %node_instance.node_id,
                        stale_count = stale_count,
                        new_failed_count = failed_count,
                        "forkjoin: stale failure check reconciled counters"
                    );
                }
            }
        }

        // max_failures: early-abort threshold only.
        //   Some(n) → abort as soon as failed_count >= n
        //   None    → never abort early
        // At completion: failed_count > 0 always means Failed.
        let early_abort = match max_failures {
            Some(max) => failed_count >= max as u64,
            None => false,
        };

        let all_done = success_count + failed_count == total_tasks;

        let exec_result = if early_abort {
            warn!(
                node_id = %node_instance.node_id,
                failed_count = failed_count,
                max_failures = ?max_failures,
                "forkjoin: max_failures reached, aborting"
            );
            node_instance.error_message = Some(format!(
                "ForkJoin aborted: {} failures reached max_failures={}",
                failed_count, max_failures.unwrap_or(0)
            ));
            ExecutionResult::failed()
        } else if all_done && failed_count > 0 {
            warn!(
                node_id = %node_instance.node_id,
                failed_count = failed_count,
                success_count = success_count,
                "forkjoin: completed with failures"
            );
            node_instance.error_message = Some(format!(
                "ForkJoin completed with {} failures out of {} tasks",
                failed_count, total_tasks
            ));
            ExecutionResult::failed()
        } else if all_done {
            debug!(
                node_id = %node_instance.node_id,
                success_count = success_count,
                "forkjoin: all tasks completed successfully"
            );
            ExecutionResult::success(None)
        } else {
            let mut indices_to_dispatch = Vec::new();

            if mode == ParallelMode::Rolling {
                if dispatched_count < total_tasks {
                    indices_to_dispatch.push(dispatched_count);
                    dispatched_count += 1;
                }
            } else if mode == ParallelMode::Batch {
                if success_count + failed_count == dispatched_count {
                    let end = std::cmp::min(dispatched_count + concurrency, total_tasks);
                    for i in dispatched_count..end {
                        indices_to_dispatch.push(i);
                    }
                    dispatched_count = end;
                }
            }

            let new_jobs: Vec<ExecuteTaskJob> = indices_to_dispatch
                .into_iter()
                .map(|idx| {
                    let caller_context = WorkflowCallerContext {
                        workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
                        node_id: node_instance.node_id.clone(),
                        parent_task_instance_id: Some(node_instance.task_instance.id.clone()),
                        item_index: Some(idx as usize),
                    };
                    ExecuteTaskJob {
                        task_instance_id: format!(
                            "{}-{}-{}",
                            workflow_instance.workflow_instance_id,
                            node_instance.node_id,
                            idx
                        ),
                        tenant_id: workflow_instance.tenant_id.clone(),
                        caller_context: Some(caller_context),
                    }
                })
                .collect();

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
        TaskType::ForkJoin
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::{PluginExecutor, PluginInterface};
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{
        ForkJoinTaskItem, ForkJoinTemplate, HttpMethod, TaskHttpTemplate, TaskTemplate,
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

    fn make_node(node_id: &str, keys: &[&str], dispatched: usize, success: u64, failed: u64, skipped: u64, processed: Vec<String>, results: serde_json::Map<String, JsonValue>) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        let state = serde_json::json!({
            "total_tasks": keys.len(),
            "dispatched_count": dispatched,
            "success_count": success,
            "failed_count": failed,
            "skipped_count": skipped,
            "processed_callbacks": processed,
            "results": results,
        });
        WorkflowNodeInstanceEntity {
            node_id: node_id.into(),
            node_type: TaskType::ForkJoin,
            task_instance: crate::task::entity::task_definition::TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: "t1".into(),
                task_id: "".into(),
                task_name: "forkjoin".into(),
                task_type: TaskType::ForkJoin,
                task_template: forkjoin_template(keys),
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
    async fn skipped_callback_counts_as_success_and_records_in_results() {
        let plugin = ForkJoinPlugin::new();
        let exec = StubExecutor;
        let keys = &["create_user", "send_email", "log_audit"];
        let mut results = serde_json::Map::new();
        results.insert("create_user".into(), serde_json::json!({"status": "Success", "output": {"id": 1}, "error": null}));
        results.insert("send_email".into(), JsonValue::Null);
        results.insert("log_audit".into(), JsonValue::Null);

        let mut node = make_node("fj", keys, 3, 1, 0, 0, vec!["wf1-fj-0".into()], results);
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-fj-1",
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

        let entry = &state["results"]["send_email"];
        assert_eq!(entry["status"].as_str().unwrap(), "Skipped");
    }

    #[tokio::test]
    async fn all_tasks_done_with_skip_completes_forkjoin() {
        let plugin = ForkJoinPlugin::new();
        let exec = StubExecutor;
        let keys = &["a", "b"];
        let mut results = serde_json::Map::new();
        results.insert("a".into(), serde_json::json!({"status": "Success", "output": {}, "error": null}));
        results.insert("b".into(), JsonValue::Null);

        let mut node = make_node("fj", keys, 2, 1, 0, 0, vec!["wf1-fj-0".into()], results);
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-fj-1",
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
        assert_eq!(state["skipped_count"], 1);
    }

    #[tokio::test]
    async fn all_failed_no_max_failures_returns_failed() {
        let plugin = ForkJoinPlugin::new();
        let exec = StubExecutor;
        let keys = &["a", "b"];
        let mut results = serde_json::Map::new();
        results.insert("a".into(), serde_json::json!({"status": "Failed", "output": null, "error": "err"}));
        results.insert("b".into(), JsonValue::Null);

        let mut node = make_node("fj", keys, 2, 0, 1, 0, vec!["wf1-fj-0".into()], results);
        if let TaskTemplate::ForkJoin(ref mut t) = node.task_instance.task_template {
            t.max_failures = None;
        }
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &exec,
                &mut node,
                &mut wf,
                "wf1-fj-1",
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
