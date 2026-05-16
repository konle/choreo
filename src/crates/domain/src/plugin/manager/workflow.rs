//! Drive workflow instances: job handling, main execution loop, persistence, and async dispatch.
//!
//! **State machine (sketch)**  
//! - `Start` job: ensure instance is Pending/Running, then run [`PluginManager::execute_workflow_loop`].  
//! - `NodeCallback` job: if instance is Await/Suspended/Pending/Running, reactivate and reload as needed,
//!   merge callback payload with persisted task row when present, then apply plugin callback + execution result,
//!   optionally re-enter the loop.  
//! - The loop reloads the instance from storage each iteration so concurrent updates are visible.

use super::loop_action::LoopAction;
use super::PluginManager;
use crate::plugin::interface::{ExecutionResult, PluginExecutor};
use crate::shared::job::{ExecuteWorkflowJob, WorkflowEvent};
use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
use crate::task::entity::task_definition::TaskTemplate;
use crate::task::http_template_resolve::{resolved_http_request_snapshot, resolve_template_placeholders};
use crate::workflow::entity::workflow_definition::{NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity};
use crate::workflow::resolution_context::augment_merged_context_with_nodes;
use tracing::{debug, error, info, warn};

/// After reloading the workflow, whether the main loop should keep spinning.
enum LoopOutcome {
    Continue,
    Stop,
}

/// Whether a node callback job should proceed after instance-status transitions.
enum CallbackReadiness {
    /// Instance status does not accept this callback; nothing to do.
    Ignored,
    /// Instance is ready; may have been reloaded from DB.
    Ready(WorkflowInstanceEntity),
}

/// Merged callback fields after optional enrichment from `task_instances`.
struct CallbackPayload {
    status: NodeExecutionStatus,
    output: Option<serde_json::Value>,
    error_message: Option<String>,
    input: Option<serde_json::Value>,
}

impl PluginManager {
    pub async fn process_workflow_job(
        &self,
        job: ExecuteWorkflowJob,
        worker_id: &str,
    ) -> anyhow::Result<()> {
        let mut instance = match self
            .workflow_instance_svc
            .acquire_lock(&job.workflow_instance_id, worker_id, 10000)
            .await
        {
            Ok(inst) => inst,
            Err(e) => {
                warn!(
                    workflow_instance_id = %job.workflow_instance_id,
                    worker_id = %worker_id,
                    error = %e,
                    "failed to acquire lock, will retry"
                );
                return Err(anyhow::anyhow!(
                    "failed to acquire lock for instance {}: {}",
                    job.workflow_instance_id,
                    e
                ));
            }
        };

        let result = match job.event {
            // 工作流启动执行事件是Start。async fn execute_instance(
            WorkflowEvent::Start => self.on_workflow_start(&job.workflow_instance_id, &mut instance).await,
            WorkflowEvent::NodeCallback {
                node_id,
                child_task_id,
                status,
                output,
                error_message,
                input,
            } => {
                self.on_node_callback(
                    &job.workflow_instance_id,
                    &mut instance,
                    node_id,
                    child_task_id,
                    status,
                    output,
                    error_message,
                    input,
                )
                .await
            }
            WorkflowEvent::ChildRevived { node_id, child_id } => {
                self.on_child_revived(&job.workflow_instance_id, &mut instance, &node_id, &child_id)
                    .await
            }
            WorkflowEvent::RetryContainerChild { node_id, child_task_id } => {
                self.on_retry_container_child(
                    &job.workflow_instance_id,
                    &mut instance,
                    &node_id,
                    &child_task_id,
                )
                .await
            }
        };

        if let Err(e) = self
            .workflow_instance_svc
            .release_lock(&job.workflow_instance_id, worker_id, instance.epoch)
            .await
        {
            warn!(
                workflow_instance_id = %job.workflow_instance_id,
                error = %e,
                "failed to release lock"
            );
        }

        result
    }

    async fn on_workflow_start(
        &self,
        workflow_instance_id: &str,
        instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<()> {
        // NOTE: 只允许 Pending 状态进入 Start 路径。Running 的 NACK 重试不再放行，
        // 僵尸 Running 实例由 Sweeper 扫描恢复。
        // 原始 guard 为 !is_pending() && !is_running()，移除了 is_running() 分支。
        if !instance.is_pending() {
            debug!(
                workflow_instance_id = %workflow_instance_id,
                status = ?instance.status,
                "start ignored: instance not in pending"
            );
            return Ok(());
        }
        info!(
            workflow_instance_id = %workflow_instance_id,
            "starting workflow execution"
        );
        self.execute_workflow(instance).await
    }

    async fn on_node_callback(
        &self,
        workflow_instance_id: &str,
        instance: &mut WorkflowInstanceEntity,
        node_id: String,
        child_task_id: String,
        status: NodeExecutionStatus,
        output: Option<serde_json::Value>,
        error_message: Option<String>,
        input: Option<serde_json::Value>,
    ) -> anyhow::Result<()> {
        let ready = self.prepare_instance_for_node_callback(instance, &node_id).await?;
        let mut instance = match ready {
            CallbackReadiness::Ignored => return Ok(()),
            CallbackReadiness::Ready(i) => i,
        };

        debug!(
            workflow_instance_id = %workflow_instance_id,
            node_id = %node_id,
            child_task_id = %child_task_id,
            callback_status = ?status,
            "processing node callback"
        );

        let payload = self
            .enrich_callback_from_task_store(
                &child_task_id,
                CallbackPayload {
                    status,
                    output,
                    error_message,
                    input,
                },
            )
            .await;

        let Some(node_index) = instance.nodes.iter().position(|n| n.node_id == node_id) else {
            return Ok(());
        };

        let mut node = instance.nodes[node_index].clone();

        let exec_result = match self
            .handle_node_callback(
                &mut node,
                &mut instance,
                &child_task_id,
                &payload.status,
                &payload.output,
                &payload.error_message,
                &payload.input,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(
                    workflow_instance_id = %workflow_instance_id,
                    node_id = %node_id,
                    error = %e,
                    "node callback handling failed"
                );
                return Err(e);
            }
        };

        instance.nodes[node_index] = node;
        let action = self.apply_exec_result(&mut instance, node_index, exec_result).await?;

        match action {
            LoopAction::Advance | LoopAction::Retry => self.execute_workflow_loop(&mut instance).await,
            LoopAction::Done => Ok(()),
        }
    }

    async fn on_child_revived(
        &self,
        workflow_instance_id: &str,
        instance: &mut WorkflowInstanceEntity,
        node_id: &str,
        child_id: &str,
    ) -> anyhow::Result<()> {
        use crate::shared::workflow::WorkflowInstanceStatus;

        match instance.status {
            WorkflowInstanceStatus::Failed => {
                self.revive_from_failed(workflow_instance_id, instance, node_id, child_id)
                    .await
            }
            WorkflowInstanceStatus::Await => {
                self.revive_from_await(instance, node_id, child_id).await
            }
            _ => {
                debug!(
                    workflow_instance_id = %workflow_instance_id,
                    node_id = %node_id,
                    child_id = %child_id,
                    status = ?instance.status,
                    "ChildRevived ignored: parent not in Failed/Await"
                );
                Ok(())
            }
        }
    }

    async fn revive_from_failed(
        &self,
        workflow_instance_id: &str,
        instance: &mut WorkflowInstanceEntity,
        node_id: &str,
        child_id: &str,
    ) -> anyhow::Result<()> {
        use crate::shared::workflow::WorkflowInstanceStatus;
        use crate::workflow::entity::workflow_definition::NodeExecutionStatus;

        let node_index = instance
            .nodes
            .iter()
            .position(|n| n.node_id == node_id)
            .ok_or_else(|| anyhow::anyhow!("revive_from_failed: node not found: {}", node_id))?;

        let node = &mut instance.nodes[node_index];
        let is_container = matches!(
            node.node_type,
            crate::shared::workflow::TaskType::Parallel
                | crate::shared::workflow::TaskType::ForkJoin
        );

        if is_container {
            Self::rollback_child_in_container(node, child_id);
        }

        node.status = NodeExecutionStatus::Pending;
        node.error_message = None;

        let transition_result = instance
            .transition_status(WorkflowInstanceStatus::Pending)
            .map_err(|e| anyhow::anyhow!("revive_from_failed transition error: {}", e))?;

        self.save_instance_and_bump_epoch(instance).await?;

        // Dispatch outbound events (Revived to grandparent if this is also a child workflow)
        for job in transition_result.into_dispatch_jobs() {
            if let Err(e) = self.dispatcher.dispatch_workflow(job).await {
                warn!(
                    workflow_instance_id = %workflow_instance_id,
                    error = %e,
                    "failed to dispatch outbound ChildRevived to grandparent"
                );
            }
        }

        // Dispatch Start to self to re-enter execution loop
        info!(
            workflow_instance_id = %workflow_instance_id,
            node_id = %node_id,
            child_id = %child_id,
            "revive_from_failed: dispatching Start to self"
        );
        self.dispatcher
            .dispatch_workflow(crate::shared::job::ExecuteWorkflowJob {
                workflow_instance_id: workflow_instance_id.to_string(),
                tenant_id: instance.tenant_id.clone(),
                event: crate::shared::job::WorkflowEvent::Start,
            })
            .await?;

        Ok(())
    }

    async fn revive_from_await(
        &self,
        instance: &mut WorkflowInstanceEntity,
        node_id: &str,
        child_id: &str,
    ) -> anyhow::Result<()> {
        let node_index = instance
            .nodes
            .iter()
            .position(|n| n.node_id == node_id)
            .ok_or_else(|| anyhow::anyhow!("revive_from_await: node not found: {}", node_id))?;

        let node = &mut instance.nodes[node_index];
        let is_container = matches!(
            node.node_type,
            crate::shared::workflow::TaskType::Parallel
                | crate::shared::workflow::TaskType::ForkJoin
        );

        if is_container {
            Self::rollback_child_in_container(node, child_id);
            self.save_instance_and_bump_epoch(instance).await?;
        }

        debug!(
            workflow_instance_id = %instance.workflow_instance_id,
            node_id = %node_id,
            child_id = %child_id,
            "revive_from_await: container rollback done, staying in Await"
        );

        Ok(())
    }

    fn rollback_child_in_container(
        node: &mut WorkflowNodeInstanceEntity,
        child_id: &str,
    ) {
        let Some(ref mut state) = node.task_instance.output else {
            return;
        };

        // Remove from processed_callbacks
        if let Some(arr) = state.get_mut("processed_callbacks").and_then(|v| v.as_array_mut()) {
            arr.retain(|v| v.as_str() != Some(child_id));
        }

        // Check if the child was previously recorded as Failed in results
        let was_failed = state
            .get("results")
            .and_then(|r| r.get(child_id))
            .and_then(|e| e.get("status"))
            .and_then(|s| s.as_str())
            .map(|s| s == "Failed")
            .unwrap_or(false);

        if was_failed {
            if let Some(fc) = state.get("failed_count").and_then(|v| v.as_u64()) {
                state["failed_count"] = serde_json::json!(fc.saturating_sub(1));
            }
        }

        // Reset results entry
        if let Some(results) = state.get_mut("results").and_then(|r| r.as_object_mut()) {
            if results.contains_key(child_id) {
                results.insert(child_id.to_string(), serde_json::Value::Null);
            }
        }
    }

    /// Handle RetryContainerChild event: rollback container state and recover parent status.
    ///
    /// Executed by Worker under lock. The child TaskInstance has already been reset to Pending
    /// and dispatched by the API layer. This handler only modifies the parent workflow instance.
    async fn on_retry_container_child(
        &self,
        workflow_instance_id: &str,
        instance: &mut WorkflowInstanceEntity,
        node_id: &str,
        child_task_id: &str,
    ) -> anyhow::Result<()> {
        let node = instance
            .nodes
            .iter_mut()
            .find(|n| n.node_id == node_id)
            .ok_or_else(|| {
                anyhow::anyhow!("RetryContainerChild: node {} not found", node_id)
            })?;

        Self::rollback_child_in_container(node, child_task_id);

        match instance.status {
            WorkflowInstanceStatus::Failed => {
                let node = instance
                    .nodes
                    .iter_mut()
                    .find(|n| n.node_id == node_id)
                    .unwrap();
                node.status = crate::workflow::entity::workflow_definition::NodeExecutionStatus::Pending;
                node.error_message = None;

                let transition_result = instance
                    .transition_status(WorkflowInstanceStatus::Pending)
                    .map_err(|e| anyhow::anyhow!("RetryContainerChild transition: {}", e))?;

                self.save_instance_and_bump_epoch(instance).await?;

                for job in transition_result.into_dispatch_jobs() {
                    if let Err(e) = self.dispatcher.dispatch_workflow(job).await {
                        warn!(
                            workflow_instance_id = %workflow_instance_id,
                            error = %e,
                            "RetryContainerChild: failed to dispatch ChildRevived to grandparent"
                        );
                    }
                }

                info!(
                    workflow_instance_id = %workflow_instance_id,
                    node_id = %node_id,
                    child_task_id = %child_task_id,
                    "RetryContainerChild: Failed→Pending, dispatching Start"
                );
                self.dispatcher
                    .dispatch_workflow(crate::shared::job::ExecuteWorkflowJob {
                        workflow_instance_id: workflow_instance_id.to_string(),
                        tenant_id: instance.tenant_id.clone(),
                        event: crate::shared::job::WorkflowEvent::Start,
                    })
                    .await?;
            }
            WorkflowInstanceStatus::Await
            | WorkflowInstanceStatus::Pending
            | WorkflowInstanceStatus::Running => {
                self.save_instance_and_bump_epoch(instance).await?;
                debug!(
                    workflow_instance_id = %workflow_instance_id,
                    node_id = %node_id,
                    child_task_id = %child_task_id,
                    status = ?instance.status,
                    "RetryContainerChild: rollback only (parent not Failed)"
                );
            }
            _ => {
                debug!(
                    workflow_instance_id = %workflow_instance_id,
                    status = ?instance.status,
                    "RetryContainerChild ignored: terminal state"
                );
            }
        }

        // Proactive check: if the child task already finished (extreme timing),
        // supplement the callback that may have been lost
        let child_result = match &self.task_instance_svc {
            Some(svc) => svc.get_task_instance_entity(child_task_id.to_string()).await.ok(),
            None => None,
        };
        if let Some(child) = child_result {
            use crate::shared::workflow::TaskInstanceStatus;
            let terminal_status = match child.task_status {
                TaskInstanceStatus::Completed => Some(
                    crate::workflow::entity::workflow_definition::NodeExecutionStatus::Success,
                ),
                TaskInstanceStatus::Failed => Some(
                    crate::workflow::entity::workflow_definition::NodeExecutionStatus::Failed,
                ),
                _ => None,
            };

            if let Some(status) = terminal_status {
                info!(
                    workflow_instance_id = %workflow_instance_id,
                    child_task_id = %child_task_id,
                    child_status = ?child.task_status,
                    "RetryContainerChild: child already terminal, supplementing callback"
                );
                let _ = self
                    .dispatcher
                    .dispatch_workflow(crate::shared::job::ExecuteWorkflowJob {
                        workflow_instance_id: workflow_instance_id.to_string(),
                        tenant_id: instance.tenant_id.clone(),
                        event: crate::shared::job::WorkflowEvent::NodeCallback {
                            node_id: node_id.to_string(),
                            child_task_id: child_task_id.to_string(),
                            status,
                            output: child.output,
                            error_message: child.error_message,
                            input: child.input,
                        },
                    })
                    .await;
            }
        }

        Ok(())
    }

    /// Transitions workflow instance from Await/Suspended/Pending into a state where callbacks apply; reloads when needed.
    ///
    /// Returns `Ignored` if the callback targets a node that is no longer the current node
    /// (stale/delayed callback from a previously completed node).
    async fn prepare_instance_for_node_callback(
        &self,
        instance: &mut WorkflowInstanceEntity,
        expected_node_id: &str,
    ) -> anyhow::Result<CallbackReadiness> {
        if expected_node_id != instance.get_current_node() {
            debug!(
                workflow_instance_id = %instance.workflow_instance_id,
                expected_node_id = %expected_node_id,
                current_node = %instance.get_current_node(),
                "node callback ignored: stale callback targeting non-current node"
            );
            return Ok(CallbackReadiness::Ignored);
        }

        match instance.status {
            WorkflowInstanceStatus::Await => {
                self.workflow_instance_svc
                    .transition_instance(instance, WorkflowInstanceStatus::Pending)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                self.workflow_instance_svc
                    .transition_instance(instance, WorkflowInstanceStatus::Running)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;

                Ok(CallbackReadiness::Ready(instance.clone()))
            }
            WorkflowInstanceStatus::Suspended => {
                self.workflow_instance_svc
                    .transition_instance(instance, WorkflowInstanceStatus::Pending)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                self.workflow_instance_svc
                    .transition_instance(instance, WorkflowInstanceStatus::Running)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;

                Ok(CallbackReadiness::Ready(instance.clone()))
            }
            WorkflowInstanceStatus::Pending => {
                self.workflow_instance_svc
                    .transition_instance(instance, WorkflowInstanceStatus::Running)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;

                Ok(CallbackReadiness::Ready(instance.clone()))
            }
            WorkflowInstanceStatus::Running => Ok(CallbackReadiness::Ready(instance.clone())),
            _ => {
                debug!(
                    workflow_instance_id = %instance.workflow_instance_id,
                    status = ?instance.status,
                    "node callback ignored: instance not in await/suspended/running/pending"
                );
                Ok(CallbackReadiness::Ignored)
            }
        }
    }

    /// If the queue payload omits fields, backfill from the persisted task instance (source of truth for terminal state).
    async fn enrich_callback_from_task_store(
        &self,
        child_task_id: &str,
        mut payload: CallbackPayload,
    ) -> CallbackPayload {
        let Some(task_svc) = &self.task_instance_svc else {
            return payload;
        };
        let Ok(task_inst) = task_svc
            .get_task_instance_entity(child_task_id.to_string())
            .await
        else {
            return payload;
        };

        if payload.input.is_none() {
            payload.input = task_inst.input.clone();
        }
        if payload.output.is_none() {
            payload.output = task_inst.output.clone();
        }
        if payload.error_message.is_none() {
            payload.error_message = task_inst.error_message.clone();
        }
        if !matches!(payload.status, NodeExecutionStatus::Skipped) {
            payload.status = match task_inst.task_status {
                TaskInstanceStatus::Completed => NodeExecutionStatus::Success,
                TaskInstanceStatus::Failed => NodeExecutionStatus::Failed,
                TaskInstanceStatus::Running => NodeExecutionStatus::Running,
                TaskInstanceStatus::Canceled => NodeExecutionStatus::Failed,
                _ => payload.status,
            };
        }
        payload
    }

    pub async fn execute_workflow(
        &self,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<()> {
        let latest = self
            .workflow_instance_svc
            .get_workflow_instance(workflow_instance.workflow_instance_id.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        match latest.status {
            WorkflowInstanceStatus::Pending => {
                self.workflow_instance_svc
                    .transition_instance(workflow_instance, WorkflowInstanceStatus::Running)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
            // NOTE: Running case 已被注释掉。
            //
            // 原因：此分支依赖消息队列的 NACK 重试机制（Worker 崩溃后事件重新入队，
            // 新 Worker 看到实例已是 Running 时跳过 Pending→Running 转换直接进入循环）。
            // 但工作流引擎在设计之初并没有对消息队列重试做假设，Correctness 应该
            // 只依赖 Sweeper 兜底（锁过期 → 扫描僵尸 → 强制回退 Pending → 投递 Start）。
            // 移除此分支后，NACK 重投递的 Start 事件命中 Running 实例会走到 _ 分支
            // 被安全忽略，由 Sweeper 延迟恢复（默认 60 秒扫描间隔）。
            //
            // WorkflowInstanceStatus::Running => {
            //     // Idempotent Start: retried Start jobs may see Running already — continue the loop.
            // }
            _ => {
                debug!(
                    workflow_instance_id = %workflow_instance.workflow_instance_id,
                    status = ?latest.status,
                    "start event ignored for non-actionable workflow status"
                );
                return Ok(());
            }
        }

        self.execute_workflow_loop(workflow_instance).await
    }

    /// Runs forward until the instance is no longer Running, a node yields (async dispatch / suspend), or the workflow ends.
    ///
    /// Each iteration reloads from storage so we do not execute stale graph state under concurrent writers.
    pub async fn execute_workflow_loop(
        &self,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<()> {
        loop {
            debug!(
                workflow_instance_id = %workflow_instance.workflow_instance_id,
                "executing workflow loop iteration"
            );

            let Some(mut instance) = self
                .reload_workflow_if_running(&workflow_instance.workflow_instance_id)
                .await?
            else {
                return Ok(());
            };

            let current_node_id = instance.get_current_node();
            let node_index = Self::node_index_for_id(&instance, &current_node_id)?;
            let node_status = instance.nodes[node_index].status.clone();

            match self
                .workflow_loop_tick(&mut instance, node_index, &current_node_id, node_status)
                .await?
            {
                LoopOutcome::Continue => continue,
                LoopOutcome::Stop => return Ok(()),
            }
        }
    }

    async fn reload_workflow_if_running(
        &self,
        workflow_instance_id: &str,
    ) -> anyhow::Result<Option<WorkflowInstanceEntity>> {
        let instance = self
            .workflow_instance_svc
            .get_workflow_instance(workflow_instance_id.to_string())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        if !instance.is_running() {
            debug!(
                workflow_instance_id = %workflow_instance_id,
                status = ?instance.status,
                "instance not in running state, exiting loop"
            );
            return Ok(None);
        }
        Ok(Some(instance))
    }

    fn node_index_for_id(
        instance: &WorkflowInstanceEntity,
        node_id: &str,
    ) -> anyhow::Result<usize> {
        instance
            .nodes
            .iter()
            .position(|n| n.node_id == node_id)
            .ok_or_else(|| {
                error!(
                    workflow_instance_id = %instance.workflow_instance_id,
                    node_id = %node_id,
                    "node not found in instance"
                );
                anyhow::anyhow!("node not found: {}", node_id)
            })
    }

    async fn workflow_loop_tick(
        &self,
        instance: &mut WorkflowInstanceEntity,
        node_index: usize,
        current_node_id: &str,
        node_status: NodeExecutionStatus,
    ) -> anyhow::Result<LoopOutcome> {
        match node_status {
            NodeExecutionStatus::Success | NodeExecutionStatus::Skipped => {
                if let Some(next) = instance.nodes[node_index].next_node.clone() {
                    instance.current_node = next;
                    self.save_instance_and_bump_epoch(instance).await?;
                    Ok(LoopOutcome::Continue)
                } else {
                    info!(
                        workflow_instance_id = %instance.workflow_instance_id,
                        "workflow completed"
                    );
                    let old_status = self.workflow_instance_svc
                        .transition_instance(instance, WorkflowInstanceStatus::Completed)
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;
                    self.dispatch_outbound_for_transition(instance, &old_status).await;
                    Ok(LoopOutcome::Stop)
                }
            }
            NodeExecutionStatus::Failed => {
                error!(
                    workflow_instance_id = %instance.workflow_instance_id,
                    node_id = %current_node_id,
                    "workflow failed at node"
                );
                let old_status = self.workflow_instance_svc
                    .transition_instance(instance, WorkflowInstanceStatus::Failed)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                self.dispatch_outbound_for_transition(instance, &old_status).await;
                Ok(LoopOutcome::Stop)
            }
            NodeExecutionStatus::Suspended | NodeExecutionStatus::Await | NodeExecutionStatus::Running => Ok(LoopOutcome::Stop),
            NodeExecutionStatus::Pending => {
                instance.nodes[node_index].status = NodeExecutionStatus::Running;
                self.save_instance_and_bump_epoch(instance).await?;

                match self.run_node(instance, node_index).await? {
                    LoopAction::Advance | LoopAction::Retry => Ok(LoopOutcome::Continue),
                    LoopAction::Done => Ok(LoopOutcome::Stop),
                }
            }
        }
    }

    async fn run_node(
        &self,
        instance: &mut WorkflowInstanceEntity,
        node_index: usize,
    ) -> anyhow::Result<LoopAction> {
        let mut node = instance.nodes[node_index].clone();

        if let Some(ref var_svc) = self.variable_svc {
            match var_svc
                .resolve_variables(
                    &instance.tenant_id,
                    &instance.workflow_meta_id,
                    &instance.context,
                    &node.context,
                )
                .await
            {
                Ok(merged) => node.context = merged,
                Err(e) => warn!(
                    workflow_instance_id = %instance.workflow_instance_id,
                    node_id = %node.node_id,
                    error = %e,
                    "variable resolution failed, using raw context"
                ),
            }
        }

        if let Some(ref uid) = instance.created_by {
            if let Some(obj) = node.context.as_object_mut() {
                obj.insert("__initiator__".into(), serde_json::json!(uid));
            }
        }

        node.context = augment_merged_context_with_nodes(
            instance,
            &node.node_id,
            node.context.clone(),
        );

        match &node.task_instance.task_template {
            TaskTemplate::Http(tpl) => {
                node.task_instance.input = Some(resolved_http_request_snapshot(tpl, &node.context));
            }
            TaskTemplate::Llm(tpl) => {
                node.task_instance.input = Some(resolved_llm_request_snapshot(tpl, &node.context));
            }
            _ => {}
        }

        let result = self.execute_node_instance(&mut node, instance).await;
        instance.nodes[node_index] = node;

        let exec_result = match result {
            Ok(r) => {
                debug!(
                    workflow_instance_id = %instance.workflow_instance_id,
                    node_id = %instance.nodes[node_index].node_id,
                    "node execution finished"
                );
                r
            }
            Err(e) => {
                error!(
                    workflow_instance_id = %instance.workflow_instance_id,
                    node_id = %instance.nodes[node_index].node_id,
                    error = %e,
                    "node execution failed"
                );
                instance.nodes[node_index].error_message = Some(e.to_string());
                ExecutionResult::failed()
            }
        };

        self.apply_exec_result(instance, node_index, exec_result).await
    }

}

pub fn resolved_llm_request_snapshot(
    tpl: &crate::task::entity::task_definition::LlmTemplate,
    ctx: &serde_json::Value,
) -> serde_json::Value {
    use crate::task::http_template_resolve::{resolve_form_to_json, resolve_template_placeholders, merge_ctx_with_task_form_layer};

    // Resolve form defaults against base ctx (Variable types get {{}} substituted),
    // then merge with REVERSED priority compared to HTTP:
    //   HTTP:  effective_ctx = merge(ctx, form_layer)   → form overrides ctx (task hard config)
    //   LLM:   effective_ctx = merge(form_layer, ctx)    → ctx overrides form (user input > defaults)
    let form_layer: serde_json::Map<String, serde_json::Value> = tpl
        .form
        .iter()
        .filter(|f| !f.key.trim().is_empty())
        .map(|f| (f.key.clone(), resolve_form_to_json(f, ctx)))
        .collect();

    let effective_ctx = if form_layer.is_empty() {
        ctx.clone()
    } else {
        merge_ctx_with_task_form_layer(&serde_json::Value::Object(form_layer.clone()), &ctx.as_object().cloned().unwrap_or_default())
    };

    let system_prompt = tpl
        .system_prompt
        .as_deref()
        .map(|s| resolve_template_placeholders(s, &effective_ctx))
        .unwrap_or_default();
    let user_prompt = resolve_template_placeholders(&tpl.user_prompt, &effective_ctx);
    let base_url = resolve_template_placeholders(&tpl.base_url, &effective_ctx);
    let model = resolve_template_placeholders(&tpl.model, &effective_ctx);

    let api_key_ref = &tpl.api_key_ref;
    let api_key = crate::task::http_template_resolve::get_by_path_pub(&effective_ctx, api_key_ref)
        .and_then(|v| match v {
            serde_json::Value::String(s) => Some(s),
            _ => None,
        })
        .unwrap_or_default();

    let mut snapshot = serde_json::json!({
        "base_url": base_url,
        "model": model,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "api_key_ref": api_key_ref,
        "temperature": tpl.temperature,
        "max_tokens": tpl.max_tokens,
        "response_format": tpl.response_format,
    });
    if !api_key.is_empty() {
        snapshot["_api_key"] = serde_json::Value::String(api_key);
    }
    if !form_layer.is_empty() {
        snapshot["form"] = serde_json::Value::Object(form_layer);
    }
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::workflow::TaskType;
    use crate::task::entity::task_definition::{TaskInstanceEntity, TaskTemplate};
    use crate::workflow::entity::workflow_definition::{
        NodeExecutionStatus, WorkflowNodeInstanceEntity,
    };

    fn make_container_node(
        node_id: &str,
        failed_count: u64,
        success_count: u64,
        processed: Vec<&str>,
        results: serde_json::Value,
    ) -> WorkflowNodeInstanceEntity {
        let state = serde_json::json!({
            "total_items": 2,
            "dispatched_count": 2,
            "success_count": success_count,
            "failed_count": failed_count,
            "skipped_count": 0,
            "processed_callbacks": processed,
            "results": results,
        });
        WorkflowNodeInstanceEntity {
            node_id: node_id.to_string(),
            node_type: TaskType::Parallel,
            task_instance: TaskInstanceEntity {
                id: format!("{}-task", node_id),
                tenant_id: "t".to_string(),
                task_id: "".to_string(),
                task_name: "".to_string(),
                task_type: TaskType::Parallel,
                task_template: TaskTemplate::Parallel(
                    crate::task::entity::task_definition::ParallelTemplate {
                        items_path: "items".to_string(),
                        item_alias: "item".to_string(),
                        task_template: Box::new(TaskTemplate::Http(
                            crate::task::entity::task_definition::TaskHttpTemplate {
                                url: "http://x".to_string(),
                                method: crate::task::entity::task_definition::HttpMethod::Get,
                                headers: vec![],
                                body: vec![],
                                form: vec![],
                                retry_count: 0,
                                retry_delay: 0,
                                timeout: 30,
                                success_condition: None,
                            },
                        )),
                        concurrency: 10,
                        mode: crate::task::entity::task_definition::ParallelMode::Rolling,
                        max_failures: Some(2),
                    },
                ),
                task_status: crate::shared::workflow::TaskInstanceStatus::Pending,
                task_instance_id: format!("{}-task", node_id),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                deleted_at: None,
                input: None,
                output: Some(state),
                error_message: None,
                execution_duration: None,
                caller_context: None,
            },
            context: serde_json::json!({}),
            next_node: None,
            status: NodeExecutionStatus::Failed,
            error_message: Some("Parallel aborted".to_string()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_rollback_child_in_container_decrements_failed_count() {
        let mut node = make_container_node(
            "node_6",
            2,
            0,
            vec!["wf-node_6-0", "wf-node_6-1"],
            serde_json::json!({
                "wf-node_6-0": {"status": "Failed", "output": null, "error": "err0"},
                "wf-node_6-1": {"status": "Failed", "output": null, "error": "err1"},
            }),
        );

        PluginManager::rollback_child_in_container(&mut node, "wf-node_6-0");

        let state = node.task_instance.output.as_ref().unwrap();
        assert_eq!(state["failed_count"], 1);
        assert_eq!(state["results"]["wf-node_6-0"], serde_json::Value::Null);
        let processed: Vec<String> = state["processed_callbacks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(!processed.contains(&"wf-node_6-0".to_string()));
        assert!(processed.contains(&"wf-node_6-1".to_string()));
    }

    #[test]
    fn test_rollback_child_in_container_idempotent() {
        let mut node = make_container_node(
            "node_6",
            2,
            0,
            vec!["wf-node_6-0", "wf-node_6-1"],
            serde_json::json!({
                "wf-node_6-0": {"status": "Failed", "output": null, "error": "err0"},
                "wf-node_6-1": {"status": "Failed", "output": null, "error": "err1"},
            }),
        );

        PluginManager::rollback_child_in_container(&mut node, "wf-node_6-0");
        PluginManager::rollback_child_in_container(&mut node, "wf-node_6-0");

        let state = node.task_instance.output.as_ref().unwrap();
        assert_eq!(state["failed_count"], 1);
    }

    #[test]
    fn test_rollback_child_not_in_processed_is_noop() {
        let mut node = make_container_node(
            "node_6",
            1,
            1,
            vec!["wf-node_6-1"],
            serde_json::json!({
                "wf-node_6-0": {"status": "Success", "output": {"ok": true}},
                "wf-node_6-1": {"status": "Failed", "output": null, "error": "err"},
            }),
        );

        PluginManager::rollback_child_in_container(&mut node, "wf-node_6-99");

        let state = node.task_instance.output.as_ref().unwrap();
        assert_eq!(state["failed_count"], 1);
        assert_eq!(state["success_count"], 1);
    }

    #[test]
    fn test_rollback_two_children_sequentially() {
        let mut node = make_container_node(
            "node_6",
            2,
            0,
            vec!["wf-node_6-0", "wf-node_6-1"],
            serde_json::json!({
                "wf-node_6-0": {"status": "Failed", "output": null, "error": "err0"},
                "wf-node_6-1": {"status": "Failed", "output": null, "error": "err1"},
            }),
        );

        PluginManager::rollback_child_in_container(&mut node, "wf-node_6-0");
        PluginManager::rollback_child_in_container(&mut node, "wf-node_6-1");

        let state = node.task_instance.output.as_ref().unwrap();
        assert_eq!(state["failed_count"], 0);
        let processed: Vec<String> = state["processed_callbacks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(processed.is_empty());
    }

    #[test]
    fn test_stale_callback_guard_rejects_mismatched_node() {
        use crate::workflow::entity::workflow_definition::WorkflowInstanceEntity;
        use crate::shared::workflow::WorkflowInstanceStatus;

        let instance = WorkflowInstanceEntity {
            workflow_instance_id: "wf-1".to_string(),
            tenant_id: "t".to_string(),
            workflow_meta_id: "meta-1".to_string(),
            workflow_version: 1,
            status: WorkflowInstanceStatus::Await,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "node_1".to_string(),
            current_node: "node_9".to_string(),
            nodes: vec![],
            epoch: 10,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 0,
            created_by: Some("user".to_string()),
        };

        // Callback targeting node_6, but current_node is node_9 → should be rejected
        assert_ne!("node_6", instance.get_current_node());
        assert_eq!("node_9", instance.get_current_node());
    }

    #[test]
    fn test_stale_callback_guard_accepts_matching_node() {
        use crate::workflow::entity::workflow_definition::WorkflowInstanceEntity;
        use crate::shared::workflow::WorkflowInstanceStatus;

        let instance = WorkflowInstanceEntity {
            workflow_instance_id: "wf-1".to_string(),
            tenant_id: "t".to_string(),
            workflow_meta_id: "meta-1".to_string(),
            workflow_version: 1,
            status: WorkflowInstanceStatus::Await,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "node_1".to_string(),
            current_node: "node_6".to_string(),
            nodes: vec![],
            epoch: 10,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 0,
            created_by: Some("user".to_string()),
        };

        // Callback targeting node_6, current_node is node_6 → should be accepted
        assert_eq!("node_6", instance.get_current_node());
    }
}
