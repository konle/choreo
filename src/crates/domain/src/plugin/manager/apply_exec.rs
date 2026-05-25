//! Apply [`ExecutionResult`] to the workflow graph: node status, instance status, persistence, queue dispatch.
//!
//! Instance status is set **in-memory only** and persisted via the single
//! `save_instance_and_bump_epoch` CAS write. Separate `transfer_status` calls
//! (complete_instance, fail_instance, …) are intentionally avoided here to
//! prevent double-write epoch drift that would cause the CAS save to fail,
//! leaving the Parallel state (success_count / processed_callbacks) updated in
//! the DB but the workflow status / node status stale.

use super::PluginManager;
use super::loop_action::LoopAction;
use crate::plugin::interface::ExecutionResult;
use crate::shared::workflow::WorkflowInstanceStatus;
use crate::workflow::entity::transition::should_notify_parent;
use crate::workflow::entity::workflow_definition::{NodeExecutionStatus, WorkflowInstanceEntity};
use tracing::{error, warn};

impl PluginManager {
    pub(super) async fn apply_exec_result(
        &self,
        instance: &mut WorkflowInstanceEntity,
        node_index: usize,
        exec_result: ExecutionResult,
    ) -> anyhow::Result<LoopAction> {
        instance.nodes[node_index].status = exec_result.status.clone();
        let old_status = instance.status.clone();
        let action = match exec_result.status {
            NodeExecutionStatus::Success | NodeExecutionStatus::Skipped => {
                if let Some(jump_to_node) = exec_result.jump_to_node {
                    instance.current_node = jump_to_node.clone();
                    instance.nodes[node_index].next_node = Some(jump_to_node);
                    LoopAction::Advance
                } else if let Some(next) = instance.nodes[node_index].next_node.clone() {
                    instance.current_node = next;
                    LoopAction::Advance
                } else {
                    instance.status = WorkflowInstanceStatus::Completed;
                    LoopAction::Done
                }
            }
            NodeExecutionStatus::Failed => {
                instance.status = WorkflowInstanceStatus::Failed;
                LoopAction::Done
            }
            NodeExecutionStatus::Await => {
                instance.status = WorkflowInstanceStatus::Await;
                LoopAction::Done
            }
            NodeExecutionStatus::Pending | NodeExecutionStatus::Suspended => {
                instance.status = WorkflowInstanceStatus::Suspended;
                LoopAction::Done
            }
            _ => LoopAction::Retry,
        };

        instance.updated_at = chrono::Utc::now();
        self.save_instance_and_bump_epoch(instance).await?;

        // After successful persistence, dispatch outbound events based on the transition
        self.dispatch_outbound_for_transition(instance, &old_status)
            .await;

        for job in &exec_result.dispatch_jobs {
            self.ensure_task_instance_for_job(instance, node_index, job)
                .await?;
        }

        for job in exec_result.dispatch_jobs {
            if let Err(e) = self.dispatcher.dispatch_task(job.clone()).await {
                error!(
                    task_instance_id = %job.task_instance_id,
                    error = %e,
                    "failed to dispatch task"
                );
                return Err(e.into());
            }
        }
        for job in exec_result.dispatch_workflow_jobs {
            if let Err(e) = self.dispatcher.dispatch_workflow(job.clone()).await {
                error!(
                    workflow_instance_id = %job.workflow_instance_id,
                    error = %e,
                    "failed to dispatch workflow"
                );
                return Err(e.into());
            }
        }

        Ok(action)
    }

    /// After a status transition has been persisted, compute and dispatch any outbound events
    /// (Terminated / Revived notifications to parent).
    pub(super) async fn dispatch_outbound_for_transition(
        &self,
        instance: &WorkflowInstanceEntity,
        old_status: &WorkflowInstanceStatus,
    ) {
        use crate::shared::job::{ExecuteWorkflowJob, WorkflowEvent};
        use crate::workflow::entity::transition::{ChildEventKind, TerminalStatus};

        let Some(event_kind) = should_notify_parent(old_status, &instance.status) else {
            return;
        };

        let Some(ref parent_ctx) = instance.parent_context else {
            return;
        };

        let event = match event_kind {
            ChildEventKind::Revived => WorkflowEvent::ChildRevived {
                node_id: parent_ctx.node_id.clone(),
                child_id: instance.workflow_instance_id.clone(),
            },
            ChildEventKind::Terminated(terminal) => {
                let status = match terminal {
                    TerminalStatus::Completed => NodeExecutionStatus::Success,
                    TerminalStatus::Failed => NodeExecutionStatus::Failed,
                };
                WorkflowEvent::NodeCallback {
                    node_id: parent_ctx.node_id.clone(),
                    child_task_id: instance.workflow_instance_id.clone(),
                    status,
                    output: Some(instance.context.clone()),
                    error_message: None,
                    input: None,
                }
            }
        };

        let job = ExecuteWorkflowJob {
            workflow_instance_id: parent_ctx.workflow_instance_id.clone(),
            tenant_id: instance.tenant_id.clone(),
            event,
        };

        if let Err(e) = self.dispatcher.dispatch_workflow(job).await {
            warn!(
                workflow_instance_id = %instance.workflow_instance_id,
                error = %e,
                "failed to dispatch outbound event to parent"
            );
        }
    }

    pub(super) async fn save_instance_and_bump_epoch(
        &self,
        instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<()> {
        self.workflow_instance_svc
            .save_workflow_instance(instance)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        instance.epoch += 1;
        instance.updated_at = chrono::Utc::now();
        Ok(())
    }
}
