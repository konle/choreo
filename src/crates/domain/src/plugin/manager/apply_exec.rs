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
use crate::workflow::entity::workflow_definition::{NodeExecutionStatus, WorkflowInstanceEntity};
use tracing::{error, warn};

impl PluginManager {
    fn determine_loop_action(
        instance: &mut WorkflowInstanceEntity,
        node_index: usize,
        exec_result: &ExecutionResult,
    ) -> LoopAction {
        match exec_result.status {
            NodeExecutionStatus::Success | NodeExecutionStatus::Skipped => {
                if let Some(ref jump_to_node) = exec_result.jump_to_node {
                    instance.current_node = jump_to_node.clone();
                    instance.nodes[node_index].next_node = Some(jump_to_node.clone());
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
        }
    }

    pub(super) async fn apply_exec_result(
        &self,
        instance: &mut WorkflowInstanceEntity,
        node_index: usize,
        exec_result: ExecutionResult,
    ) -> anyhow::Result<LoopAction> {
        instance.nodes[node_index].status = exec_result.status.clone();
        let old_status = instance.status.clone();
        let action = Self::determine_loop_action(instance, node_index, &exec_result);

        instance.updated_at = chrono::Utc::now();
        self.save_instance_and_bump_epoch(instance).await?;

        let node = &instance.nodes[node_index];
        match &node.status {
            NodeExecutionStatus::Success => {
                self.emit_notification(
                    "node.success",
                    Some(&instance.workflow_meta_id),
                    None,
                    super::workflow::make_node_payload(instance, node),
                );
            }
            NodeExecutionStatus::Failed => {
                self.emit_notification(
                    "node.failed",
                    Some(&instance.workflow_meta_id),
                    None,
                    super::workflow::make_node_payload(instance, node),
                );
            }
            NodeExecutionStatus::Skipped => {
                self.emit_notification(
                    "node.skipped",
                    Some(&instance.workflow_meta_id),
                    None,
                    super::workflow::make_node_payload(instance, node),
                );
            }
            _ => {}
        }

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
        use crate::shared::job::ExecuteWorkflowJob;
        use crate::workflow::entity::transition::should_notify_parent;

        let Some(event_kind) = should_notify_parent(old_status, &instance.status) else {
            return;
        };

        let Some(ref parent_ctx) = instance.parent_context else {
            return;
        };

        let event = Self::build_event_from_transition(
            &event_kind,
            parent_ctx,
            &instance.workflow_instance_id,
            &instance.context,
        );

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

    fn build_event_from_transition(
        event_kind: &crate::workflow::entity::transition::ChildEventKind,
        parent_ctx: &crate::shared::job::WorkflowCallerContext,
        child_id: &str,
        context: &serde_json::Value,
    ) -> crate::shared::job::WorkflowEvent {
        use crate::shared::job::WorkflowEvent;
        use crate::workflow::entity::transition::{ChildEventKind, TerminalStatus};

        match event_kind {
            ChildEventKind::Revived => WorkflowEvent::ChildRevived {
                node_id: parent_ctx.node_id.clone(),
                child_id: child_id.to_string(),
            },
            ChildEventKind::Terminated(terminal) => {
                let status = match terminal {
                    TerminalStatus::Completed => NodeExecutionStatus::Success,
                    TerminalStatus::Failed => NodeExecutionStatus::Failed,
                };
                WorkflowEvent::NodeCallback {
                    node_id: parent_ctx.node_id.clone(),
                    child_task_id: child_id.to_string(),
                    status,
                    output: Some(context.clone()),
                    error_message: None,
                    input: None,
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::ExecutionResult;
    use crate::shared::workflow::{TaskType, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{TaskInstanceEntity, TaskTemplate};
    use crate::workflow::entity::workflow_definition::{NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity};
    use chrono::Utc;

    fn make_exec_result(status: NodeExecutionStatus, jump_to_node: Option<String>) -> ExecutionResult {
        ExecutionResult {
            status,
            dispatch_jobs: vec![],
            dispatch_workflow_jobs: vec![],
            jump_to_node,
        }
    }

    fn make_node(node_id: &str, next_node: Option<&str>) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.into(),
            node_type: TaskType::Http,
            task_instance: TaskInstanceEntity {
                id: format!("task-{}", node_id),
                tenant_id: "t1".into(),
                task_id: "http-def".into(),
                task_name: "test".into(),
                task_type: TaskType::Http,
                task_template: TaskTemplate::Grpc,
                task_status: crate::shared::workflow::TaskInstanceStatus::Pending,
                task_instance_id: format!("task-inst-{}", node_id),
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
            next_node: next_node.map(String::from),
            status: NodeExecutionStatus::Pending,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_instance(status: WorkflowInstanceStatus, nodes: Vec<WorkflowNodeInstanceEntity>) -> WorkflowInstanceEntity {
        let now = Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: "wf-1".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "node0".into(),
            current_node: "node0".into(),
            nodes,
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 1,
            created_by: None,
        }
    }

    #[test]
    fn test_determine_loop_action_success_jump_to() {
        let node = make_node("node0", None);
        let mut instance = make_instance(WorkflowInstanceStatus::Running, vec![node]);
        let result = make_exec_result(NodeExecutionStatus::Success, Some("node3".into()));
        let action = PluginManager::determine_loop_action(&mut instance, 0, &result);
        assert_eq!(action, LoopAction::Advance);
        assert_eq!(instance.current_node, "node3");
        assert_eq!(instance.nodes[0].next_node.as_deref(), Some("node3"));
    }

    #[test]
    fn test_determine_loop_action_success_next_node() {
        let node = make_node("node0", Some("node1"));
        let mut instance = make_instance(WorkflowInstanceStatus::Running, vec![node]);
        let result = make_exec_result(NodeExecutionStatus::Success, None);
        let action = PluginManager::determine_loop_action(&mut instance, 0, &result);
        assert_eq!(action, LoopAction::Advance);
        assert_eq!(instance.current_node, "node1");
    }

    #[test]
    fn test_determine_loop_action_success_no_next() {
        let node = make_node("node0", None);
        let mut instance = make_instance(WorkflowInstanceStatus::Running, vec![node]);
        let result = make_exec_result(NodeExecutionStatus::Success, None);
        let action = PluginManager::determine_loop_action(&mut instance, 0, &result);
        assert_eq!(action, LoopAction::Done);
        assert_eq!(instance.status, WorkflowInstanceStatus::Completed);
    }

    #[test]
    fn test_determine_loop_action_failed() {
        let node = make_node("node0", None);
        let mut instance = make_instance(WorkflowInstanceStatus::Running, vec![node]);
        let result = make_exec_result(NodeExecutionStatus::Failed, None);
        let action = PluginManager::determine_loop_action(&mut instance, 0, &result);
        assert_eq!(action, LoopAction::Done);
        assert_eq!(instance.status, WorkflowInstanceStatus::Failed);
    }

    #[test]
    fn test_determine_loop_action_await() {
        let node = make_node("node0", None);
        let mut instance = make_instance(WorkflowInstanceStatus::Running, vec![node]);
        let result = make_exec_result(NodeExecutionStatus::Await, None);
        let action = PluginManager::determine_loop_action(&mut instance, 0, &result);
        assert_eq!(action, LoopAction::Done);
        assert_eq!(instance.status, WorkflowInstanceStatus::Await);
    }

    #[test]
    fn test_determine_loop_action_pending_and_suspended() {
        for s in [NodeExecutionStatus::Pending, NodeExecutionStatus::Suspended] {
            let node = make_node("node0", None);
            let mut instance = make_instance(WorkflowInstanceStatus::Running, vec![node]);
            let result = make_exec_result(s, None);
            let action = PluginManager::determine_loop_action(&mut instance, 0, &result);
            assert_eq!(action, LoopAction::Done);
            assert_eq!(instance.status, WorkflowInstanceStatus::Suspended);
        }
    }

    #[test]
    fn test_determine_loop_action_retry() {
        let node = make_node("node0", None);
        let mut instance = make_instance(WorkflowInstanceStatus::Running, vec![node]);
        let result = make_exec_result(NodeExecutionStatus::Running, None);
        let action = PluginManager::determine_loop_action(&mut instance, 0, &result);
        assert_eq!(action, LoopAction::Retry);
    }

    #[test]
    fn test_determine_loop_action_skipped_jump_to() {
        let node = make_node("node0", None);
        let mut instance = make_instance(WorkflowInstanceStatus::Running, vec![node]);
        let result = make_exec_result(NodeExecutionStatus::Skipped, Some("node5".into()));
        let action = PluginManager::determine_loop_action(&mut instance, 0, &result);
        assert_eq!(action, LoopAction::Advance);
        assert_eq!(instance.current_node, "node5");
    }
}
