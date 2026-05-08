use crate::shared::job::{ExecuteWorkflowJob, WorkflowEvent};
use crate::shared::workflow::WorkflowInstanceStatus;
use crate::workflow::entity::workflow_definition::WorkflowInstanceEntity;

/// Describes the kind of child event that should be sent to a parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildEventKind {
    Terminated(TerminalStatus),
    Revived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalStatus {
    Completed,
    Failed,
}

/// An outbound event that must be dispatched after a successful state transition.
#[derive(Debug, Clone)]
pub struct OutboundEvent {
    pub target_workflow_instance_id: String,
    pub target_tenant_id: String,
    pub event: WorkflowEvent,
}

/// Result of a successful `transition_status` call.
#[derive(Debug, Clone)]
pub struct StateTransitionResult {
    pub old_status: WorkflowInstanceStatus,
    pub new_status: WorkflowInstanceStatus,
    pub outbound_events: Vec<OutboundEvent>,
}

impl StateTransitionResult {
    pub fn into_dispatch_jobs(self) -> Vec<ExecuteWorkflowJob> {
        self.outbound_events
            .into_iter()
            .map(|e| ExecuteWorkflowJob {
                workflow_instance_id: e.target_workflow_instance_id,
                tenant_id: e.target_tenant_id,
                event: e.event,
            })
            .collect()
    }
}

/// Pure function: given a status transition (old → new), determine whether
/// the parent should be notified and with what event kind.
///
/// Returns `None` if no notification is needed.
pub fn should_notify_parent(
    old: &WorkflowInstanceStatus,
    new: &WorkflowInstanceStatus,
) -> Option<ChildEventKind> {
    match (old, new) {
        // Terminated events: entering a terminal-like state from Running
        (WorkflowInstanceStatus::Running, WorkflowInstanceStatus::Completed) => {
            Some(ChildEventKind::Terminated(TerminalStatus::Completed))
        }
        (WorkflowInstanceStatus::Running, WorkflowInstanceStatus::Failed) => {
            Some(ChildEventKind::Terminated(TerminalStatus::Failed))
        }
        // Revived event: leaving Failed state (recovery)
        (WorkflowInstanceStatus::Failed, WorkflowInstanceStatus::Pending) => {
            Some(ChildEventKind::Revived)
        }
        _ => None,
    }
}

impl WorkflowInstanceEntity {
    /// Unified state transition entry point.
    /// Validates the transition, updates status, and computes outbound events.
    ///
    /// The caller is responsible for persisting the instance and dispatching outbound events.
    pub fn transition_status(
        &mut self,
        new_status: WorkflowInstanceStatus,
    ) -> Result<StateTransitionResult, String> {
        if !self.status.can_transition_to(&new_status) {
            return Err(format!(
                "invalid workflow instance state transition: {:?} -> {:?}",
                self.status, new_status
            ));
        }

        let old_status = self.status.clone();
        self.status = new_status.clone();
        self.updated_at = chrono::Utc::now();

        let mut outbound_events = Vec::new();

        if let Some(event_kind) = should_notify_parent(&old_status, &new_status) {
            if let Some(ref parent_ctx) = self.parent_context {
                let event = match event_kind {
                    ChildEventKind::Revived => WorkflowEvent::ChildRevived {
                        node_id: parent_ctx.node_id.clone(),
                        child_id: self.workflow_instance_id.clone(),
                    },
                    ChildEventKind::Terminated(terminal) => {
                        let status = match terminal {
                            TerminalStatus::Completed => {
                                crate::workflow::entity::workflow_definition::NodeExecutionStatus::Success
                            }
                            TerminalStatus::Failed => {
                                crate::workflow::entity::workflow_definition::NodeExecutionStatus::Failed
                            }
                        };
                        WorkflowEvent::NodeCallback {
                            node_id: parent_ctx.node_id.clone(),
                            child_task_id: self.workflow_instance_id.clone(),
                            status,
                            output: Some(self.context.clone()),
                            error_message: None,
                            input: None,
                        }
                    }
                };

                outbound_events.push(OutboundEvent {
                    target_workflow_instance_id: parent_ctx.workflow_instance_id.clone(),
                    target_tenant_id: self.tenant_id.clone(),
                    event,
                });
            }
        }

        Ok(StateTransitionResult {
            old_status,
            new_status,
            outbound_events,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::job::WorkflowCallerContext;
    use chrono::Utc;

    fn make_instance(
        status: WorkflowInstanceStatus,
        parent_context: Option<WorkflowCallerContext>,
    ) -> WorkflowInstanceEntity {
        let now = Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: "child-wf-1".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "start".into(),
            current_node: "node1".into(),
            nodes: vec![],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context,
            depth: 1,
            created_by: None,
        }
    }

    fn parent_ctx() -> WorkflowCallerContext {
        WorkflowCallerContext {
            workflow_instance_id: "parent-wf-1".into(),
            node_id: "subwf_node".into(),
            parent_task_instance_id: None,
            item_index: None,
        }
    }

    #[test]
    fn test_should_notify_parent_revived() {
        assert_eq!(
            should_notify_parent(
                &WorkflowInstanceStatus::Failed,
                &WorkflowInstanceStatus::Pending
            ),
            Some(ChildEventKind::Revived)
        );
    }

    #[test]
    fn test_should_notify_parent_terminated_completed() {
        assert_eq!(
            should_notify_parent(
                &WorkflowInstanceStatus::Running,
                &WorkflowInstanceStatus::Completed
            ),
            Some(ChildEventKind::Terminated(TerminalStatus::Completed))
        );
    }

    #[test]
    fn test_should_notify_parent_terminated_failed() {
        assert_eq!(
            should_notify_parent(
                &WorkflowInstanceStatus::Running,
                &WorkflowInstanceStatus::Failed
            ),
            Some(ChildEventKind::Terminated(TerminalStatus::Failed))
        );
    }

    #[test]
    fn test_should_notify_parent_no_event_for_normal_transitions() {
        assert_eq!(
            should_notify_parent(
                &WorkflowInstanceStatus::Pending,
                &WorkflowInstanceStatus::Running
            ),
            None
        );
        assert_eq!(
            should_notify_parent(
                &WorkflowInstanceStatus::Running,
                &WorkflowInstanceStatus::Await
            ),
            None
        );
        assert_eq!(
            should_notify_parent(
                &WorkflowInstanceStatus::Await,
                &WorkflowInstanceStatus::Pending
            ),
            None
        );
        assert_eq!(
            should_notify_parent(
                &WorkflowInstanceStatus::Running,
                &WorkflowInstanceStatus::Suspended
            ),
            None
        );
        assert_eq!(
            should_notify_parent(
                &WorkflowInstanceStatus::Suspended,
                &WorkflowInstanceStatus::Pending
            ),
            None
        );
    }

    #[test]
    fn test_transition_status_with_parent_context_revived() {
        let mut instance =
            make_instance(WorkflowInstanceStatus::Failed, Some(parent_ctx()));

        let result = instance
            .transition_status(WorkflowInstanceStatus::Pending)
            .unwrap();

        assert_eq!(result.old_status, WorkflowInstanceStatus::Failed);
        assert_eq!(result.new_status, WorkflowInstanceStatus::Pending);
        assert_eq!(result.outbound_events.len(), 1);

        let event = &result.outbound_events[0];
        assert_eq!(event.target_workflow_instance_id, "parent-wf-1");
        assert_eq!(event.target_tenant_id, "t1");
        assert!(matches!(
            event.event,
            WorkflowEvent::ChildRevived {
                ref node_id,
                ref child_id
            } if node_id == "subwf_node" && child_id == "child-wf-1"
        ));

        assert_eq!(instance.status, WorkflowInstanceStatus::Pending);
    }

    #[test]
    fn test_transition_status_without_parent_context_no_event() {
        let mut instance = make_instance(WorkflowInstanceStatus::Failed, None);

        let result = instance
            .transition_status(WorkflowInstanceStatus::Pending)
            .unwrap();

        assert_eq!(result.outbound_events.len(), 0);
        assert_eq!(instance.status, WorkflowInstanceStatus::Pending);
    }

    #[test]
    fn test_transition_status_invalid_transition() {
        let mut instance = make_instance(WorkflowInstanceStatus::Completed, None);

        let result = instance.transition_status(WorkflowInstanceStatus::Running);
        assert!(result.is_err());
        // Status should not change on error
        assert_eq!(instance.status, WorkflowInstanceStatus::Completed);
    }

    #[test]
    fn test_transition_status_pending_to_running_no_event() {
        let mut instance =
            make_instance(WorkflowInstanceStatus::Pending, Some(parent_ctx()));

        let result = instance
            .transition_status(WorkflowInstanceStatus::Running)
            .unwrap();

        assert_eq!(result.outbound_events.len(), 0);
        assert_eq!(instance.status, WorkflowInstanceStatus::Running);
    }

    #[test]
    fn test_transition_status_running_to_completed_emits_terminated() {
        let mut instance =
            make_instance(WorkflowInstanceStatus::Running, Some(parent_ctx()));

        let result = instance
            .transition_status(WorkflowInstanceStatus::Completed)
            .unwrap();

        assert_eq!(result.outbound_events.len(), 1);
        let event = &result.outbound_events[0];
        assert_eq!(event.target_workflow_instance_id, "parent-wf-1");
        assert!(matches!(
            event.event,
            WorkflowEvent::NodeCallback { ref node_id, ref status, .. }
            if node_id == "subwf_node" && *status == crate::workflow::entity::workflow_definition::NodeExecutionStatus::Success
        ));
        assert_eq!(instance.status, WorkflowInstanceStatus::Completed);
    }

    #[test]
    fn test_transition_status_running_to_failed_emits_terminated() {
        let mut instance =
            make_instance(WorkflowInstanceStatus::Running, Some(parent_ctx()));

        let result = instance
            .transition_status(WorkflowInstanceStatus::Failed)
            .unwrap();

        assert_eq!(result.outbound_events.len(), 1);
        let event = &result.outbound_events[0];
        assert_eq!(event.target_workflow_instance_id, "parent-wf-1");
        assert!(matches!(
            event.event,
            WorkflowEvent::NodeCallback { ref node_id, ref status, .. }
            if node_id == "subwf_node" && *status == crate::workflow::entity::workflow_definition::NodeExecutionStatus::Failed
        ));
        assert_eq!(instance.status, WorkflowInstanceStatus::Failed);
    }

    #[test]
    fn test_transition_status_running_to_completed_no_parent_no_event() {
        let mut instance =
            make_instance(WorkflowInstanceStatus::Running, None);

        let result = instance
            .transition_status(WorkflowInstanceStatus::Completed)
            .unwrap();

        assert_eq!(result.outbound_events.len(), 0);
        assert_eq!(instance.status, WorkflowInstanceStatus::Completed);
    }

    #[test]
    fn test_transition_dispatch_jobs() {
        let mut instance =
            make_instance(WorkflowInstanceStatus::Failed, Some(parent_ctx()));

        let result = instance
            .transition_status(WorkflowInstanceStatus::Pending)
            .unwrap();

        let jobs = result.into_dispatch_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].workflow_instance_id, "parent-wf-1");
        assert_eq!(jobs[0].tenant_id, "t1");
        assert!(matches!(
            jobs[0].event,
            WorkflowEvent::ChildRevived { .. }
        ));
    }
}
