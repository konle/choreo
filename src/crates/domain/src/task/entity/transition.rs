use crate::shared::job::{ExecuteWorkflowJob, WorkflowEvent};
use crate::shared::workflow::TaskInstanceStatus;
use crate::task::entity::task_definition::TaskInstanceEntity;
use crate::workflow::entity::workflow_definition::NodeExecutionStatus;

/// Describes what kind of notification to send to the parent workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskChildEventKind {
    Terminated(TaskTerminalStatus),
    Revived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskTerminalStatus {
    Completed,
    Failed,
}

/// An outbound event produced by a task status transition.
#[derive(Debug, Clone)]
pub struct TaskOutboundEvent {
    pub target_workflow_instance_id: String,
    pub target_tenant_id: String,
    pub event: WorkflowEvent,
}

/// Result of a task transition_status call.
#[derive(Debug, Clone)]
pub struct TaskTransitionResult {
    pub old_status: TaskInstanceStatus,
    pub new_status: TaskInstanceStatus,
    pub outbound_events: Vec<TaskOutboundEvent>,
}

impl TaskTransitionResult {
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

/// Pure function: given a task instance status transition (old → new),
/// determine whether the parent workflow should be notified.
pub fn should_notify_parent_task(
    old: &TaskInstanceStatus,
    new: &TaskInstanceStatus,
) -> Option<TaskChildEventKind> {
    match (old, new) {
        (TaskInstanceStatus::Running, TaskInstanceStatus::Completed) => {
            Some(TaskChildEventKind::Terminated(TaskTerminalStatus::Completed))
        }
        (TaskInstanceStatus::Running, TaskInstanceStatus::Failed) => {
            Some(TaskChildEventKind::Terminated(TaskTerminalStatus::Failed))
        }
        (TaskInstanceStatus::Failed, TaskInstanceStatus::Pending) => {
            Some(TaskChildEventKind::Revived)
        }
        _ => None,
    }
}

impl TaskInstanceEntity {
    /// Unified status transition for task instances.
    /// Validates the transition and computes outbound events.
    ///
    /// The caller is responsible for persisting and dispatching.
    pub fn transition_status(
        &mut self,
        new_status: TaskInstanceStatus,
    ) -> Result<TaskTransitionResult, String> {
        if !self.task_status.can_transition_to(&new_status) {
            return Err(format!(
                "invalid task instance state transition: {:?} -> {:?}",
                self.task_status, new_status
            ));
        }

        let old_status = self.task_status.clone();
        self.task_status = new_status.clone();
        self.updated_at = chrono::Utc::now();

        let mut outbound_events = Vec::new();

        if let Some(event_kind) = should_notify_parent_task(&old_status, &new_status) {
            if let Some(ref caller_ctx) = self.caller_context {
                let event = match event_kind {
                    TaskChildEventKind::Revived => WorkflowEvent::ChildRevived {
                        node_id: caller_ctx.node_id.clone(),
                        child_id: self.task_instance_id.clone(),
                    },
                    TaskChildEventKind::Terminated(terminal) => {
                        let status = match terminal {
                            TaskTerminalStatus::Completed => NodeExecutionStatus::Success,
                            TaskTerminalStatus::Failed => NodeExecutionStatus::Failed,
                        };
                        WorkflowEvent::NodeCallback {
                            node_id: caller_ctx.node_id.clone(),
                            child_task_id: self.task_instance_id.clone(),
                            status,
                            output: self.output.clone(),
                            error_message: self.error_message.clone(),
                            input: self.input.clone(),
                        }
                    }
                };

                outbound_events.push(TaskOutboundEvent {
                    target_workflow_instance_id: caller_ctx.workflow_instance_id.clone(),
                    target_tenant_id: self.tenant_id.clone(),
                    event,
                });
            }
        }

        Ok(TaskTransitionResult {
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
    use crate::shared::workflow::TaskType;
    use crate::task::entity::task_definition::{TaskInstanceEntity, TaskTemplate};
    use chrono::Utc;

    fn make_task_instance(
        status: TaskInstanceStatus,
        caller_context: Option<WorkflowCallerContext>,
    ) -> TaskInstanceEntity {
        let now = Utc::now();
        TaskInstanceEntity {
            id: "task-1".into(),
            tenant_id: "t1".into(),
            task_id: "def-1".into(),
            task_name: "http_call".into(),
            task_type: TaskType::Http,
            task_template: TaskTemplate::Grpc,
            task_status: status,
            task_instance_id: "task-inst-1".into(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
            input: Some(serde_json::json!({"url": "http://example.com"})),
            output: Some(serde_json::json!({"result": "ok"})),
            error_message: None,
            execution_duration: None,
            caller_context,
        }
    }

    fn caller_ctx() -> WorkflowCallerContext {
        WorkflowCallerContext {
            workflow_instance_id: "parent-wf-1".into(),
            node_id: "http_node".into(),
            parent_task_instance_id: None,
            item_index: None,
        }
    }

    #[test]
    fn test_should_notify_parent_task_terminated_completed() {
        assert_eq!(
            should_notify_parent_task(
                &TaskInstanceStatus::Running,
                &TaskInstanceStatus::Completed
            ),
            Some(TaskChildEventKind::Terminated(TaskTerminalStatus::Completed))
        );
    }

    #[test]
    fn test_should_notify_parent_task_terminated_failed() {
        assert_eq!(
            should_notify_parent_task(
                &TaskInstanceStatus::Running,
                &TaskInstanceStatus::Failed
            ),
            Some(TaskChildEventKind::Terminated(TaskTerminalStatus::Failed))
        );
    }

    #[test]
    fn test_should_notify_parent_task_revived() {
        assert_eq!(
            should_notify_parent_task(
                &TaskInstanceStatus::Failed,
                &TaskInstanceStatus::Pending
            ),
            Some(TaskChildEventKind::Revived)
        );
    }

    #[test]
    fn test_should_notify_parent_task_no_event_normal() {
        assert_eq!(
            should_notify_parent_task(
                &TaskInstanceStatus::Pending,
                &TaskInstanceStatus::Running
            ),
            None
        );
    }

    #[test]
    fn test_task_transition_status_with_caller_completed() {
        let mut task = make_task_instance(TaskInstanceStatus::Running, Some(caller_ctx()));

        let result = task
            .transition_status(TaskInstanceStatus::Completed)
            .unwrap();

        assert_eq!(result.old_status, TaskInstanceStatus::Running);
        assert_eq!(result.new_status, TaskInstanceStatus::Completed);
        assert_eq!(result.outbound_events.len(), 1);

        let event = &result.outbound_events[0];
        assert_eq!(event.target_workflow_instance_id, "parent-wf-1");
        assert!(matches!(
            event.event,
            WorkflowEvent::NodeCallback {
                ref node_id,
                ref child_task_id,
                ref status,
                ..
            } if node_id == "http_node"
                && child_task_id == "task-inst-1"
                && *status == NodeExecutionStatus::Success
        ));
    }

    #[test]
    fn test_task_transition_status_with_caller_failed() {
        let mut task = make_task_instance(TaskInstanceStatus::Running, Some(caller_ctx()));
        task.error_message = Some("timeout".into());

        let result = task
            .transition_status(TaskInstanceStatus::Failed)
            .unwrap();

        assert_eq!(result.outbound_events.len(), 1);
        let event = &result.outbound_events[0];
        assert!(matches!(
            event.event,
            WorkflowEvent::NodeCallback {
                ref status,
                ref error_message,
                ..
            } if *status == NodeExecutionStatus::Failed
                && *error_message == Some("timeout".into())
        ));
    }

    #[test]
    fn test_task_transition_status_without_caller_no_event() {
        let mut task = make_task_instance(TaskInstanceStatus::Running, None);

        let result = task
            .transition_status(TaskInstanceStatus::Completed)
            .unwrap();

        assert_eq!(result.outbound_events.len(), 0);
        assert_eq!(task.task_status, TaskInstanceStatus::Completed);
    }

    #[test]
    fn test_task_transition_status_revived_with_caller() {
        let mut task = make_task_instance(TaskInstanceStatus::Failed, Some(caller_ctx()));

        let result = task
            .transition_status(TaskInstanceStatus::Pending)
            .unwrap();

        assert_eq!(result.outbound_events.len(), 1);
        let event = &result.outbound_events[0];
        assert!(matches!(
            event.event,
            WorkflowEvent::ChildRevived {
                ref node_id,
                ref child_id,
            } if node_id == "http_node" && child_id == "task-inst-1"
        ));
    }

    #[test]
    fn test_task_transition_status_invalid() {
        let mut task = make_task_instance(TaskInstanceStatus::Completed, None);

        let result = task.transition_status(TaskInstanceStatus::Running);
        assert!(result.is_err());
        assert_eq!(task.task_status, TaskInstanceStatus::Completed);
    }

    #[test]
    fn test_task_transition_dispatch_jobs() {
        let mut task = make_task_instance(TaskInstanceStatus::Running, Some(caller_ctx()));

        let result = task
            .transition_status(TaskInstanceStatus::Completed)
            .unwrap();

        let jobs = result.into_dispatch_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].workflow_instance_id, "parent-wf-1");
        assert_eq!(jobs[0].tenant_id, "t1");
    }
}
