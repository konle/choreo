use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkflowStatus {
    Draft,
    Published,
    Deleted,
    Archived,
}
impl Display for WorkflowStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl WorkflowStatus {
    /// State machine transition rules:
    ///   Draft   -> Published
    ///   Published -> Archived
    ///   Archived -> Deleted
    pub fn can_transition_to(&self, target: &WorkflowStatus) -> bool {
        matches!(
            (self, target),
            (WorkflowStatus::Draft, WorkflowStatus::Published)
                | (WorkflowStatus::Published, WorkflowStatus::Archived)
                | (WorkflowStatus::Archived, WorkflowStatus::Deleted)
        )
    }
}

// 工作流实例状态枚举 用于表示工作流实例的状态 如待执行、执行中、已完成、失败
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowInstanceStatus {
    Pending,
    Running,
    Await,
    Completed,
    Failed,
    Canceled,
    Suspended,
}
impl Display for WorkflowInstanceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl WorkflowInstanceStatus {
    /// State machine transition rules:
    ///   Pending   -> Running
    ///   Running   -> Completed | Failed | Suspended | Await
    ///   Failed    -> Pending (retry) | Canceled | Await (container child skip)
    ///   Suspended -> Pending (resume) | Canceled
    ///   Completed -> (terminal)
    ///   Canceled  -> (terminal)
    ///   Await     -> Pending
    pub fn can_transition_to(&self, target: &WorkflowInstanceStatus) -> bool {
        matches!(
            (self, target),
            (
                WorkflowInstanceStatus::Pending,
                WorkflowInstanceStatus::Running
            ) | (
                WorkflowInstanceStatus::Running,
                WorkflowInstanceStatus::Completed
            ) | (
                WorkflowInstanceStatus::Running,
                WorkflowInstanceStatus::Failed
            ) | (
                WorkflowInstanceStatus::Running,
                WorkflowInstanceStatus::Suspended
            ) | (
                WorkflowInstanceStatus::Running,
                WorkflowInstanceStatus::Await
            ) | (
                WorkflowInstanceStatus::Failed,
                WorkflowInstanceStatus::Pending
            ) | (
                WorkflowInstanceStatus::Failed,
                WorkflowInstanceStatus::Canceled
            ) | (
                WorkflowInstanceStatus::Failed,
                WorkflowInstanceStatus::Await
            ) | (
                WorkflowInstanceStatus::Suspended,
                WorkflowInstanceStatus::Pending
            ) | (
                WorkflowInstanceStatus::Suspended,
                WorkflowInstanceStatus::Canceled
            ) | (
                WorkflowInstanceStatus::Await,
                WorkflowInstanceStatus::Pending
            )
        )
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WorkflowInstanceStatus::Completed | WorkflowInstanceStatus::Canceled
        )
    }
}

// 任务状态枚举 用于表示任务模板的状态 如草稿和已发布状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Draft,
    Published,
}
impl Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// 任务实例状态枚举 用于表示任务实例的状态 如待执行、执行中、已完成、失败
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskInstanceStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Canceled,
    Skipped,
}
impl Display for TaskInstanceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TaskInstanceStatus {
    /// State machine transition rules:
    ///   Pending   -> Running | Canceled
    ///   Running   -> Completed | Failed
    ///   Failed    -> Pending (retry) | Canceled | Skipped
    ///   Completed -> (terminal)
    ///   Canceled  -> (terminal)
    pub fn can_transition_to(&self, target: &TaskInstanceStatus) -> bool {
        matches!(
            (self, target),
            (TaskInstanceStatus::Pending, TaskInstanceStatus::Running)
                | (TaskInstanceStatus::Pending, TaskInstanceStatus::Canceled)
                | (TaskInstanceStatus::Running, TaskInstanceStatus::Completed)
                | (TaskInstanceStatus::Running, TaskInstanceStatus::Failed)
                | (TaskInstanceStatus::Failed, TaskInstanceStatus::Pending)
                | (TaskInstanceStatus::Failed, TaskInstanceStatus::Canceled)
                | (TaskInstanceStatus::Failed, TaskInstanceStatus::Skipped)
        )
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskInstanceStatus::Completed
                | TaskInstanceStatus::Canceled
                | TaskInstanceStatus::Skipped
        )
    }
}
// 任务类型枚举 用于表示任务的类型 如http、grpc、审批等
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskType {
    Http,
    IfCondition,
    ContextRewrite,
    Parallel,
    ForkJoin,
    SubWorkflow,
    Grpc,
    Approval,
    Pause,
    Llm,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_to_await_transition_allowed() {
        assert!(WorkflowInstanceStatus::Failed.can_transition_to(&WorkflowInstanceStatus::Await));
    }

    #[test]
    fn await_to_pending_still_allowed() {
        assert!(WorkflowInstanceStatus::Await.can_transition_to(&WorkflowInstanceStatus::Pending));
    }

    #[test]
    fn completed_to_await_forbidden() {
        assert!(
            !WorkflowInstanceStatus::Completed.can_transition_to(&WorkflowInstanceStatus::Await)
        );
    }

    #[test]
    fn suspended_to_await_forbidden() {
        assert!(
            !WorkflowInstanceStatus::Suspended.can_transition_to(&WorkflowInstanceStatus::Await)
        );
    }
}
