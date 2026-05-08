use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowEvent {
    Start,
    NodeCallback {
        node_id: String,
        child_task_id: String,
        status: crate::workflow::entity::workflow_definition::NodeExecutionStatus,
        output: Option<serde_json::Value>,
        error_message: Option<String>,
        input: Option<serde_json::Value>,
    },
    ChildRevived {
        node_id: String,
        child_id: String,
    },
    RetryContainerChild {
        node_id: String,
        child_task_id: String,
    },
}

impl Default for WorkflowEvent {
    fn default() -> Self {
        WorkflowEvent::Start
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteWorkflowJob {
    pub workflow_instance_id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub event: WorkflowEvent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowCallerContext {
    pub workflow_instance_id: String,
    pub node_id: String,
    pub parent_task_instance_id: Option<String>,
    pub item_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteTaskJob {
    pub task_instance_id: String,
    pub tenant_id: String,
    pub caller_context: Option<WorkflowCallerContext>,
}

#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    async fn dispatch_task(&self, job: ExecuteTaskJob) -> anyhow::Result<()>;
    async fn dispatch_workflow(&self, job: ExecuteWorkflowJob) -> anyhow::Result<()>;
}
