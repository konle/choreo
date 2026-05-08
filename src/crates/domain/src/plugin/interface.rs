use async_trait::async_trait;
use crate::shared::workflow::TaskType;
use crate::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob};
use crate::workflow::entity::workflow_definition::{WorkflowNodeInstanceEntity, WorkflowInstanceEntity, NodeExecutionStatus};

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub status: NodeExecutionStatus,
    pub dispatch_jobs: Vec<ExecuteTaskJob>,
    pub dispatch_workflow_jobs: Vec<ExecuteWorkflowJob>,
    pub jump_to_node: Option<String>,
}

impl ExecutionResult {
    pub fn success(jump_to_node: Option<String>) -> Self {
        Self { status: NodeExecutionStatus::Success, dispatch_jobs: vec![], dispatch_workflow_jobs: vec![], jump_to_node }
    }

    pub fn failed() -> Self {
        Self { status: NodeExecutionStatus::Failed, dispatch_jobs: vec![], dispatch_workflow_jobs: vec![], jump_to_node: None }
    }

    pub fn suspended() -> Self {
        Self { status: NodeExecutionStatus::Suspended, dispatch_jobs: vec![], dispatch_workflow_jobs: vec![], jump_to_node: None }
    }

    pub fn pending() -> Self {
        Self { status: NodeExecutionStatus::Pending, dispatch_jobs: vec![], dispatch_workflow_jobs: vec![], jump_to_node: None }
    }

    pub fn skipped(jump_to_node: Option<String>) -> Self {
        Self {
            status: NodeExecutionStatus::Skipped,
            dispatch_jobs: vec![],
            dispatch_workflow_jobs: vec![],
            jump_to_node,
        }
    }

    pub fn async_dispatch(job: ExecuteTaskJob) -> Self {
        Self { status: NodeExecutionStatus::Await, dispatch_jobs: vec![job], dispatch_workflow_jobs: vec![], jump_to_node: None }
    }

    pub fn async_dispatch_multiple(jobs: Vec<ExecuteTaskJob>) -> Self {
        Self { status: NodeExecutionStatus::Await, dispatch_jobs: jobs, dispatch_workflow_jobs: vec![], jump_to_node: None }
    }

    pub fn async_dispatch_workflow(job: ExecuteWorkflowJob) -> Self {
        Self { status: NodeExecutionStatus::Await, dispatch_jobs: vec![], dispatch_workflow_jobs: vec![job], jump_to_node: None }
    }
}

#[async_trait]
pub trait PluginInterface: Send + Sync {
    async fn execute(
        &self,
        executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult>;

    // 新增: 处理异步回调
    async fn handle_callback(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        _workflow_instance: &mut WorkflowInstanceEntity,
        _child_task_id: &str,
        status: &NodeExecutionStatus,
        output: &Option<serde_json::Value>,
        error_message: &Option<String>,
        input: &Option<serde_json::Value>,
    ) -> anyhow::Result<ExecutionResult> {
        // 默认实现，如果是普通节点，子任务完成代表节点完成
        node_instance.error_message = error_message.clone();
        node_instance.task_instance.input = input.clone();
        node_instance.task_instance.output = output.clone();
        node_instance.task_instance.error_message = error_message.clone();
        
        match status {
            NodeExecutionStatus::Success => Ok(ExecutionResult::success(None)),
            NodeExecutionStatus::Skipped => Ok(ExecutionResult::skipped(None)),
            NodeExecutionStatus::Failed => Ok(ExecutionResult::failed()),
            _ => Ok(ExecutionResult::pending()),
        }
    }

    fn plugin_type(&self) -> TaskType;
}

#[async_trait]
pub trait PluginExecutor: Send + Sync {
    async fn execute_node_instance(
        &self,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult>;

    async fn handle_node_callback(
        &self,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
        child_task_id: &str,
        status: &NodeExecutionStatus,
        output: &Option<serde_json::Value>,
        error_message: &Option<String>,
        input: &Option<serde_json::Value>,
    ) -> anyhow::Result<ExecutionResult>;

    /// Check if a task instance is still in Failed state.
    /// Used by container plugins for Stale Failure Check.
    /// Returns true if the task is still Failed, false otherwise (retried/completed/not found).
    async fn is_task_still_failed(&self, task_instance_id: &str) -> bool {
        let _ = task_instance_id;
        true // default: assume still failed (conservative)
    }
}
