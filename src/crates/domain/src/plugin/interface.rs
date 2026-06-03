use crate::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob};
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::TaskTemplate;
use crate::workflow::entity::workflow_definition::{
    NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};
use async_trait::async_trait;
use serde_json::{Value as JsonValue, json};

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub status: NodeExecutionStatus,
    pub dispatch_jobs: Vec<ExecuteTaskJob>,
    pub dispatch_workflow_jobs: Vec<ExecuteWorkflowJob>,
    pub jump_to_node: Option<String>,
}

impl ExecutionResult {
    pub fn success(jump_to_node: Option<String>) -> Self {
        Self {
            status: NodeExecutionStatus::Success,
            dispatch_jobs: vec![],
            dispatch_workflow_jobs: vec![],
            jump_to_node,
        }
    }

    pub fn failed() -> Self {
        Self {
            status: NodeExecutionStatus::Failed,
            dispatch_jobs: vec![],
            dispatch_workflow_jobs: vec![],
            jump_to_node: None,
        }
    }

    pub fn suspended() -> Self {
        Self {
            status: NodeExecutionStatus::Suspended,
            dispatch_jobs: vec![],
            dispatch_workflow_jobs: vec![],
            jump_to_node: None,
        }
    }

    pub fn pending() -> Self {
        Self {
            status: NodeExecutionStatus::Pending,
            dispatch_jobs: vec![],
            dispatch_workflow_jobs: vec![],
            jump_to_node: None,
        }
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
        Self {
            status: NodeExecutionStatus::Await,
            dispatch_jobs: vec![job],
            dispatch_workflow_jobs: vec![],
            jump_to_node: None,
        }
    }

    pub fn async_dispatch_multiple(jobs: Vec<ExecuteTaskJob>) -> Self {
        Self {
            status: NodeExecutionStatus::Await,
            dispatch_jobs: jobs,
            dispatch_workflow_jobs: vec![],
            jump_to_node: None,
        }
    }

    pub fn async_dispatch_workflow(job: ExecuteWorkflowJob) -> Self {
        Self {
            status: NodeExecutionStatus::Await,
            dispatch_jobs: vec![],
            dispatch_workflow_jobs: vec![job],
            jump_to_node: None,
        }
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
        true
    }

    /// Resolve the real-time status of a child task instance.
    /// Routes to the correct storage (task_instances or workflow_instances) based on task_template type.
    /// Returns ChildStatus indicating the child's current state.
    async fn resolve_child_status(
        &self,
        child_task_instance_id: &str,
        task_template: &TaskTemplate,
    ) -> ChildStatus {
        let _ = (child_task_instance_id, task_template);
        ChildStatus::NotFound
    }
}

/// Status of a child task resolved from storage (the single source of truth).
#[derive(Debug, Clone)]
pub enum ChildStatus {
    /// Child completed successfully. Carries output if available.
    Completed(Option<JsonValue>),
    /// Child failed. Carries output and error_message if available.
    Failed(Option<JsonValue>, Option<String>),
    /// Child was skipped. Carries output if available.
    Skipped(Option<JsonValue>),
    /// Child is currently running (Pending/Running in storage).
    Running,
    /// Child instance not found in storage (not yet created / dispatched).
    NotFound,
}

/// Gathered status of all children in a container plugin (ForkJoin / Parallel).
#[derive(Debug, Default)]
pub struct ContainerGatherResult {
    pub completed_count: u64,
    pub failed_count: u64,
    pub skipped_count: u64,
    pub running_count: u64,
    pub not_found_count: u64,
    pub results_map: serde_json::Map<String, JsonValue>,
}

impl ContainerGatherResult {
    pub fn terminal_count(&self) -> u64 {
        self.completed_count + self.failed_count
    }

    pub fn record(&mut self, key: String, status: ChildStatus) {
        match status {
            ChildStatus::Completed(output) => {
                self.completed_count += 1;
                self.results_map
                    .insert(key, json!({ "status": "Success", "output": output }));
            }
            ChildStatus::Failed(output, error) => {
                self.failed_count += 1;
                self.results_map.insert(
                    key,
                    json!({ "status": "Failed", "output": output, "error": error }),
                );
            }
            ChildStatus::Skipped(output) => {
                self.skipped_count += 1;
                self.completed_count += 1;
                self.results_map
                    .insert(key, json!({ "status": "Skipped", "output": output }));
            }
            ChildStatus::Running => {
                self.running_count += 1;
                self.results_map.insert(key, JsonValue::Null);
            }
            ChildStatus::NotFound => {
                self.not_found_count += 1;
                self.results_map.insert(key, JsonValue::Null);
            }
        }
    }
}

/// Decision returned by `diagnose` for container plugins.
#[derive(Debug)]
pub enum ContainerDecision {
    AllDone(ContainerOutcome),
    AllDispatched,
    NeedDispatch,
    EarlyAbort,
}

#[derive(Debug)]
pub enum ContainerOutcome {
    Success,
    Failed,
}

/// Whether the container should abort early due to max_failures threshold.
pub fn should_abort(max_failures: Option<u32>, failed_count: u64) -> bool {
    match max_failures {
        Some(0) => failed_count > 0,
        Some(max) => failed_count >= max as u64,
        None => false,
    }
}
