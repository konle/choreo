use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::TaskInstanceEntity;
use crate::workflow::entity::workflow_definition::NodeExecutionStatus;
use async_trait::async_trait;

pub struct TaskExecutionResult {
    pub status: NodeExecutionStatus,
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error_message: Option<String>,
}

#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// 执行任务的具体逻辑
    async fn execute_task(
        &self,
        task_instance: &TaskInstanceEntity,
    ) -> anyhow::Result<TaskExecutionResult>;

    /// 获取该执行器支持的任务类型
    fn task_type(&self) -> TaskType;
}
