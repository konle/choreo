use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::TaskInstanceEntity;
use crate::task::interface::{TaskExecutionResult, TaskExecutor};
use crate::task::service::TaskInstanceService;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

pub struct TaskManager {
    executors: HashMap<TaskType, Box<dyn TaskExecutor>>,
    task_instance_svc: Arc<TaskInstanceService>,
}

impl TaskManager {
    pub fn new(task_instance_svc: Arc<TaskInstanceService>) -> Self {
        Self {
            executors: HashMap::new(),
            task_instance_svc,
        }
    }

    pub fn task_instance_svc(&self) -> &TaskInstanceService {
        &self.task_instance_svc
    }

    pub fn register(&mut self, executor: Box<dyn TaskExecutor>) {
        let task_type = executor.task_type();
        info!(task_type = ?task_type, "registered task executor");
        self.executors.insert(task_type, executor);
    }

    pub async fn execute_task(
        &self,
        task_instance: &TaskInstanceEntity,
    ) -> anyhow::Result<TaskExecutionResult> {
        let executor = self
            .executors
            .get(&task_instance.task_type)
            .ok_or_else(|| {
                error!(
                    task_type = ?task_instance.task_type,
                    task_instance_id = %task_instance.task_instance_id,
                    "no executor registered for task type"
                );
                anyhow::anyhow!(
                    "no task executor registered for task type: {:?}",
                    task_instance.task_type
                )
            })?;

        executor.execute_task(task_instance).await
    }
}
