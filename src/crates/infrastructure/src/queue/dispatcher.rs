use apalis::prelude::*;
use apalis_redis::RedisStorage;
use async_trait::async_trait;
use domain::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob, TaskDispatcher};

pub struct ApalisDispatcher {
    task_storage: RedisStorage<ExecuteTaskJob>,
    workflow_storage: RedisStorage<ExecuteWorkflowJob>,
}

impl ApalisDispatcher {
    pub fn new(
        task_storage: RedisStorage<ExecuteTaskJob>,
        workflow_storage: RedisStorage<ExecuteWorkflowJob>,
    ) -> Self {
        Self {
            task_storage,
            workflow_storage,
        }
    }
}

#[async_trait]
impl TaskDispatcher for ApalisDispatcher {
    async fn dispatch_task(&self, job: ExecuteTaskJob) -> anyhow::Result<()> {
        let mut storage = self.task_storage.clone();
        storage
            .push(job)
            .await
            .map_err(|e| anyhow::anyhow!("failed to push task: {}", e))?;
        Ok(())
    }

    async fn dispatch_workflow(&self, job: ExecuteWorkflowJob) -> anyhow::Result<()> {
        let mut storage = self.workflow_storage.clone();
        storage
            .push(job)
            .await
            .map_err(|e| anyhow::anyhow!("failed to push workflow: {}", e))?;
        Ok(())
    }
}
