use apalis::prelude::*;
use apalis_redis::RedisStorage;
use domain::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob};

#[derive(Clone)]
pub struct JobProducer {
    workflow_storage: RedisStorage<ExecuteWorkflowJob>,
    task_storage: RedisStorage<ExecuteTaskJob>,
}

impl JobProducer {
    pub fn new(
        workflow_storage: RedisStorage<ExecuteWorkflowJob>,
        task_storage: RedisStorage<ExecuteTaskJob>,
    ) -> Self {
        Self {
            workflow_storage,
            task_storage,
        }
    }

    pub async fn enqueue_workflow(
        &mut self,
        job: ExecuteWorkflowJob,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.workflow_storage.push(job).await?;
        Ok(())
    }

    pub async fn enqueue_task(
        &mut self,
        job: ExecuteTaskJob,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.task_storage.push(job).await?;
        Ok(())
    }

    pub async fn connect(redis_url: &str) -> Self {
        let wf_conn = apalis_redis::connect(redis_url)
            .await
            .expect("Failed to connect to Redis");
        let task_conn = apalis_redis::connect(redis_url)
            .await
            .expect("Failed to connect to Redis");
        Self::new(RedisStorage::new(wf_conn), RedisStorage::new(task_conn))
    }
}
