use apalis_redis::RedisStorage;
use domain::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob};

pub async fn create_workflow_storage(redis_url: &str) -> RedisStorage<ExecuteWorkflowJob> {
    let conn = apalis_redis::connect(redis_url)
        .await
        .expect("Failed to connect to Redis for workflow queue");
    RedisStorage::new(conn)
}

pub async fn create_task_storage(redis_url: &str) -> RedisStorage<ExecuteTaskJob> {
    let conn = apalis_redis::connect(redis_url)
        .await
        .expect("Failed to connect to Redis for task queue");
    RedisStorage::new(conn)
}
