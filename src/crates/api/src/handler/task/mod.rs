pub mod task_handler;
pub mod task_instance_handler;
pub mod task_instance_request;
use axum::Router;
use std::sync::Arc;

pub use task_handler::TaskHandler;
pub use task_instance_handler::TaskInstanceHandler;

pub fn routes(
    task_handler: Arc<TaskHandler>,
    task_instance_handler: Arc<TaskInstanceHandler>,
) -> Router {
    Router::new()
        .merge(task_handler::routes(task_handler))
        .nest(
            "/instance",
            task_instance_handler::routes(task_instance_handler),
        )
}
