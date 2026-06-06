use crate::error::ApiError;
use application::auth::context::AuthContext;
use crate::middleware::permission::require_permission;
use crate::response::response::Response;
use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    middleware::from_fn,
    routing::{get, post, put},
};
use domain::shared::workflow::{TaskStatus, TaskType};
use domain::task::entity::task_definition::{TaskEntity, TaskTemplate};
use domain::task::service::{CreateTaskCommand, TaskService, UpdateTaskCommand};
use domain::user::entity::Permission;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct CreateTaskRequest {
    pub name: String,
    pub task_type: TaskType,
    pub task_template: TaskTemplate,
    pub description: String,
    pub status: TaskStatus,
}

impl From<CreateTaskRequest> for CreateTaskCommand {
    fn from(req: CreateTaskRequest) -> Self {
        Self {
            name: req.name,
            task_type: req.task_type,
            task_template: req.task_template,
            description: req.description,
            status: req.status,
        }
    }
}

#[derive(Deserialize)]
pub struct UpdateTaskRequest {
    pub name: String,
    pub task_type: TaskType,
    pub task_template: TaskTemplate,
    pub description: String,
    pub status: TaskStatus,
}

impl From<UpdateTaskRequest> for UpdateTaskCommand {
    fn from(req: UpdateTaskRequest) -> Self {
        Self {
            name: req.name,
            task_type: req.task_type,
            task_template: req.task_template,
            description: req.description,
            status: req.status,
        }
    }
}

#[derive(Clone)]
pub struct TaskHandler {
    service: TaskService,
}

impl TaskHandler {
    pub fn new(service: TaskService) -> Self {
        Self { service }
    }
}

pub fn routes(handler: Arc<TaskHandler>) -> Router {
    let reads = Router::new()
        .route("/", get(list_tasks))
        .route("/{id}", get(get_task));

    let writes = Router::new()
        .route("/", post(create_task))
        .route("/{id}", put(update_task).delete(delete_task))
        .layer(from_fn(require_permission(Permission::TemplateWrite)));

    Router::new().merge(reads).merge(writes).with_state(handler)
}

async fn create_task(
    State(handler): State<Arc<TaskHandler>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<Json<Response<TaskEntity>>, ApiError> {
    let result = handler
        .service
        .create_task(auth.tenant_id, req.into())
        .await?;
    Ok(Json(Response::success(result)))
}

async fn list_tasks(
    State(handler): State<Arc<TaskHandler>>,
    Extension(auth): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Response<Vec<TaskEntity>>>, ApiError> {
    let result = if let Some(task_type) = params.get("task_type") {
        handler
            .service
            .list_task_entities_by_type(&auth.tenant_id, task_type)
            .await?
    } else {
        handler.service.list_task_entities(&auth.tenant_id).await?
    };
    Ok(Json(Response::success(result)))
}

async fn get_task(
    State(handler): State<Arc<TaskHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<TaskEntity>>, ApiError> {
    let result = handler
        .service
        .get_task_entity_scoped(&auth.tenant_id, &id)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn update_task(
    State(handler): State<Arc<TaskHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateTaskRequest>,
) -> Result<Json<Response<TaskEntity>>, ApiError> {
    let result = handler
        .service
        .update_task(&auth.tenant_id, &id, req.into())
        .await?;
    Ok(Json(Response::success(result)))
}

async fn delete_task(
    State(handler): State<Arc<TaskHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<()>>, ApiError> {
    handler
        .service
        .delete_task_entity(&auth.tenant_id, &id)
        .await?;
    Ok(Json(Response::success(())))
}
