use crate::middleware::auth::AuthContext;
use crate::middleware::permission::require_permission;
use crate::response::response::Response;
use crate::{error::ApiError, handler::task::task_instance_request::ListTaskInstancesRequest};
use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    middleware::from_fn,
    routing::{get, post},
};
use chrono::Utc;
use common::pagination::PaginatedData;
use domain::plugin::manager::resolved_llm_request_snapshot;
use domain::shared::workflow::TaskInstanceStatus;
use domain::task::entity::task_definition::{TaskInstanceEntity, TaskTemplate};
use domain::task::http_template_resolve::resolved_http_request_snapshot;
use domain::task::service::{TaskInstanceService, TaskService};
use domain::user::entity::Permission;
use domain::variable::service::VariableService;
use domain::{
    shared::job::{ExecuteTaskJob, TaskDispatcher},
    task::entity::query::TaskInstanceQuery,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct CreateTaskInstanceRequest {
    pub task_id: String,
    #[serde(default)]
    pub context: Option<serde_json::Value>,
}

#[derive(Clone)]
pub struct TaskInstanceHandler {
    service: TaskInstanceService,
    task_service: TaskService,
    variable_service: VariableService,
    dispatcher: Arc<dyn TaskDispatcher>,
}

impl TaskInstanceHandler {
    pub fn new(
        service: TaskInstanceService,
        task_service: TaskService,
        variable_service: VariableService,
        dispatcher: Arc<dyn TaskDispatcher>,
    ) -> Self {
        Self {
            service,
            task_service,
            variable_service,
            dispatcher,
        }
    }
}

pub fn routes(handler: Arc<TaskInstanceHandler>) -> Router {
    let reads = Router::new()
        .route("/", get(list_task_instances))
        .route("/{id}", get(get_task_instance));

    let writes = Router::new()
        .route("/", post(create_task_instance))
        .route("/{id}/execute", post(execute_task_instance))
        .route("/{id}/retry", post(retry_task_instance))
        .route("/{id}/cancel", post(cancel_task_instance))
        .layer(from_fn(require_permission(Permission::InstanceExecute)));

    Router::new().merge(reads).merge(writes).with_state(handler)
}

async fn create_task_instance(
    State(handler): State<Arc<TaskInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<CreateTaskInstanceRequest>,
) -> Result<Json<Response<TaskInstanceEntity>>, ApiError> {
    let task = handler
        .task_service
        .get_task_entity_scoped(&auth.tenant_id, &req.task_id)
        .await?;
    let user_ctx = req.context.unwrap_or_else(|| json!({}));

    let resolved_ctx = handler
        .variable_service
        .resolve_standalone_context(&auth.tenant_id, &user_ctx)
        .await
        .map_err(|e| {
            warn!(error = %e, "variable resolution failed for standalone task, using raw context");
            ApiError::internal(format!("variable resolution failed: {}", e))
        })?;

    let input = match &task.task_template {
        TaskTemplate::Http(tpl) => Some(resolved_http_request_snapshot(tpl, &resolved_ctx)),
        TaskTemplate::Llm(tpl) => Some(resolved_llm_request_snapshot(tpl, &resolved_ctx)),
        _ => None,
    };

    let now = Utc::now();
    let instance_id = Uuid::new_v4().to_string();
    let entity = TaskInstanceEntity {
        id: Uuid::new_v4().to_string(),
        tenant_id: auth.tenant_id,
        task_id: task.id.clone(),
        task_name: task.name.clone(),
        task_type: task.task_type.clone(),
        task_template: task.task_template.clone(),
        task_status: TaskInstanceStatus::Pending,
        task_instance_id: instance_id,
        created_at: now,
        updated_at: now,
        deleted_at: None,
        input,
        output: None,
        error_message: None,
        execution_duration: None,
        caller_context: None,
    };
    let result = handler.service.create_task_instance_entity(entity).await?;
    Ok(Json(Response::success(result)))
}

async fn list_task_instances(
    State(handler): State<Arc<TaskInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Query(req): Query<ListTaskInstancesRequest>,
) -> Result<Json<Response<PaginatedData<TaskInstanceEntity>>>, ApiError> {
    let mut query = TaskInstanceQuery::from(req);
    query.tenant_id = auth.tenant_id.clone();
    info!(
        "list_task_instances query: {:?} tenant_id: {}",
        query, auth.tenant_id
    );
    let result = handler.service.list_task_instance_entities(&query).await?;
    Ok(Json(Response::success(result)))
}

async fn get_task_instance(
    State(handler): State<Arc<TaskInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<TaskInstanceEntity>>, ApiError> {
    let result = handler
        .service
        .get_task_instance_entity_scoped(&auth.tenant_id, &id)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn execute_task_instance(
    State(handler): State<Arc<TaskInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<TaskInstanceEntity>>, ApiError> {
    let task_instance_entity = handler
        .service
        .get_task_instance_entity_scoped(&auth.tenant_id, &id)
        .await?;
    //let updated = handler.service.submit_instance(&id).await?;
    if task_instance_entity.task_status != TaskInstanceStatus::Pending {
        return Err(ApiError::bad_request("Task instance is not pending"));
    }

    handler
        .dispatcher
        .dispatch_task(ExecuteTaskJob {
            task_instance_id: id.clone(),
            tenant_id: auth.tenant_id,
            caller_context: None,
        })
        .await
        .map_err(|e| {
            error!(task_instance_id = %id, error = %e, "failed to dispatch task execution");
            ApiError::internal(e.to_string())
        })?;

    info!(task_instance_id = %id, "task execution dispatched");

    Ok(Json(Response::success(task_instance_entity)))
}

async fn retry_task_instance(
    State(handler): State<Arc<TaskInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<TaskInstanceEntity>>, ApiError> {
    let instance = handler
        .service
        .get_task_instance_entity_scoped(&auth.tenant_id, &id)
        .await?;
    let result = handler.service.retry_instance(&id).await?;

    if instance.caller_context.is_none() {
        handler
            .dispatcher
            .dispatch_task(ExecuteTaskJob {
                task_instance_id: result.task_instance_id.clone(),
                tenant_id: auth.tenant_id,
                caller_context: None,
            })
            .await
            .map_err(|e| {
                error!(task_instance_id = %id, error = %e, "failed to dispatch task retry");
                ApiError::internal(e.to_string())
            })?;
        info!(task_instance_id = %id, "standalone task retry dispatched");
    }

    Ok(Json(Response::success(result)))
}

async fn cancel_task_instance(
    State(handler): State<Arc<TaskInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<TaskInstanceEntity>>, ApiError> {
    handler
        .service
        .get_task_instance_entity_scoped(&auth.tenant_id, &id)
        .await?;
    let result = handler.service.cancel_instance(&id).await?;
    Ok(Json(Response::success(result)))
}
