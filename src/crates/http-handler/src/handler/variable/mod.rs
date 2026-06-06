use crate::error::ApiError;
use application::auth::context::AuthContext;
use crate::middleware::permission_guard::{
    Guard, RequireMetaVariableWrite, RequireTenantVariableWrite,
};
use crate::response::response::Response;
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::{get, post},
};
use domain::variable::entity::{VariableEntity, VariableScope, VariableType};
use domain::variable::service::VariableService;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone)]
pub struct VariableHandler {
    service: VariableService,
}

impl VariableHandler {
    pub fn new(service: VariableService) -> Self {
        Self { service }
    }
}

#[derive(Deserialize)]
pub struct CreateVariableRequest {
    pub key: String,
    pub value: String,
    pub variable_type: VariableType,
    pub description: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateVariableRequest {
    pub value: String,
    pub variable_type: VariableType,
    pub description: Option<String>,
}

pub fn tenant_variable_routes(handler: Arc<VariableHandler>) -> Router {
    Router::new()
        .route("/", post(create_tenant_variable).get(list_tenant_variables))
        .route(
            "/{id}",
            get(get_tenant_variable)
                .put(update_tenant_variable)
                .delete(delete_tenant_variable),
        )
        .with_state(handler)
}

pub fn workflow_meta_variable_routes(handler: Arc<VariableHandler>) -> Router {
    Router::new()
        .route("/", post(create_meta_variable).get(list_meta_variables))
        .route(
            "/{var_id}",
            get(get_meta_variable)
                .put(update_meta_variable)
                .delete(delete_meta_variable),
        )
        .with_state(handler)
}

// ── Tenant variable handlers ──

async fn create_tenant_variable(
    _: Guard<RequireTenantVariableWrite>,
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<CreateVariableRequest>,
) -> Result<Json<Response<VariableEntity>>, ApiError> {
    let entity = VariableEntity {
        id: String::new(),
        tenant_id: auth.tenant_id.clone(),
        scope: VariableScope::Tenant,
        scope_id: auth.tenant_id,
        key: req.key,
        value: req.value,
        variable_type: req.variable_type,
        description: req.description,
        created_by: auth.user_id,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let result = handler.service.create(entity).await?;
    Ok(Json(Response::success(result)))
}

async fn list_tenant_variables(
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Response<Vec<VariableEntity>>>, ApiError> {
    let result = handler
        .service
        .list_by_scope(&auth.tenant_id, &VariableScope::Tenant, &auth.tenant_id)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn get_tenant_variable(
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<VariableEntity>>, ApiError> {
    let result = handler.service.get_by_id(&auth.tenant_id, &id).await?;
    Ok(Json(Response::success(result)))
}

async fn update_tenant_variable(
    _: Guard<RequireTenantVariableWrite>,
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateVariableRequest>,
) -> Result<Json<Response<VariableEntity>>, ApiError> {
    let existing = handler.service.get_by_id(&auth.tenant_id, &id).await?;
    let entity = VariableEntity {
        id: existing.id,
        tenant_id: existing.tenant_id,
        scope: existing.scope,
        scope_id: existing.scope_id,
        key: existing.key,
        value: req.value,
        variable_type: req.variable_type,
        description: req.description,
        created_by: existing.created_by,
        created_at: existing.created_at,
        updated_at: chrono::Utc::now(),
    };
    let result = handler.service.update(entity).await?;
    Ok(Json(Response::success(result)))
}

async fn delete_tenant_variable(
    _: Guard<RequireTenantVariableWrite>,
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<()>>, ApiError> {
    handler.service.delete(&auth.tenant_id, &id).await?;
    Ok(Json(Response::success(())))
}

// ── Workflow meta variable handlers ──

async fn create_meta_variable(
    _: Guard<RequireMetaVariableWrite>,
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(meta_id): Path<String>,
    Json(req): Json<CreateVariableRequest>,
) -> Result<Json<Response<VariableEntity>>, ApiError> {
    let entity = VariableEntity {
        id: String::new(),
        tenant_id: auth.tenant_id,
        scope: VariableScope::WorkflowMeta,
        scope_id: meta_id,
        key: req.key,
        value: req.value,
        variable_type: req.variable_type,
        description: req.description,
        created_by: auth.user_id,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let result = handler.service.create(entity).await?;
    Ok(Json(Response::success(result)))
}

async fn list_meta_variables(
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(meta_id): Path<String>,
) -> Result<Json<Response<Vec<VariableEntity>>>, ApiError> {
    let result = handler
        .service
        .list_by_scope(&auth.tenant_id, &VariableScope::WorkflowMeta, &meta_id)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn get_meta_variable(
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path((_meta_id, var_id)): Path<(String, String)>,
) -> Result<Json<Response<VariableEntity>>, ApiError> {
    let result = handler.service.get_by_id(&auth.tenant_id, &var_id).await?;
    Ok(Json(Response::success(result)))
}

async fn update_meta_variable(
    _: Guard<RequireMetaVariableWrite>,
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path((_meta_id, var_id)): Path<(String, String)>,
    Json(req): Json<UpdateVariableRequest>,
) -> Result<Json<Response<VariableEntity>>, ApiError> {
    let existing = handler.service.get_by_id(&auth.tenant_id, &var_id).await?;
    let entity = VariableEntity {
        id: existing.id,
        tenant_id: existing.tenant_id,
        scope: existing.scope,
        scope_id: existing.scope_id,
        key: existing.key,
        value: req.value,
        variable_type: req.variable_type,
        description: req.description,
        created_by: existing.created_by,
        created_at: existing.created_at,
        updated_at: chrono::Utc::now(),
    };
    let result = handler.service.update(entity).await?;
    Ok(Json(Response::success(result)))
}

async fn delete_meta_variable(
    _: Guard<RequireMetaVariableWrite>,
    State(handler): State<Arc<VariableHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path((_meta_id, var_id)): Path<(String, String)>,
) -> Result<Json<Response<()>>, ApiError> {
    handler.service.delete(&auth.tenant_id, &var_id).await?;
    Ok(Json(Response::success(())))
}
