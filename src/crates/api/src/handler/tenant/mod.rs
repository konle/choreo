use crate::error::ApiError;
use crate::response::response::Response;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use domain::tenant::entity::TenantEntity;
use domain::tenant::service::TenantService;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct CreateTenantRequest {
    pub name: String,
    pub description: String,
}

#[derive(Clone)]
pub struct TenantHandler {
    pub service: TenantService,
}

impl TenantHandler {
    pub fn new(service: TenantService) -> Self {
        Self { service }
    }
}

pub fn routes(handler: Arc<TenantHandler>) -> Router {
    Router::new()
        .route("/", post(create_tenant).get(list_tenants))
        .route(
            "/{id}",
            get(get_tenant).put(update_tenant).delete(delete_tenant),
        )
        .route("/{id}/suspend", post(suspend_tenant))
        .with_state(handler)
}

async fn create_tenant(
    State(handler): State<Arc<TenantHandler>>,
    Json(req): Json<CreateTenantRequest>,
) -> Result<Json<Response<TenantEntity>>, ApiError> {
    let entity = handler
        .service
        .create_tenant(req.name, req.description)
        .await?;
    Ok(Json(Response::success(entity)))
}

async fn get_tenant(
    State(handler): State<Arc<TenantHandler>>,
    Path(id): Path<String>,
) -> Result<Json<Response<TenantEntity>>, ApiError> {
    let entity = handler.service.get_tenant(&id).await?;
    Ok(Json(Response::success(entity)))
}

async fn list_tenants(
    State(handler): State<Arc<TenantHandler>>,
) -> Result<Json<Response<Vec<TenantEntity>>>, ApiError> {
    let list = handler.service.list_tenants().await?;
    Ok(Json(Response::success(list)))
}

async fn update_tenant(
    State(handler): State<Arc<TenantHandler>>,
    Path(id): Path<String>,
    Json(req): Json<CreateTenantRequest>,
) -> Result<Json<Response<()>>, ApiError> {
    let mut entity = handler.service.get_tenant(&id).await?;
    entity.name = req.name;
    entity.description = req.description;
    entity.updated_at = chrono::Utc::now();
    handler.service.update_tenant(&entity).await?;
    Ok(Json(Response::success(())))
}

async fn delete_tenant(
    State(handler): State<Arc<TenantHandler>>,
    Path(id): Path<String>,
) -> Result<Json<Response<()>>, ApiError> {
    handler.service.delete_tenant(&id).await?;
    Ok(Json(Response::success(())))
}

async fn suspend_tenant(
    State(handler): State<Arc<TenantHandler>>,
    Path(id): Path<String>,
) -> Result<Json<Response<()>>, ApiError> {
    handler.service.suspend_tenant(&id).await?;
    Ok(Json(Response::success(())))
}
