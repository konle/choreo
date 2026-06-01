use crate::error::ApiError;
use crate::middleware::auth::AuthContext;
use crate::response::response::Response;
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::{get, put},
};
use domain::notification::entity::NotificationRecord;
use domain::notification::service::NotificationService;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone)]
pub struct NotificationHandler {
    service: NotificationService,
}

impl NotificationHandler {
    pub fn new(service: NotificationService) -> Self {
        Self { service }
    }
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub page: Option<u64>,
    pub page_size: Option<u64>,
}

pub fn routes(handler: Arc<NotificationHandler>) -> Router {
    Router::new()
        .route("/", get(list_notifications))
        .route("/unread-count", get(unread_count))
        .route("/read-all", put(mark_all_read))
        .route("/{id}/read", put(mark_read))
        .with_state(handler)
}

async fn list_notifications(
    State(handler): State<Arc<NotificationHandler>>,
    Extension(auth): Extension<AuthContext>,
    axum::extract::Query(query): axum::extract::Query<ListQuery>,
) -> Result<Json<Response<Vec<NotificationRecord>>>, ApiError> {
    let page = query.page.unwrap_or(1);
    let page_size = query.page_size.unwrap_or(20);
    let (records, _total) = handler
        .service
        .list_notifications(&auth.tenant_id, &auth.user_id, page, page_size)
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(records)))
}

async fn unread_count(
    State(handler): State<Arc<NotificationHandler>>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Response<u64>>, ApiError> {
    let count = handler
        .service
        .unread_count(&auth.tenant_id, &auth.user_id)
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(count)))
}

async fn mark_read(
    State(handler): State<Arc<NotificationHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<()>>, ApiError> {
    handler
        .service
        .mark_read(&auth.tenant_id, &auth.user_id, &id)
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(())))
}

async fn mark_all_read(
    State(handler): State<Arc<NotificationHandler>>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Response<u64>>, ApiError> {
    let count = handler
        .service
        .mark_all_read(&auth.tenant_id, &auth.user_id)
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(count)))
}
