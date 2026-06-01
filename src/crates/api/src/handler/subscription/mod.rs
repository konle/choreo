use crate::error::ApiError;
use crate::middleware::auth::AuthContext;
use crate::response::response::Response;
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::get,
};
use domain::notification::entity::{NotificationChannel, NotificationSubscription, SubscriptionScope};
use domain::notification::service::NotificationService;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone)]
pub struct SubscriptionHandler {
    service: NotificationService,
}

impl SubscriptionHandler {
    pub fn new(service: NotificationService) -> Self {
        Self { service }
    }
}

#[derive(Deserialize)]
pub struct CreateSubscriptionRequest {
    pub scope: String,
    pub resource_type: Option<String>,
    pub resource_id: Option<String>,
    pub event_types: Vec<String>,
    pub channels: Vec<NotificationChannel>,
}

#[derive(Deserialize)]
pub struct UpdateSubscriptionRequest {
    pub event_types: Vec<String>,
    pub channels: Vec<NotificationChannel>,
    pub enabled: Option<bool>,
}

pub fn routes(handler: Arc<SubscriptionHandler>) -> Router {
    Router::new()
        .route("/", get(list_subscriptions).post(create_subscription))
        .route(
            "/{id}",
            get(get_subscription)
                .put(update_subscription)
                .delete(delete_subscription),
        )
        .with_state(handler)
}

async fn list_subscriptions(
    State(handler): State<Arc<SubscriptionHandler>>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Response<Vec<NotificationSubscription>>>, ApiError> {
    let subs = handler
        .service
        .list_subscriptions(&auth.tenant_id, &auth.user_id)
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(subs)))
}

async fn create_subscription(
    State(handler): State<Arc<SubscriptionHandler>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<CreateSubscriptionRequest>,
) -> Result<Json<Response<NotificationSubscription>>, ApiError> {
    let scope = match req.scope.as_str() {
        "global" | "Global" => SubscriptionScope::Global,
        "resource" | "Resource" => SubscriptionScope::Resource,
        _ => return Err(ApiError::bad_request("scope must be 'global' or 'resource'")),
    };
    let sub = handler
        .service
        .create_subscription(
            &auth.tenant_id,
            &auth.user_id,
            scope,
            req.resource_type,
            req.resource_id,
            req.event_types,
            req.channels,
        )
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(sub)))
}

async fn get_subscription(
    State(handler): State<Arc<SubscriptionHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<NotificationSubscription>>, ApiError> {
    let sub = handler
        .service
        .get_subscription(&auth.tenant_id, &id)
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(sub)))
}

async fn update_subscription(
    State(handler): State<Arc<SubscriptionHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateSubscriptionRequest>,
) -> Result<Json<Response<NotificationSubscription>>, ApiError> {
    let mut sub = handler
        .service
        .get_subscription(&auth.tenant_id, &id)
        .await
        .map_err(|e| ApiError::internal(e))?;
    if sub.user_id != auth.user_id {
        return Err(ApiError::forbidden("cannot modify another user's subscription"));
    }
    sub.event_types = req.event_types;
    sub.channels = req.channels;
    if let Some(enabled) = req.enabled {
        sub.enabled = enabled;
    }
    let updated = handler
        .service
        .update_subscription(sub)
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(updated)))
}

async fn delete_subscription(
    State(handler): State<Arc<SubscriptionHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<()>>, ApiError> {
    handler
        .service
        .delete_subscription(&auth.tenant_id, &id)
        .await
        .map_err(|e| ApiError::internal(e))?;
    Ok(Json(Response::success(())))
}
