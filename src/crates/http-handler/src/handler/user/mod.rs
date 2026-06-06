use crate::error::ApiError;
use application::auth::context::AuthContext;
use crate::response::response::Response;
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::{delete, post},
};
use domain::user::entity::{TenantRole, UserTenantRole};
use domain::user::service::UserService;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Deserialize)]
pub struct AssignRoleRequest {
    pub username: String,
    pub role: String,
}

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub email: String,
    pub role: String,
}

#[derive(Serialize)]
pub struct CreateUserResponse {
    pub username: String,
    pub initial_password: String,
}

#[derive(Serialize)]
pub struct UserRoleInfo {
    pub user_id: String,
    pub username: String,
    pub email: String,
    pub tenant_id: String,
    pub role: TenantRole,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone)]
pub struct UserHandler {
    pub service: UserService,
}

impl UserHandler {
    pub fn new(service: UserService) -> Self {
        Self { service }
    }
}

pub fn routes(handler: Arc<UserHandler>) -> Router {
    Router::new()
        .route("/", post(assign_role).get(list_users))
        .route("/create", post(create_user))
        .route("/{username}", delete(remove_role).put(update_role))
        .with_state(handler)
}

async fn list_users(
    State(handler): State<Arc<UserHandler>>,
    Extension(ctx): Extension<AuthContext>,
) -> Result<Json<Response<Vec<UserRoleInfo>>>, ApiError> {
    let roles = handler.service.list_tenant_users(&ctx.tenant_id).await?;
    let mut result = Vec::with_capacity(roles.len());
    for r in roles {
        match handler.service.get_user(&r.user_id).await {
            Ok(user) => result.push(UserRoleInfo {
                user_id: user.user_id,
                username: user.username,
                email: user.email,
                tenant_id: r.tenant_id,
                role: r.role,
                created_at: r.created_at,
            }),
            Err(_) => continue,
        }
    }
    Ok(Json(Response::success(result)))
}

async fn resolve_username(handler: &UserHandler, username: &str) -> Result<String, ApiError> {
    let user = handler
        .service
        .get_user_by_username(username)
        .await
        .map_err(|_| ApiError::bad_request(format!("User not found: {}", username)))?;
    Ok(user.user_id)
}

async fn assign_role(
    State(handler): State<Arc<UserHandler>>,
    Extension(ctx): Extension<AuthContext>,
    Json(req): Json<AssignRoleRequest>,
) -> Result<Json<Response<UserTenantRole>>, ApiError> {
    let role = TenantRole::parse(&req.role)
        .ok_or_else(|| ApiError::bad_request(format!("Invalid role: {}", req.role)))?;

    let user_id = resolve_username(&handler, &req.username).await?;

    let result = handler
        .service
        .assign_role(&user_id, &ctx.tenant_id, &role)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn update_role(
    State(handler): State<Arc<UserHandler>>,
    Extension(ctx): Extension<AuthContext>,
    Path(username): Path<String>,
    Json(req): Json<AssignRoleRequest>,
) -> Result<Json<Response<UserTenantRole>>, ApiError> {
    let role = TenantRole::parse(&req.role)
        .ok_or_else(|| ApiError::bad_request(format!("Invalid role: {}", req.role)))?;

    let user_id = resolve_username(&handler, &username).await?;

    let result = handler
        .service
        .assign_role(&user_id, &ctx.tenant_id, &role)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn remove_role(
    State(handler): State<Arc<UserHandler>>,
    Extension(ctx): Extension<AuthContext>,
    Path(username): Path<String>,
) -> Result<Json<Response<()>>, ApiError> {
    let user_id = resolve_username(&handler, &username).await?;

    handler
        .service
        .remove_role(&user_id, &ctx.tenant_id)
        .await?;
    Ok(Json(Response::success(())))
}

fn generate_initial_password() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let chars: Vec<char> = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
        .chars()
        .collect();
    (0..12)
        .map(|_| chars[rng.random_range(0..chars.len())])
        .collect()
}

async fn create_user(
    State(handler): State<Arc<UserHandler>>,
    Extension(ctx): Extension<AuthContext>,
    Json(req): Json<CreateUserRequest>,
) -> Result<Json<Response<CreateUserResponse>>, ApiError> {
    let role = TenantRole::parse(&req.role)
        .ok_or_else(|| ApiError::bad_request(format!("Invalid role: {}", req.role)))?;

    let initial_password = generate_initial_password();

    let password_hash = bcrypt::hash(&initial_password, bcrypt::DEFAULT_COST)
        .map_err(|e| ApiError::internal(format!("Password hashing failed: {}", e)))?;

    let user = handler
        .service
        .create_user(req.username.clone(), req.email, password_hash, false)
        .await?;

    handler
        .service
        .assign_role(&user.user_id, &ctx.tenant_id, &role)
        .await?;

    info!(username = %user.username, user_id = %user.user_id, "admin created user");

    Ok(Json(Response::success(CreateUserResponse {
        username: user.username,
        initial_password,
    })))
}
