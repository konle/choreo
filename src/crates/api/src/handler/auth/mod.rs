use crate::error::ApiError;
use crate::middleware::auth::{AuthContext, Claims, create_token};
use crate::response::response::Response;
use axum::{
    Json, Router,
    extract::{Extension, State},
    routing::{get, post},
};
use domain::tenant::service::TenantService;
use domain::user::entity::{UserEntity, UserStatus};
use domain::user::service::UserService;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
    pub tenant_id: String,
}

#[derive(Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub email: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    pub old_password: String,
    pub new_password: String,
}

#[derive(Serialize)]
pub struct TenantOption {
    pub tenant_id: String,
    pub name: String,
}

#[derive(Clone)]
pub struct AuthHandler {
    pub user_service: UserService,
    pub tenant_service: TenantService,
}

impl AuthHandler {
    pub fn new(user_service: UserService, tenant_service: TenantService) -> Self {
        Self {
            user_service,
            tenant_service,
        }
    }
}

pub fn public_routes(handler: Arc<AuthHandler>) -> Router {
    Router::new()
        .route("/login", post(login))
        .route("/register", post(register))
        .route("/tenants", get(list_tenants))
        .with_state(handler)
}

pub fn protected_routes(handler: Arc<AuthHandler>) -> Router {
    Router::new()
        .route("/change-password", post(change_password))
        .route("/profile", get(get_profile))
        .with_state(handler)
}

async fn list_tenants(
    State(handler): State<Arc<AuthHandler>>,
) -> Result<Json<Response<Vec<TenantOption>>>, ApiError> {
    let tenants = handler.tenant_service.list_tenants().await?;
    let options: Vec<TenantOption> = tenants
        .into_iter()
        .filter(|t| t.status == domain::tenant::entity::TenantStatus::Active)
        .map(|t| TenantOption {
            tenant_id: t.tenant_id,
            name: t.name,
        })
        .collect();
    Ok(Json(Response::success(options)))
}

async fn fetch_valid_user(
    user_service: &UserService,
    username: &str,
    password: &str,
) -> Result<UserEntity, ApiError> {
    let user = user_service
        .get_user_by_username(username)
        .await
        .map_err(|_| {
            warn!(username = %username, "login failed: invalid username");
            ApiError::bad_request("Invalid username or password")
        })?;

    if user.status != UserStatus::Active {
        warn!(username = %username, status = ?user.status, "login failed: user disabled");
        return Err(ApiError::bad_request("User is disabled"));
    }

    let valid = bcrypt::verify(password, &user.password_hash).map_err(|e| {
        error!(username = %username, error = %e, "bcrypt verification error");
        ApiError::internal("Password verification failed")
    })?;

    if !valid {
        warn!(username = %username, "login failed: wrong password");
        return Err(ApiError::bad_request("Invalid username or password"));
    }

    Ok(user)
}

async fn resolve_login_role(
    user_service: &UserService,
    user: &UserEntity,
    tenant_id: &str,
) -> Result<String, ApiError> {
    if user.is_super_admin {
        return Ok("SuperAdmin".to_string());
    }
    let user_role = user_service
        .get_role(&user.user_id, tenant_id)
        .await
        .map_err(|_| {
            warn!(
                username = %user.username,
                tenant_id = %tenant_id,
                "login failed: user not in tenant"
            );
            ApiError::bad_request("User does not belong to this tenant")
        })?;
    Ok(format!("{}", user_role.role))
}

fn generate_login_token(
    user: &UserEntity,
    tenant_id: String,
    role: String,
) -> Result<String, ApiError> {
    let exp = chrono::Utc::now().timestamp() as usize + 86400;
    let claims = Claims {
        sub: user.user_id.clone(),
        username: user.username.clone(),
        is_super_admin: user.is_super_admin,
        tenant_id,
        role,
        exp,
    };
    create_token(&claims).map_err(|e| {
        error!(username = %user.username, error = %e, "token creation failed");
        ApiError::internal(format!("Token creation failed: {}", e))
    })
}

async fn login(
    State(handler): State<Arc<AuthHandler>>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<Response<serde_json::Value>>, ApiError> {
    let user = fetch_valid_user(&handler.user_service, &req.username, &req.password).await?;

    handler
        .tenant_service
        .get_tenant(&req.tenant_id)
        .await
        .map_err(|_| {
            warn!(tenant_id = %req.tenant_id, "login failed: tenant not found");
            ApiError::bad_request("Tenant not found")
        })?;

    let role = resolve_login_role(&handler.user_service, &user, &req.tenant_id).await?;
    let token = generate_login_token(&user, req.tenant_id, role)?;

    info!(username = %user.username, user_id = %user.user_id, "login successful");

    Ok(Json(Response::success(serde_json::json!({
        "token": token,
        "user_id": user.user_id,
        "username": user.username,
    }))))
}

async fn register(
    State(handler): State<Arc<AuthHandler>>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<Response<serde_json::Value>>, ApiError> {
    let password_hash = bcrypt::hash(&req.password, bcrypt::DEFAULT_COST).map_err(|e| {
        error!(error = %e, "password hashing failed during register");
        ApiError::internal(format!("Password hashing failed: {}", e))
    })?;

    let user = handler
        .user_service
        .create_user(req.username, req.email, password_hash, false)
        .await?;

    info!(username = %user.username, user_id = %user.user_id, "user registered");

    Ok(Json(Response::success(serde_json::json!({
        "user_id": user.user_id,
        "username": user.username,
    }))))
}

fn validate_old_password(
    user: &UserEntity,
    old_password: &str,
    user_id: &str,
) -> Result<(), ApiError> {
    let valid = bcrypt::verify(old_password, &user.password_hash).map_err(|e| {
        error!(user_id = %user_id, error = %e, "bcrypt verification error");
        ApiError::internal("Password verification failed")
    })?;
    if !valid {
        return Err(ApiError::bad_request("Old password is incorrect"));
    }
    Ok(())
}

fn hash_new_password(new_password: &str) -> Result<String, ApiError> {
    if new_password.len() < 6 {
        return Err(ApiError::bad_request(
            "New password must be at least 6 characters",
        ));
    }
    bcrypt::hash(new_password, bcrypt::DEFAULT_COST).map_err(|e| {
        error!(error = %e, "password hashing failed");
        ApiError::internal(format!("Password hashing failed: {e}"))
    })
}

async fn change_password(
    State(handler): State<Arc<AuthHandler>>,
    Extension(ctx): Extension<AuthContext>,
    Json(req): Json<ChangePasswordRequest>,
) -> Result<Json<Response<()>>, ApiError> {
    let user = handler
        .user_service
        .get_user(&ctx.user_id)
        .await
        .map_err(|_| ApiError::bad_request("User not found"))?;

    validate_old_password(&user, &req.old_password, &ctx.user_id)?;
    let new_hash = hash_new_password(&req.new_password)?;

    handler
        .user_service
        .change_password(&ctx.user_id, new_hash)
        .await?;

    info!(user_id = %ctx.user_id, username = %ctx.username, "password changed");
    Ok(Json(Response::success(())))
}

async fn get_profile(
    State(handler): State<Arc<AuthHandler>>,
    Extension(ctx): Extension<AuthContext>,
) -> Result<Json<Response<serde_json::Value>>, ApiError> {
    let user = handler
        .user_service
        .get_user(&ctx.user_id)
        .await
        .map_err(|_| ApiError::bad_request("User not found"))?;

    Ok(Json(Response::success(serde_json::json!({
        "user_id": user.user_id,
        "username": user.username,
        "email": user.email,
        "is_super_admin": user.is_super_admin,
        "status": format!("{}", user.status),
        "created_at": user.created_at.to_rfc3339(),
    }))))
}
