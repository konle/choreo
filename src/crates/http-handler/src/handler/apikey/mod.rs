use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::{delete, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;

use crate::error::ApiError;
use crate::response::response::Response;
use application::auth::context::{AuthContext, Claims};
use application::auth::token::TokenService;
use domain::apikey::service::ApiKeyService;
use domain::user::entity::TenantRole;

#[derive(Deserialize)]
pub struct TokenExchangeRequest {
    pub key: String,
}

#[derive(Serialize)]
pub struct TokenExchangeResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u32,
}

#[derive(Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
    pub role: String,
    pub expires_at: Option<String>,
    pub token_ttl_secs: Option<u32>,
}

#[derive(Serialize)]
pub struct CreateApiKeyResponse {
    pub id: String,
    pub name: String,
    pub key: String,
    pub key_prefix: String,
    pub role: String,
    pub expires_at: Option<String>,
    pub token_ttl_secs: u32,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct ApiKeyListItem {
    pub id: String,
    pub name: String,
    pub key_prefix: String,
    pub role: String,
    pub expires_at: Option<String>,
    pub token_ttl_secs: u32,
    pub status: String,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
    pub last_used_at: Option<String>,
}

#[derive(Clone)]
pub struct ApiKeyHandler {
    pub service: ApiKeyService,
}

impl ApiKeyHandler {
    pub fn new(service: ApiKeyService) -> Self {
        Self { service }
    }
}

pub fn public_routes(handler: Arc<ApiKeyHandler>) -> Router {
    Router::new()
        .route("/token", post(exchange_token))
        .with_state(handler)
}

pub fn protected_routes(handler: Arc<ApiKeyHandler>) -> Router {
    Router::new()
        .route("/", post(create_api_key).get(list_api_keys))
        .route("/{id}", delete(revoke_api_key))
        .with_state(handler)
}

async fn exchange_token(
    State(handler): State<Arc<ApiKeyHandler>>,
    Json(req): Json<TokenExchangeRequest>,
) -> Result<Json<Response<TokenExchangeResponse>>, ApiError> {
    let entity = handler
        .service
        .authenticate(&req.key)
        .await
        .map_err(|_| ApiError::unauthorized("Invalid or expired API key"))?;

    let exp = chrono::Utc::now().timestamp() as usize + entity.token_ttl_secs as usize;
    let claims = Claims {
        sub: entity.id.clone(),
        username: entity.name.clone(),
        is_super_admin: false,
        tenant_id: entity.tenant_id.clone(),
        role: format!("{}", entity.role),
        exp,
    };

    let token_service = TokenService::new(TokenService::jwt_secret());
    let access_token = token_service.create_token(&claims).map_err(|e| {
        error!(error = %e, "api key token creation failed");
        ApiError::internal(format!("Token creation failed: {}", e))
    })?;

    Ok(Json(Response::success(TokenExchangeResponse {
        access_token,
        token_type: "Bearer".to_string(),
        expires_in: entity.token_ttl_secs,
    })))
}

fn validate_api_key_role_and_ttl(
    role_str: &str,
    token_ttl_secs: Option<u32>,
) -> Result<(TenantRole, u32), ApiError> {
    let role = TenantRole::parse(role_str)
        .ok_or_else(|| ApiError::bad_request(format!("Invalid role: {role_str}")))?;
    if matches!(role, TenantRole::TenantAdmin) {
        return Err(ApiError::bad_request(
            "API keys cannot have TenantAdmin role",
        ));
    }
    let ttl = token_ttl_secs.unwrap_or(3600);
    if !(60..=86400).contains(&ttl) {
        return Err(ApiError::bad_request(
            "token_ttl_secs must be between 60 and 86400",
        ));
    }
    Ok((role, ttl))
}

fn parse_expires_at(s: &Option<String>) -> Result<Option<chrono::DateTime<chrono::Utc>>, ApiError> {
    match s {
        Some(s) => Ok(Some(
            chrono::DateTime::parse_from_rfc3339(s)
                .map_err(|_| {
                    ApiError::bad_request("Invalid expires_at: expected ISO 8601 / RFC3339")
                })?
                .with_timezone(&chrono::Utc),
        )),
        None => Ok(None),
    }
}

async fn create_api_key(
    State(handler): State<Arc<ApiKeyHandler>>,
    Extension(ctx): Extension<AuthContext>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<Json<Response<CreateApiKeyResponse>>, ApiError> {
    let (role, token_ttl_secs) = validate_api_key_role_and_ttl(&req.role, req.token_ttl_secs)?;
    let expires_at = parse_expires_at(&req.expires_at)?;

    let (entity, key) = handler
        .service
        .create_api_key(
            &ctx.tenant_id,
            &req.name,
            role,
            expires_at,
            token_ttl_secs,
            &ctx.user_id,
        )
        .await?;

    Ok(Json(Response::success(CreateApiKeyResponse {
        id: entity.id,
        name: entity.name,
        key,
        key_prefix: entity.key_prefix,
        role: format!("{}", entity.role),
        expires_at: entity.expires_at.map(|t| t.to_rfc3339()),
        token_ttl_secs: entity.token_ttl_secs,
        created_at: entity.created_at.to_rfc3339(),
    })))
}

async fn list_api_keys(
    State(handler): State<Arc<ApiKeyHandler>>,
    Extension(ctx): Extension<AuthContext>,
) -> Result<Json<Response<Vec<ApiKeyListItem>>>, ApiError> {
    let list = handler.service.list(&ctx.tenant_id).await?;
    let items: Vec<ApiKeyListItem> = list
        .into_iter()
        .map(|e| ApiKeyListItem {
            id: e.id,
            name: e.name,
            key_prefix: e.key_prefix,
            role: format!("{}", e.role),
            expires_at: e.expires_at.map(|t| t.to_rfc3339()),
            token_ttl_secs: e.token_ttl_secs,
            status: format!("{:?}", e.status),
            created_by: e.created_by,
            created_at: e.created_at.to_rfc3339(),
            updated_at: e.updated_at.to_rfc3339(),
            last_used_at: e.last_used_at.map(|t| t.to_rfc3339()),
        })
        .collect();
    Ok(Json(Response::success(items)))
}

async fn revoke_api_key(
    State(handler): State<Arc<ApiKeyHandler>>,
    Extension(ctx): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<()>>, ApiError> {
    handler.service.revoke(&ctx.tenant_id, &id).await?;
    Ok(Json(Response::success(())))
}
