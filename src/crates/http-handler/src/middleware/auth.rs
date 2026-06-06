use crate::response::response::Response as ApiResponse;
use application::auth::token::TokenService;
use axum::{
    Json,
    extract::Request,
    http::{StatusCode, header},
    middleware::Next,
    response::Response,
};
use tracing::warn;

pub async fn auth_middleware(
    mut req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<ApiResponse<()>>)> {
    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    let token = match auth_header {
        Some(t) => t,
        None => {
            warn!(path = %req.uri().path(), "missing or invalid Authorization header");
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ApiResponse::error(
                    401,
                    "Missing or invalid Authorization header".to_string(),
                )),
            ));
        }
    };

    let token_service = TokenService::new(TokenService::jwt_secret());

    let claims = match token_service.verify_token(token) {
        Ok(c) => c,
        Err(e) => {
            warn!(path = %req.uri().path(), error = %e, "invalid or expired token");
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ApiResponse::error(
                    401,
                    "Invalid or expired token".to_string(),
                )),
            ));
        }
    };

    let x_tenant_id = req
        .headers()
        .get("X-Tenant-Id")
        .and_then(|v| v.to_str().ok());

    let ctx = token_service.claims_to_auth_context(&claims, x_tenant_id);

    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}
