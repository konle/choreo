use crate::response::response::Response as ApiResponse;
use axum::{
    Json,
    extract::Request,
    http::{StatusCode, header},
    middleware::Next,
    response::Response,
};
use domain::user::entity::TenantRole;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub username: String,
    pub is_super_admin: bool,
    pub tenant_id: String,
    pub role: String,
    pub exp: usize,
}

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: String,
    pub username: String,
    pub is_super_admin: bool,
    pub tenant_id: String,
    pub role: Option<TenantRole>,
}

pub fn jwt_secret() -> String {
    std::env::var("JWT_SECRET").unwrap_or_else(|_| "workflow-default-secret-change-me".to_string())
}

pub fn create_token(claims: &Claims) -> Result<String, jsonwebtoken::errors::Error> {
    encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(jwt_secret().as_bytes()),
    )
}

pub fn verify_token(token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(jwt_secret().as_bytes()),
        &Validation::default(),
    )?;
    Ok(data.claims)
}

pub fn resolve_tenant_id(claims: &Claims, header_tenant_id: Option<&str>) -> String {
    if claims.is_super_admin {
        header_tenant_id.unwrap_or(&claims.tenant_id).to_string()
    } else {
        claims.tenant_id.clone()
    }
}

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

    let claims = match verify_token(token) {
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

    let tenant_id = {
        let header_val = req.headers()
            .get("X-Tenant-Id")
            .and_then(|v| v.to_str().ok());
        resolve_tenant_id(&claims, header_val)
    };

    let ctx = AuthContext {
        user_id: claims.sub,
        username: claims.username,
        is_super_admin: claims.is_super_admin,
        tenant_id,
        role: TenantRole::from_str(&claims.role),
    };

    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_claims(super_admin: bool, tenant_id: &str) -> Claims {
        Claims {
            sub: "u1".into(),
            username: "test".into(),
            is_super_admin: super_admin,
            tenant_id: tenant_id.into(),
            role: "Developer".into(),
            exp: 9999999999,
        }
    }

    #[test]
    fn resolve_tenant_id_normal_user() {
        let claims = make_claims(false, "tenant-a");
        assert_eq!(resolve_tenant_id(&claims, None), "tenant-a");
        assert_eq!(resolve_tenant_id(&claims, Some("other")), "tenant-a");
    }

    #[test]
    fn resolve_tenant_id_super_admin_with_header() {
        let claims = make_claims(true, "tenant-a");
        assert_eq!(resolve_tenant_id(&claims, Some("tenant-b")), "tenant-b");
    }

    #[test]
    fn resolve_tenant_id_super_admin_no_header() {
        let claims = make_claims(true, "tenant-a");
        assert_eq!(resolve_tenant_id(&claims, None), "tenant-a");
    }
}
