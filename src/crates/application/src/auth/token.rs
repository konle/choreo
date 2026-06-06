use crate::auth::context::{AuthContext, Claims};
use domain::user::entity::TenantRole;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};

pub struct TokenService {
    secret: String,
}

impl TokenService {
    pub fn new(secret: String) -> Self {
        Self { secret }
    }

    pub fn jwt_secret() -> String {
        std::env::var("JWT_SECRET")
            .unwrap_or_else(|_| "workflow-default-secret-change-me".to_string())
    }

    pub fn create_token(&self, claims: &Claims) -> Result<String, jsonwebtoken::errors::Error> {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
    }

    pub fn verify_token(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &Validation::default(),
        )?;
        Ok(data.claims)
    }

    pub fn resolve_tenant_id(&self, claims: &Claims, header_tenant_id: Option<&str>) -> String {
        if claims.is_super_admin {
            header_tenant_id.unwrap_or(&claims.tenant_id).to_string()
        } else {
            claims.tenant_id.clone()
        }
    }

    pub fn claims_to_auth_context(
        &self,
        claims: &Claims,
        header_tenant_id: Option<&str>,
    ) -> AuthContext {
        let tenant_id = self.resolve_tenant_id(claims, header_tenant_id);
        AuthContext {
            user_id: claims.sub.clone(),
            username: claims.username.clone(),
            is_super_admin: claims.is_super_admin,
            tenant_id,
            role: TenantRole::parse(&claims.role),
        }
    }
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
        let svc = TokenService::new("secret".into());
        let claims = make_claims(false, "tenant-a");
        assert_eq!(svc.resolve_tenant_id(&claims, None), "tenant-a");
        assert_eq!(svc.resolve_tenant_id(&claims, Some("other")), "tenant-a");
    }

    #[test]
    fn resolve_tenant_id_super_admin_with_header() {
        let svc = TokenService::new("secret".into());
        let claims = make_claims(true, "tenant-a");
        assert_eq!(
            svc.resolve_tenant_id(&claims, Some("tenant-b")),
            "tenant-b"
        );
    }

    #[test]
    fn resolve_tenant_id_super_admin_no_header() {
        let svc = TokenService::new("secret".into());
        let claims = make_claims(true, "tenant-a");
        assert_eq!(svc.resolve_tenant_id(&claims, None), "tenant-a");
    }

    #[test]
    fn create_and_verify_token() {
        let svc = TokenService::new("test-secret-123".into());
        let claims = make_claims(false, "tenant-a");
        let token = svc.create_token(&claims).unwrap();
        let verified = svc.verify_token(&token).unwrap();
        assert_eq!(verified.sub, "u1");
        assert_eq!(verified.tenant_id, "tenant-a");
        assert_eq!(verified.role, "Developer");
    }

    #[test]
    fn verify_invalid_token() {
        let svc = TokenService::new("test-secret-123".into());
        assert!(svc.verify_token("invalid-token").is_err());
    }
}
