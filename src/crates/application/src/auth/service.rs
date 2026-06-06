use crate::auth::context::AuthContext;
use crate::auth::token::TokenService;
use domain::user::entity::Permission;

#[derive(Clone)]
pub struct AuthService {
    token_service: TokenService,
}

impl AuthService {
    pub fn new(token_service: TokenService) -> Self {
        Self { token_service }
    }

    pub fn verify_token(&self, token: &str) -> Result<AuthContext, String> {
        self.token_service
            .verify_token(token)
            .map(|claims| self.token_service.claims_to_auth_context(&claims, None))
            .map_err(|e| format!("invalid token: {}", e))
    }

    pub fn verify_token_with_tenant(
        &self,
        token: &str,
        x_tenant_id: Option<&str>,
    ) -> Result<AuthContext, String> {
        self.token_service
            .verify_token(token)
            .map(|claims| self.token_service.claims_to_auth_context(&claims, x_tenant_id))
            .map_err(|e| format!("invalid token: {}", e))
    }

    pub fn authorize(&self, ctx: &AuthContext, perm: &Permission) -> Result<(), String> {
        if ctx.is_super_admin {
            return Ok(());
        }

        if *perm == Permission::TenantManage {
            return Err("SuperAdmin only".to_string());
        }

        match &ctx.role {
            Some(role) if role.has_permission(perm) => Ok(()),
            _ => Err("insufficient permissions".to_string()),
        }
    }

    pub fn token(&self) -> &TokenService {
        &self.token_service
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::context::Claims;
    use domain::user::entity::TenantRole;

    fn make_ctx(role: TenantRole) -> AuthContext {
        AuthContext {
            user_id: "u1".into(),
            username: "test".into(),
            is_super_admin: false,
            tenant_id: "t1".into(),
            role: Some(role),
        }
    }

    fn make_super_admin() -> AuthContext {
        AuthContext {
            user_id: "admin".into(),
            username: "admin".into(),
            is_super_admin: true,
            tenant_id: "t1".into(),
            role: None,
        }
    }

    #[test]
    fn authorize_super_admin_always_ok() {
        let svc = AuthService::new(TokenService::new("secret".into()));
        let ctx = make_super_admin();
        assert!(svc.authorize(&ctx, &Permission::TenantManage).is_ok());
        assert!(svc.authorize(&ctx, &Permission::UserManage).is_ok());
        assert!(svc.authorize(&ctx, &Permission::TemplateWrite).is_ok());
    }

    #[test]
    fn authorize_normal_user_denied_tenant_manage() {
        let svc = AuthService::new(TokenService::new("secret".into()));
        let ctx = make_ctx(TenantRole::TenantAdmin);
        assert!(svc.authorize(&ctx, &Permission::TenantManage).is_err());
    }

    #[test]
    fn authorize_viewer_can_only_read() {
        let svc = AuthService::new(TokenService::new("secret".into()));
        let ctx = make_ctx(TenantRole::Viewer);
        assert!(svc.authorize(&ctx, &Permission::ReadOnly).is_ok());
        assert!(svc.authorize(&ctx, &Permission::TemplateWrite).is_err());
        assert!(svc.authorize(&ctx, &Permission::InstanceExecute).is_err());
    }
}
