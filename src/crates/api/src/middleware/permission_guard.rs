use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use domain::user::entity::{Permission, TenantRole};
use std::marker::PhantomData;

use crate::error::ApiError;
use crate::middleware::auth::AuthContext;

/// Trait implemented by zero-sized marker types to define a permission level.
pub trait PermissionLevel: Send + Sync + 'static {
    fn check(auth: &AuthContext) -> Result<(), ApiError>;
}

/// Axum extractor that rejects requests lacking the required permission.
///
/// Usage in handler signatures:
/// ```ignore
/// async fn my_handler(
///     _: Guard<RequireTemplateWrite>,
///     Extension(auth): Extension<AuthContext>,
/// ) -> ... { }
/// ```
pub struct Guard<P: PermissionLevel>(PhantomData<P>);

impl<P, S> FromRequestParts<S> for Guard<P>
where
    P: PermissionLevel,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let auth = parts
            .extensions
            .get::<AuthContext>()
            .ok_or_else(|| ApiError::forbidden("not authenticated"))?;
        P::check(auth)?;
        Ok(Guard(PhantomData))
    }
}

fn check_permission(auth: &AuthContext, perm: Permission) -> Result<(), ApiError> {
    if auth.is_super_admin {
        return Ok(());
    }
    match &auth.role {
        Some(role) if role.has_permission(&perm) => Ok(()),
        _ => Err(ApiError::forbidden("insufficient permissions")),
    }
}

// ── Static permission markers ──

pub struct RequireTemplateWrite;
impl PermissionLevel for RequireTemplateWrite {
    fn check(auth: &AuthContext) -> Result<(), ApiError> {
        check_permission(auth, Permission::TemplateWrite)
    }
}

pub struct RequireInstanceExecute;
impl PermissionLevel for RequireInstanceExecute {
    fn check(auth: &AuthContext) -> Result<(), ApiError> {
        check_permission(auth, Permission::InstanceExecute)
    }
}

pub struct RequireApprovalAdmin;
impl PermissionLevel for RequireApprovalAdmin {
    fn check(auth: &AuthContext) -> Result<(), ApiError> {
        check_permission(auth, Permission::ApprovalAdmin)
    }
}

pub struct RequireApprovalDecide;
impl PermissionLevel for RequireApprovalDecide {
    fn check(auth: &AuthContext) -> Result<(), ApiError> {
        check_permission(auth, Permission::ApprovalDecide)
    }
}

// ── Dynamic permission markers (business-rule driven) ──

/// Tenant-scope variable writes: only TenantAdmin.
pub struct RequireTenantVariableWrite;
impl PermissionLevel for RequireTenantVariableWrite {
    fn check(auth: &AuthContext) -> Result<(), ApiError> {
        if auth.is_super_admin {
            return Ok(());
        }
        match &auth.role {
            Some(TenantRole::TenantAdmin) => Ok(()),
            _ => Err(ApiError::forbidden(
                "only TenantAdmin can write tenant variables",
            )),
        }
    }
}

/// Workflow-meta-scope variable writes: TenantAdmin or Developer.
pub struct RequireMetaVariableWrite;
impl PermissionLevel for RequireMetaVariableWrite {
    fn check(auth: &AuthContext) -> Result<(), ApiError> {
        check_permission(auth, Permission::TemplateWrite)
    }
}

/// Draft workflow instance creation: requires TemplateWrite level (Developer+).
pub struct RequireDraftInstanceCreate;
impl PermissionLevel for RequireDraftInstanceCreate {
    fn check(auth: &AuthContext) -> Result<(), ApiError> {
        check_permission(auth, Permission::TemplateWrite)
    }
}
