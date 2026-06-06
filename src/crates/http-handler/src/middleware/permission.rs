use crate::response::response::Response as ApiResponse;
use application::auth::context::AuthContext;
use axum::{Json, extract::Request, http::StatusCode, middleware::Next, response::Response};
use domain::user::entity::Permission;

#[allow(clippy::type_complexity)]
pub fn require_permission(
    permission: Permission,
) -> impl Fn(
    Request,
    Next,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Response, (StatusCode, Json<ApiResponse<()>>)>>
            + Send,
    >,
> + Clone
+ Send {
    move |req: Request, next: Next| {
        let perm = permission.clone();
        Box::pin(async move {
            let ctx = req.extensions().get::<AuthContext>().cloned();
            let ctx = match ctx {
                Some(c) => c,
                None => {
                    return Err((
                        StatusCode::UNAUTHORIZED,
                        Json(ApiResponse::error(401, "Not authenticated".to_string())),
                    ));
                }
            };

            if ctx.is_super_admin {
                return Ok(next.run(req).await);
            }

            if perm == Permission::TenantManage {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(ApiResponse::error(403, "SuperAdmin only".to_string())),
                ));
            }

            match &ctx.role {
                Some(role) if role.has_permission(&perm) => Ok(next.run(req).await),
                _ => Err((
                    StatusCode::FORBIDDEN,
                    Json(ApiResponse::error(
                        403,
                        "Insufficient permissions".to_string(),
                    )),
                )),
            }
        })
    }
}

#[allow(clippy::type_complexity)]
pub fn require_super_admin() -> impl Fn(
    Request,
    Next,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Response, (StatusCode, Json<ApiResponse<()>>)>>
            + Send,
    >,
> + Clone
+ Send {
    move |req: Request, next: Next| {
        Box::pin(async move {
            let ctx = req.extensions().get::<AuthContext>().cloned();
            match ctx {
                Some(c) if c.is_super_admin => Ok(next.run(req).await),
                _ => Err((
                    StatusCode::FORBIDDEN,
                    Json(ApiResponse::error(403, "SuperAdmin only".to_string())),
                )),
            }
        })
    }
}
