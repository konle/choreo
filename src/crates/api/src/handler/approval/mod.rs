use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::{get, post},
};
use domain::approval::entity::{ApprovalInstanceEntity, Decision};
use domain::approval::service::ApprovalService;
use domain::plugin::plugins::approval::approval_status_to_node_status;
use domain::shared::job::{ExecuteWorkflowJob, TaskDispatcher, WorkflowEvent};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};

use crate::error::ApiError;
use crate::middleware::auth::AuthContext;
use crate::middleware::permission_guard::{Guard, RequireApprovalAdmin, RequireApprovalDecide};
use crate::response::response::Response;

#[derive(Clone)]
pub struct ApprovalHandler {
    service: ApprovalService,
    dispatcher: Arc<dyn TaskDispatcher>,
}

impl ApprovalHandler {
    pub fn new(service: ApprovalService, dispatcher: Arc<dyn TaskDispatcher>) -> Self {
        Self {
            service,
            dispatcher,
        }
    }
}

#[derive(Deserialize)]
pub struct DecideRequest {
    pub decision: Decision,
    pub comment: Option<String>,
}

pub fn routes(handler: Arc<ApprovalHandler>) -> Router {
    Router::new()
        .route("/", get(list_my_approvals))
        .route("/all", get(list_all_approvals))
        .route("/{id}", get(get_approval))
        .route("/{id}/decide", post(decide_approval))
        .with_state(handler)
}

async fn list_my_approvals(
    State(handler): State<Arc<ApprovalHandler>>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Response<Vec<ApprovalInstanceEntity>>>, ApiError> {
    let result = handler
        .service
        .list_pending_by_approver(&auth.tenant_id, &auth.user_id)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn list_all_approvals(
    _: Guard<RequireApprovalAdmin>,
    State(handler): State<Arc<ApprovalHandler>>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Response<Vec<ApprovalInstanceEntity>>>, ApiError> {
    let result = handler.service.list_by_tenant(&auth.tenant_id).await?;
    Ok(Json(Response::success(result)))
}

async fn get_approval(
    State(handler): State<Arc<ApprovalHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<ApprovalInstanceEntity>>, ApiError> {
    let result = handler.service.get_by_id(&auth.tenant_id, &id).await?;
    Ok(Json(Response::success(result)))
}

async fn decide_approval(
    _: Guard<RequireApprovalDecide>,
    State(handler): State<Arc<ApprovalHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<DecideRequest>,
) -> Result<Json<Response<ApprovalInstanceEntity>>, ApiError> {
    let approval = handler
        .service
        .decide(
            &auth.tenant_id,
            &id,
            &auth.user_id,
            req.decision,
            req.comment,
        )
        .await?;

    info!(
        approval_id = %id,
        user_id = %auth.user_id,
        status = ?approval.status,
        "approval decision submitted"
    );

    if approval.status != domain::approval::entity::ApprovalStatus::Pending {
        let node_status = approval_status_to_node_status(&approval.status);
        let output = serde_json::json!({
            "approval_id": approval.id,
            "status": format!("{:?}", approval.status),
            "decisions": approval.decisions.iter().map(|d| {
                serde_json::json!({
                    "user_id": d.user_id,
                    "decision": format!("{:?}", d.decision),
                    "comment": d.comment,
                })
            }).collect::<Vec<_>>(),
        });

        let error_message = if approval.status == domain::approval::entity::ApprovalStatus::Rejected
        {
            Some("Approval rejected".to_string())
        } else {
            None
        };

        let event = WorkflowEvent::NodeCallback {
            node_id: approval.node_id.clone(),
            child_task_id: approval.id.clone(),
            status: node_status,
            output: Some(output),
            error_message,
            input: None,
        };

        handler
            .dispatcher
            .dispatch_workflow(ExecuteWorkflowJob {
                workflow_instance_id: approval.workflow_instance_id.clone(),
                tenant_id: approval.tenant_id.clone(),
                event,
            })
            .await
            .map_err(|e| {
                error!(
                    approval_id = %id,
                    workflow_instance_id = %approval.workflow_instance_id,
                    error = %e,
                    "failed to dispatch approval callback"
                );
                ApiError::internal(format!("Failed to dispatch callback: {}", e))
            })?;
    }

    Ok(Json(Response::success(approval)))
}
