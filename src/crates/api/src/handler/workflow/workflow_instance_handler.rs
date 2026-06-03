use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    middleware::from_fn,
    routing::{get, post},
};
use domain::workflow::{
    entity::workflow_definition::{
        NodeExecutionStatus, WorkflowEntity, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
    },
    service::{WorkflowDefinitionService, WorkflowInstanceService, node_callback_child_task_id},
};
use domain::{
    shared::{
        job::{
            ExecuteTaskJob, ExecuteWorkflowJob, TaskDispatcher, WorkflowCallerContext,
            WorkflowEvent,
        },
        workflow::{TaskType, WorkflowInstanceStatus, WorkflowStatus},
    },
    user::entity::Permission,
    workflow::entity::query::WorkflowInstanceQuery,
};
use tracing::{error, info};

use common::pagination::PaginatedData;

use crate::error::ApiError;
use crate::handler::workflow::workflow_instance_request::{
    CreateWorkflowInstanceRequest, ListWorkflowInstancesRequest, ResumeNodeRequest,
    RetryWorkflowNodeRequest, SkipWorkflowNodeRequest,
};
use crate::middleware::auth::AuthContext;
use crate::middleware::permission::require_permission;
use crate::middleware::permission_guard::{PermissionLevel, RequireDraftInstanceCreate};
use crate::response::response::Response;
use std::sync::Arc;

#[derive(Clone)]
pub struct WorkflowInstanceHandler {
    definition_service: WorkflowDefinitionService,
    instance_service: WorkflowInstanceService,
    dispatcher: Arc<dyn TaskDispatcher>,
}

impl WorkflowInstanceHandler {
    pub fn new(
        definition_service: WorkflowDefinitionService,
        instance_service: WorkflowInstanceService,
        dispatcher: Arc<dyn TaskDispatcher>,
    ) -> Self {
        Self {
            definition_service,
            instance_service,
            dispatcher,
        }
    }
}

pub fn routes(handler: Arc<WorkflowInstanceHandler>) -> Router {
    let reads = Router::new()
        .route("/", get(list_instances))
        .route("/{id}", get(get_instance));

    let writes = Router::new()
        .route("/", post(create_instance))
        .route("/{id}/execute", post(execute_instance))
        .route("/{id}/cancel", post(cancel_instance))
        .route("/{id}/resume", post(resume_instance))
        .route("/{id}/skip-node", post(skip_node))
        .route("/{id}/retry-node", post(retry_node))
        .route("/{id}/resume-node", post(resume_node))
        .layer(from_fn(require_permission(Permission::InstanceExecute)));

    Router::new().merge(reads).merge(writes).with_state(handler)
}

async fn load_workflow_entity_for_create(
    handler: &WorkflowInstanceHandler,
    auth: &AuthContext,
    req: &CreateWorkflowInstanceRequest,
) -> Result<WorkflowEntity, ApiError> {
    handler
        .definition_service
        .get_workflow_meta_entity_scoped(&auth.tenant_id, &req.workflow_meta_id)
        .await?;
    let workflow_entity = handler
        .definition_service
        .get_workflow_entity(req.workflow_meta_id.clone(), req.version)
        .await?;
    Ok(workflow_entity)
}

async fn validate_and_create_instance(
    handler: &WorkflowInstanceHandler,
    auth: &AuthContext,
    req: CreateWorkflowInstanceRequest,
    workflow_entity: WorkflowEntity,
) -> Result<WorkflowInstanceEntity, ApiError> {
    match workflow_entity.status {
        WorkflowStatus::Draft => {
            RequireDraftInstanceCreate::check(auth)?;
        }
        WorkflowStatus::Published => {}
        _ => {
            return Err(ApiError::bad_request(
                "workflow is not a draft or published",
            ));
        }
    }
    handler
        .instance_service
        .create_instance(
            &auth.tenant_id,
            &workflow_entity,
            req.context,
            None,
            0,
            Some(auth.user_id.clone()),
        )
        .await
        .map_err(ApiError::internal)
}

async fn create_instance(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<CreateWorkflowInstanceRequest>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    let workflow_entity = load_workflow_entity_for_create(&handler, &auth, &req).await?;
    let instance = validate_and_create_instance(&handler, &auth, req, workflow_entity).await?;

    info!(
        workflow_instance_id = %instance.workflow_instance_id,
        tenant_id = %auth.tenant_id,
        "workflow instance created"
    );

    Ok(Json(Response::success(instance)))
}

async fn list_instances(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Query(req): Query<ListWorkflowInstancesRequest>,
) -> Result<Json<Response<PaginatedData<WorkflowInstanceEntity>>>, ApiError> {
    let mut query = WorkflowInstanceQuery::from(req);
    query.tenant_id = auth.tenant_id.clone();
    info!(
        "list_instances query: {:?} tenant_id: {}",
        query, auth.tenant_id
    );
    let result = handler
        .instance_service
        .list_workflow_instances(&auth.tenant_id, &query)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn get_instance(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    let result = handler
        .instance_service
        .get_workflow_instance_scoped(&auth.tenant_id, &id)
        .await?;
    Ok(Json(Response::success(result)))
}

async fn execute_instance(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    let instance = handler
        .instance_service
        .get_workflow_instance_scoped(&auth.tenant_id, &id)
        .await?;

    if instance.status != WorkflowInstanceStatus::Pending {
        return Err(ApiError::bad_request(format!(
            "workflow instance is not in Pending state (current: {:?})",
            instance.status
        )));
    }

    handler
        .dispatcher
        .dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: id.clone(),
            tenant_id: auth.tenant_id,
            event: WorkflowEvent::Start,
        })
        .await
        .map_err(|e| {
            error!(workflow_instance_id = %id, error = %e, "failed to dispatch workflow execution");
            ApiError::internal(e.to_string())
        })?;

    info!(workflow_instance_id = %id, "workflow execution dispatched");

    Ok(Json(Response::success(instance)))
}

async fn cancel_instance(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    handler
        .instance_service
        .get_workflow_instance_scoped(&auth.tenant_id, &id)
        .await?;
    let result = handler.instance_service.cancel_instance(&id).await?;
    info!(workflow_instance_id = %id, "workflow instance cancelled");
    Ok(Json(Response::success(result)))
}

async fn retry_container_child(
    dispatcher: &Arc<dyn TaskDispatcher>,
    updated: &WorkflowInstanceEntity,
    auth: &AuthContext,
    id: &str,
    cid: &str,
    node: &WorkflowNodeInstanceEntity,
    req: &RetryWorkflowNodeRequest,
) -> Result<(), ApiError> {
    let item_index = cid.rsplit('-').next().and_then(|s| s.parse::<usize>().ok());
    let caller_context = WorkflowCallerContext {
        workflow_instance_id: updated.workflow_instance_id.clone(),
        node_id: req.node_id.clone(),
        parent_task_instance_id: Some(node.task_instance.id.clone()),
        item_index,
    };
    dispatcher
        .dispatch_task(ExecuteTaskJob {
            task_instance_id: cid.to_string(),
            tenant_id: auth.tenant_id.clone(),
            caller_context: Some(caller_context),
        })
        .await
        .map_err(|e| {
            error!(workflow_instance_id = %id, child_task_id = %cid, error = %e, "failed to dispatch child task retry");
            ApiError::internal(e.to_string())
        })?;
    dispatcher
        .dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: updated.workflow_instance_id.clone(),
            tenant_id: auth.tenant_id.clone(),
            event: WorkflowEvent::RetryContainerChild {
                node_id: req.node_id.clone(),
                child_task_id: cid.to_string(),
            },
        })
        .await
        .map_err(|e| {
            error!(workflow_instance_id = %id, child_task_id = %cid, error = %e, "failed to dispatch RetryContainerChild event");
            ApiError::internal(e.to_string())
        })?;
    info!(workflow_instance_id = %id, node_id = %req.node_id, child_task_id = %cid, "retry-node: container child re-dispatched");
    Ok(())
}

async fn retry_atomic_node(
    dispatcher: &Arc<dyn TaskDispatcher>,
    updated: &WorkflowInstanceEntity,
    auth: &AuthContext,
    id: &str,
    req: &RetryWorkflowNodeRequest,
) -> Result<(), ApiError> {
    dispatcher
        .dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: updated.workflow_instance_id.clone(),
            tenant_id: auth.tenant_id.clone(),
            event: WorkflowEvent::Start,
        })
        .await
        .map_err(|e| {
            error!(workflow_instance_id = %id, error = %e, "failed to dispatch Start after retry");
            ApiError::internal(e.to_string())
        })?;
    info!(workflow_instance_id = %id, node_id = %req.node_id, "retry-node: atomic node retried and Start dispatched");
    Ok(())
}

async fn dispatch_retry(
    dispatcher: &Arc<dyn TaskDispatcher>,
    updated: &WorkflowInstanceEntity,
    auth: &AuthContext,
    id: &str,
    node: &WorkflowNodeInstanceEntity,
    req: &RetryWorkflowNodeRequest,
) -> Result<(), ApiError> {
    if matches!(node.node_type, TaskType::Parallel | TaskType::ForkJoin) {
        let cid = req.child_task_id.as_ref().unwrap();
        retry_container_child(dispatcher, updated, auth, id, cid, node, req).await
    } else {
        retry_atomic_node(dispatcher, updated, auth, id, req).await
    }
}

async fn retry_node(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<RetryWorkflowNodeRequest>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    handler
        .instance_service
        .get_workflow_instance_scoped(&auth.tenant_id, &id)
        .await?;

    let updated = handler
        .instance_service
        .retry_workflow_node(
            &auth.tenant_id,
            &id,
            &req.node_id,
            req.child_task_id.clone(),
        )
        .await
        .map_err(ApiError::bad_request)?;

    let node = updated
        .nodes
        .iter()
        .find(|n| n.node_id == req.node_id)
        .ok_or_else(|| ApiError::internal("retried node missing after save"))?;

    dispatch_retry(&handler.dispatcher, &updated, &auth, &id, node, &req).await?;
    Ok(Json(Response::success(updated)))
}

async fn resume_instance(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    handler
        .instance_service
        .get_workflow_instance_scoped(&auth.tenant_id, &id)
        .await?;
    let result = handler.instance_service.resume_instance(&id).await?;
    info!(workflow_instance_id = %id, "workflow instance resumed");
    Ok(Json(Response::success(result)))
}

fn prepare_skip_dispatch_params<'a>(
    updated: &'a WorkflowInstanceEntity,
    node: &'a WorkflowNodeInstanceEntity,
    req: &'a SkipWorkflowNodeRequest,
) -> (String, serde_json::Value) {
    let child_task_id = match req.child_task_id {
        Some(ref cid) => cid.clone(),
        None => node_callback_child_task_id(updated, node),
    };
    let out = node.task_instance.output.clone().unwrap_or(req.output.clone());
    (child_task_id, out)
}

async fn skip_node(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<SkipWorkflowNodeRequest>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    handler
        .instance_service
        .get_workflow_instance_scoped(&auth.tenant_id, &id)
        .await?;

    let updated = handler
        .instance_service
        .skip_workflow_node(
            &auth.tenant_id,
            &id,
            &req.node_id,
            req.child_task_id.clone(),
            req.output.clone(),
        )
        .await
        .map_err(ApiError::bad_request)?;

    let node = updated
        .nodes
        .iter()
        .find(|n| n.node_id == req.node_id)
        .ok_or_else(|| ApiError::internal("skipped node missing after save"))?;

    let (child_task_id, out) = prepare_skip_dispatch_params(&updated, node, &req);

    handler
        .dispatcher
        .dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: updated.workflow_instance_id.clone(),
            tenant_id: auth.tenant_id.clone(),
            event: WorkflowEvent::NodeCallback {
                node_id: req.node_id.clone(),
                child_task_id,
                status: NodeExecutionStatus::Skipped,
                output: Some(out),
                error_message: None,
                input: None,
            },
        })
        .await
        .map_err(|e| {
            error!(workflow_instance_id = %id, error = %e, "failed to dispatch skip NodeCallback");
            ApiError::internal(e.to_string())
        })?;

    info!(workflow_instance_id = %id, node_id = %req.node_id, child_task_id = ?req.child_task_id, "skip-node persisted and NodeCallback dispatched");

    Ok(Json(Response::success(updated)))
}

fn find_resume_node<'a>(
    instance: &'a WorkflowInstanceEntity,
    node_id: &str,
) -> Result<&'a WorkflowNodeInstanceEntity, ApiError> {
    let node = instance
        .nodes
        .iter()
        .find(|n| n.node_id == node_id)
        .ok_or_else(|| ApiError::bad_request(format!("Node not found: {}", node_id)))?;

    if node.status != NodeExecutionStatus::Suspended {
        return Err(ApiError::bad_request(format!(
            "Node {} is not suspended (current: {:?})",
            node_id, node.status
        )));
    }
    Ok(node)
}

fn validate_manual_pause(node: &WorkflowNodeInstanceEntity) -> Result<(), ApiError> {
    use domain::task::entity::task_definition::{PauseMode, TaskTemplate};
    let is_manual_pause =
        matches!(&node.task_instance.task_template, TaskTemplate::Pause(t) if t.mode == PauseMode::Manual);
    if !is_manual_pause {
        return Err(ApiError::bad_request(
            "resume-node only applies to Manual Pause nodes",
        ));
    }
    Ok(())
}

fn validate_resume_timer(node: &WorkflowNodeInstanceEntity) -> Result<(), ApiError> {
    let resume_at = node
        .task_instance
        .output
        .as_ref()
        .and_then(|o| o.get("resume_at"))
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .ok_or_else(|| ApiError::bad_request("Pause node missing valid resume_at in output"))?;

    if chrono::Utc::now() < resume_at {
        return Err(ApiError::bad_request(format!(
            "Pause timer not expired yet, please wait until {}",
            resume_at.to_rfc3339()
        )));
    }
    Ok(())
}

async fn fetch_and_validate_resume(
    handler: &WorkflowInstanceHandler,
    tenant_id: &str,
    id: &str,
    node_id: &str,
) -> Result<(WorkflowInstanceEntity, String, Option<serde_json::Value>), ApiError> {
    let instance = handler
        .instance_service
        .get_workflow_instance_scoped(tenant_id, id)
        .await?;
    let node = find_resume_node(&instance, node_id)?;
    validate_manual_pause(node)?;
    validate_resume_timer(node)?;
    let child_task_id = node_callback_child_task_id(&instance, node);
    let output = node.task_instance.output.clone();
    Ok((instance, child_task_id, output))
}

async fn resume_node(
    State(handler): State<Arc<WorkflowInstanceHandler>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<ResumeNodeRequest>,
) -> Result<Json<Response<WorkflowInstanceEntity>>, ApiError> {
    let (instance, child_task_id, output) =
        fetch_and_validate_resume(&handler, &auth.tenant_id, &id, &req.node_id).await?;

    handler
        .dispatcher
        .dispatch_workflow(ExecuteWorkflowJob {
            workflow_instance_id: instance.workflow_instance_id,
            tenant_id: auth.tenant_id.clone(),
            event: WorkflowEvent::NodeCallback {
                node_id: req.node_id,
                child_task_id,
                status: NodeExecutionStatus::Success,
                output,
                error_message: None,
                input: None,
            },
        })
        .await
        .map_err(|e| {
            error!(workflow_instance_id = %id, error = %e, "failed to dispatch resume NodeCallback");
            ApiError::internal(e.to_string())
        })?;

    info!(workflow_instance_id = %id, "resume-node: manual pause confirmed");

    let updated = handler
        .instance_service
        .get_workflow_instance_scoped(&auth.tenant_id, &id)
        .await?;

    Ok(Json(Response::success(updated)))
}
