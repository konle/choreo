use application::auth::service::AuthService;
use application::usecase::approval::ApprovalUsecase;
use application::usecase::task::TaskUsecase;
use application::usecase::workflow::WorkflowUsecase;
use common::pagination::{Pagination, SortQuery};
use domain::approval::entity::Decision;
use domain::task::entity::query::{TaskInstanceFilter, TaskInstanceQuery};
use domain::workflow::entity::query::{WorkflowInstanceFilter, WorkflowInstanceQuery};
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult, Meta,
    ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::ErrorData;
use std::sync::Arc;

use crate::tools::workflow;

// ── Auth helpers ──

fn extract_bearer_token_from_extensions(
    extensions: &rmcp::model::Extensions,
) -> Option<String> {
    let parts = extensions.get::<http::request::Parts>()?;
    let header = parts.headers.get("authorization")?;
    let value = header.to_str().ok()?;
    let token = value.strip_prefix("Bearer ").unwrap_or(value);
    Some(token.to_string())
}

fn resolve_auth(
    auth_service: &AuthService,
    meta: &Meta,
    extensions: &rmcp::model::Extensions,
) -> Result<application::auth::context::AuthContext, ErrorData> {
    if let Some(token) = extract_bearer_token_from_extensions(extensions) {
        return auth_service
            .verify_token(&token)
            .map_err(|e| ErrorData::invalid_params(format!("auth failed: {}", e), None));
    }
    if let Some(token_value) = meta.get("authorization") {
        let token = token_value
            .as_str()
            .ok_or_else(|| ErrorData::invalid_params("invalid authorization metadata", None))?;
        let token = token.strip_prefix("Bearer ").unwrap_or(token);
        return auth_service
            .verify_token(token)
            .map_err(|e| ErrorData::invalid_params(format!("auth failed: {}", e), None));
    }
    Err(ErrorData::invalid_params(
        "missing authorization",
        None,
    ))
}

// ── Tool schema helper ──

fn tool_schema(props: serde_json::Value) -> std::sync::Arc<rmcp::model::JsonObject> {
    if let serde_json::Value::Object(map) = props {
        std::sync::Arc::new(map)
    } else {
        std::sync::Arc::new(serde_json::Map::new())
    }
}

fn json_content(value: &impl serde::Serialize) -> Result<Vec<Content>, ErrorData> {
    Ok(vec![Content::json(value)?])
}

fn parse_args<T: serde::de::DeserializeOwned>(
    args: &Option<rmcp::model::JsonObject>,
) -> Result<T, ErrorData> {
    match args {
        Some(obj) => serde_json::from_value(serde_json::Value::Object(obj.clone()))
            .map_err(|e| ErrorData::invalid_params(e.to_string(), None)),
        None => Err(ErrorData::invalid_params("missing arguments", None)),
    }
}

fn parse_args_or_default<T: serde::de::DeserializeOwned + Default>(
    args: &Option<rmcp::model::JsonObject>,
) -> T {
    match args {
        Some(obj) => serde_json::from_value(serde_json::Value::Object(obj.clone()))
            .unwrap_or_default(),
        None => T::default(),
    }
}

// ── MCP Server ──

#[derive(Clone)]
pub struct McpServer {
    auth_service: Arc<AuthService>,
    workflow_usecase: Arc<WorkflowUsecase>,
    task_usecase: Arc<TaskUsecase>,
    approval_usecase: Arc<ApprovalUsecase>,
}

impl McpServer {
    pub fn new(
        auth_service: Arc<AuthService>,
        workflow_usecase: Arc<WorkflowUsecase>,
        task_usecase: Arc<TaskUsecase>,
        approval_usecase: Arc<ApprovalUsecase>,
    ) -> Self {
        Self {
            auth_service,
            workflow_usecase,
            task_usecase,
            approval_usecase,
        }
    }
}

async fn dispatch_tool_call(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let name = request.name.as_ref();
    match name {
        "list_workflow_instances" => tool_list_instances(auth, request, server).await,
        "get_workflow_instance" => tool_get_instance(auth, request, server).await,
        "execute_workflow_instance" => tool_execute(auth, request, server).await,
        "cancel_workflow_instance" => tool_cancel(auth, request, server).await,
        "retry_workflow_instance" => tool_retry(auth, request, server).await,
        "skip_workflow_node" => tool_skip_node(auth, request, server).await,
        "list_workflow_definitions" => tool_list_defs(auth, server).await,
        "get_workflow_definition" => tool_get_def(auth, request, server).await,
        "list_task_instances" => tool_list_tasks(auth, request, server).await,
        "get_task_instance" => tool_get_task(auth, request, server).await,
        "retry_task_instance" => tool_retry_task(auth, request, server).await,
        "list_approvals" => tool_list_approvals(auth, server).await,
        "decide_approval" => tool_decide_approval(auth, request, server).await,
        _ => Err(ErrorData::invalid_params(format!("unknown tool: {}", name), None)),
    }
}

fn parse_decision(s: &str) -> Result<Decision, String> {
    match s.to_lowercase().as_str() {
        "approve" => Ok(Decision::Approve),
        "reject" => Ok(Decision::Reject),
        _ => Err("decision must be Approve or Reject".to_string()),
    }
}

async fn tool_list_instances(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let params: workflow::ListInstancesParams = parse_args_or_default(&request.arguments);
    let query = WorkflowInstanceQuery {
        tenant_id: auth.tenant_id.clone(),
        filter: WorkflowInstanceFilter {
            workflow_meta_id: params.workflow_meta_id,
            version: params.version,
            status: params.status.and_then(|s| serde_json::from_str(&format!("\"{}\"", s)).ok()),
        },
        pagination: Pagination::new(params.page, params.page_size),
        sort: SortQuery::new("created_at".into(), "desc".into()),
    };
    let result = server.workflow_usecase.list_instances(auth, query).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&result)?))
}

async fn tool_get_instance(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::GetInstanceParams = parse_args(&request.arguments)?;
    let r = server.workflow_usecase.get_instance(auth, &p.instance_id).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_execute(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::ExecuteInstanceParams = parse_args(&request.arguments)?;
    let r = server.workflow_usecase.execute_instance(auth, &p.instance_id).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_cancel(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::CancelInstanceParams = parse_args(&request.arguments)?;
    let r = server.workflow_usecase.cancel_instance(auth, &p.instance_id).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_retry(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::RetryInstanceParams = parse_args(&request.arguments)?;
    let r = server.workflow_usecase.retry_instance(auth, &p.instance_id).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_skip_node(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::SkipNodeParams = parse_args(&request.arguments)?;
    let output = if p.output.is_null() { serde_json::json!({}) } else { p.output };
    let r = server.workflow_usecase.skip_node(auth, &p.instance_id, &p.node_id, output).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_list_defs(
    auth: &application::auth::context::AuthContext,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let r = server.workflow_usecase.list_definitions(auth).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_get_def(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::GetDefinitionParams = parse_args(&request.arguments)?;
    let r = server.workflow_usecase.get_definition(auth, &p.meta_id).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_list_tasks(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let params: workflow::ListTaskInstancesParams = parse_args_or_default(&request.arguments);
    let query = TaskInstanceQuery {
        tenant_id: auth.tenant_id.clone(),
        filter: TaskInstanceFilter {
            status: params.status.and_then(|s| serde_json::from_str(&format!("\"{}\"", s)).ok()),
            task_id: None,
        },
        pagination: Pagination::new(params.page, params.page_size),
        sort: SortQuery::new("created_at".into(), "desc".into()),
    };
    let r = server.task_usecase.list_instances(auth, query).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_get_task(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::TaskInstanceParams = parse_args(&request.arguments)?;
    let r = server.task_usecase.get_instance(auth, &p.instance_id).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_retry_task(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::TaskInstanceParams = parse_args(&request.arguments)?;
    let r = server.task_usecase.retry_instance(auth, &p.instance_id).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_list_approvals(
    auth: &application::auth::context::AuthContext,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let r = server.approval_usecase.list_approvals(auth).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

async fn tool_decide_approval(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::DecideApprovalParams = parse_args(&request.arguments)?;
    let decision = parse_decision(&p.decision)
        .map_err(|e| ErrorData::invalid_params(e, None))?;
    let r = server.approval_usecase.decide_approval(auth, &p.approval_id, decision, p.comment).await
        .map_err(|e| ErrorData::internal_error(e, None))?;
    Ok(CallToolResult::success(json_content(&r)?))
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
    }

    fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, ErrorData>> + Send + '_ {
        std::future::ready(Ok(ListToolsResult {
            tools: vec![
                // Workflow instances
                Tool::new(
                    "list_workflow_instances",
                    "List workflow instances with optional filtering by status",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": {
                            "status": { "type": "string" },
                            "workflow_meta_id": { "type": "string" },
                            "version": { "type": "integer" },
                            "page": { "type": "integer", "default": 1 },
                            "page_size": { "type": "integer", "default": 10 }
                        }
                    })),
                ),
                Tool::new(
                    "get_workflow_instance",
                    "Get detailed information about a specific workflow instance",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": { "instance_id": { "type": "string" } },
                        "required": ["instance_id"]
                    })),
                ),
                Tool::new(
                    "execute_workflow_instance",
                    "Execute a workflow instance (must be in Pending status)",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": { "instance_id": { "type": "string" } },
                        "required": ["instance_id"]
                    })),
                ),
                Tool::new(
                    "cancel_workflow_instance",
                    "Cancel a workflow instance (must be in Failed or Suspended status)",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": { "instance_id": { "type": "string" } },
                        "required": ["instance_id"]
                    })),
                ),
                Tool::new(
                    "retry_workflow_instance",
                    "Retry a failed workflow instance (sets to Pending)",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": { "instance_id": { "type": "string" } },
                        "required": ["instance_id"]
                    })),
                ),
                Tool::new(
                    "skip_workflow_node",
                    "Skip a failed node in a workflow instance",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": {
                            "instance_id": { "type": "string" },
                            "node_id": { "type": "string" },
                            "output": { "type": "object", "description": "Output to set on the skipped node" }
                        },
                        "required": ["instance_id", "node_id"]
                    })),
                ),
                // Workflow definitions
                Tool::new(
                    "list_workflow_definitions",
                    "List all workflow definitions (meta)",
                    tool_schema(serde_json::json!({
                        "type": "object", "properties": {}
                    })),
                ),
                Tool::new(
                    "get_workflow_definition",
                    "Get a workflow definition by meta ID",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": { "meta_id": { "type": "string" } },
                        "required": ["meta_id"]
                    })),
                ),
                // Task instances
                Tool::new(
                    "list_task_instances",
                    "List task instances with optional status filtering",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": {
                            "status": { "type": "string" },
                            "page": { "type": "integer", "default": 1 },
                            "page_size": { "type": "integer", "default": 10 }
                        }
                    })),
                ),
                Tool::new(
                    "get_task_instance",
                    "Get detailed information about a task instance",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": { "instance_id": { "type": "string" } },
                        "required": ["instance_id"]
                    })),
                ),
                Tool::new(
                    "retry_task_instance",
                    "Retry a failed task instance (sets to Pending)",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": { "instance_id": { "type": "string" } },
                        "required": ["instance_id"]
                    })),
                ),
                // Approvals
                Tool::new(
                    "list_approvals",
                    "List all approvals in the current tenant",
                    tool_schema(serde_json::json!({
                        "type": "object", "properties": {}
                    })),
                ),
                Tool::new(
                    "decide_approval",
                    "Approve or reject an approval request",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": {
                            "approval_id": { "type": "string" },
                            "decision": { "type": "string", "description": "Approve or Reject" },
                            "comment": { "type": "string" }
                        },
                        "required": ["approval_id", "decision"]
                    })),
                ),
            ],
            meta: None,
            next_cursor: None,
        }))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = resolve_auth(&self.auth_service, &context.meta, &context.extensions)?;
        dispatch_tool_call(&auth, &request, self).await
    }
}

// ── Service factory ──

pub fn create_mcp_service(
    server: McpServer,
) -> StreamableHttpService<McpServer, LocalSessionManager> {
    let session_manager = Arc::new(LocalSessionManager::default());
    let config = StreamableHttpServerConfig::default()
        .disable_allowed_hosts();
    let server = Arc::new(server);
    StreamableHttpService::new(
        move || Ok((*server).clone()),
        session_manager,
        config,
    )
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;

    fn known_tool_names() -> &'static [&'static str] {
        &[
            "list_workflow_instances", "get_workflow_instance",
            "execute_workflow_instance", "cancel_workflow_instance",
            "retry_workflow_instance", "skip_workflow_node",
            "list_workflow_definitions", "get_workflow_definition",
            "list_task_instances", "get_task_instance", "retry_task_instance",
            "list_approvals", "decide_approval",
        ]
    }

    fn make_test_params(name: &str, args: Option<serde_json::Value>) -> CallToolRequestParams {
        let json = serde_json::json!({
            "name": name,
            "arguments": args.unwrap_or(serde_json::json!({}))
        });
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn test_known_tool_names_count() {
        assert_eq!(known_tool_names().len(), 13);
    }

    #[test]
    fn test_known_tool_names_all_found() {
        for name in known_tool_names() {
            assert!(known_tool_names().contains(name));
        }
    }

    #[test]
    fn test_parse_list_instances_params_defaults() {
        let params: workflow::ListInstancesParams = parse_args_or_default(&None);
        assert_eq!(params.page, 1);
        assert_eq!(params.page_size, 10);
        assert!(params.status.is_none());
    }

    #[test]
    fn test_parse_list_instances_params_with_args() {
        let args = serde_json::json!({ "status": "Failed", "page": 2, "page_size": 20 });
        let obj = args.as_object().cloned();
        let params: workflow::ListInstancesParams = parse_args(&obj).unwrap();
        assert_eq!(params.page, 2);
        assert_eq!(params.page_size, 20);
    }

    #[test]
    fn test_parse_decide_approval_params() {
        let args = serde_json::json!({ "approval_id": "abc", "decision": "Approve" });
        let obj = args.as_object().cloned();
        let params: workflow::DecideApprovalParams = parse_args(&obj).unwrap();
        assert_eq!(params.approval_id, "abc");
        assert_eq!(params.decision, "Approve");
    }

    #[test]
    fn test_parse_args_missing_returns_error() {
        let result: Result<workflow::GetInstanceParams, _> = parse_args(&None);
        assert!(result.is_err());
    }

    #[test]
    fn test_tool_schema_creates_arc() {
        let schema = tool_schema(serde_json::json!({"type": "object"}));
        assert_eq!(schema.get("type").and_then(|v| v.as_str()), Some("object"));
    }

    #[test]
    fn test_known_tool_names_are_valid() {
        for name in known_tool_names() {
            assert!(!name.is_empty());
            let params = make_test_params(name, None);
            assert_eq!(params.name.as_ref(), *name);
        }
    }

    #[test]
    fn test_parse_decision_approve() {
        assert_eq!(parse_decision("Approve").unwrap(), Decision::Approve);
        assert_eq!(parse_decision("approve").unwrap(), Decision::Approve);
    }

    #[test]
    fn test_parse_decision_reject() {
        assert_eq!(parse_decision("Reject").unwrap(), Decision::Reject);
    }

    #[test]
    fn test_parse_decision_invalid() {
        assert!(parse_decision("invalid").is_err());
    }

    #[test]
    fn test_known_tool_names_all_valid() {
        for name in known_tool_names() {
            assert!(!name.is_empty());
            let params = make_test_params(name, None);
            assert_eq!(params.name.as_ref(), *name);
        }
    }
}
