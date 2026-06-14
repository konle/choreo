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
use std::future::Future;
use std::pin::Pin;
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

type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<CallToolResult, ErrorData>> + Send + 'a>>;
type ToolFn = for<'a> fn(&'a application::auth::context::AuthContext, &'a CallToolRequestParams, &'a McpServer) -> ToolFuture<'a>;

mod tool_handlers {
    use super::*;

    pub(super) fn cancel_workflow_instance<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_cancel(a, r, s))
    }
    pub(super) fn decide_approval<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_decide_approval(a, r, s))
    }
    pub(super) fn execute_task<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_execute_task(a, r, s))
    }
    pub(super) fn execute_workflow_instance<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_execute(a, r, s))
    }
    pub(super) fn get_task_instance<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_get_task(a, r, s))
    }
    pub(super) fn get_workflow_definition<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_get_def(a, r, s))
    }
    pub(super) fn get_workflow_instance<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_get_instance(a, r, s))
    }
    pub(super) fn list_approvals<'a>(a: &'a application::auth::context::AuthContext, _r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_list_approvals(a, s))
    }
    pub(super) fn list_task_instances<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_list_tasks(a, r, s))
    }
    pub(super) fn list_workflow_definitions<'a>(a: &'a application::auth::context::AuthContext, _r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_list_defs(a, s))
    }
    pub(super) fn list_workflow_instances<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_list_instances(a, r, s))
    }
    pub(super) fn retry_task_instance<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_retry_task(a, r, s))
    }
    pub(super) fn retry_workflow_instance<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_retry(a, r, s))
    }
    pub(super) fn skip_workflow_node<'a>(a: &'a application::auth::context::AuthContext, r: &'a CallToolRequestParams, s: &'a McpServer) -> ToolFuture<'a> {
        Box::pin(super::tool_skip_node(a, r, s))
    }
}

static TOOLS: &[(&str, ToolFn)] = &[
    ("cancel_workflow_instance", tool_handlers::cancel_workflow_instance as ToolFn),
    ("decide_approval", tool_handlers::decide_approval as ToolFn),
    ("execute_task", tool_handlers::execute_task as ToolFn),
    ("execute_workflow_instance", tool_handlers::execute_workflow_instance as ToolFn),
    ("get_task_instance", tool_handlers::get_task_instance as ToolFn),
    ("get_workflow_definition", tool_handlers::get_workflow_definition as ToolFn),
    ("get_workflow_instance", tool_handlers::get_workflow_instance as ToolFn),
    ("list_approvals", tool_handlers::list_approvals as ToolFn),
    ("list_task_instances", tool_handlers::list_task_instances as ToolFn),
    ("list_workflow_definitions", tool_handlers::list_workflow_definitions as ToolFn),
    ("list_workflow_instances", tool_handlers::list_workflow_instances as ToolFn),
    ("retry_task_instance", tool_handlers::retry_task_instance as ToolFn),
    ("retry_workflow_instance", tool_handlers::retry_workflow_instance as ToolFn),
    ("skip_workflow_node", tool_handlers::skip_workflow_node as ToolFn),
];

async fn dispatch_tool_call(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let name = request.name.as_ref();
    match TOOLS.binary_search_by_key(&name, |(n, _)| n) {
        Ok(i) => TOOLS[i].1(auth, request, server).await,
        Err(_) => Err(ErrorData::invalid_params(format!("unknown tool: {}", name), None)),
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

async fn tool_execute_task(
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
    server: &McpServer,
) -> Result<CallToolResult, ErrorData> {
    let p: workflow::ExecuteTaskParams = parse_args(&request.arguments)?;
    let r = server.task_usecase.execute_task_by_name(auth, &p.task_name, p.context).await
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
                // Task execution
                Tool::new(
                    "execute_task",
                    "Create and execute a task instance by task name",
                    tool_schema(serde_json::json!({
                        "type": "object",
                        "properties": {
                            "task_name": { "type": "string", "description": "Name of the task template to execute" },
                            "context": { "type": "object", "description": "Optional input context for the task" }
                        },
                        "required": ["task_name"]
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

    fn all_tool_names() -> &'static [&'static str] {
        &[
            "cancel_workflow_instance",
            "decide_approval",
            "execute_task",
            "execute_workflow_instance",
            "get_task_instance",
            "get_workflow_definition",
            "get_workflow_instance",
            "list_approvals",
            "list_task_instances",
            "list_workflow_definitions",
            "list_workflow_instances",
            "retry_task_instance",
            "retry_workflow_instance",
            "skip_workflow_node",
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
    fn test_tools_table_sorted() {
        for i in 1..TOOLS.len() {
            assert!(TOOLS[i - 1].0 < TOOLS[i].0, "TOOLS[{}]={} >= TOOLS[{}]={}", i - 1, TOOLS[i - 1].0, i, TOOLS[i].0);
        }
    }

    #[test]
    fn test_all_tool_names_found() {
        for name in all_tool_names() {
            assert!(TOOLS.binary_search_by_key(name, |(n, _)| n).is_ok(), "tool not found: {}", name);
        }
    }

    #[test]
    fn test_unknown_tool_not_found() {
        assert!(TOOLS.binary_search_by_key(&"nonexistent_tool", |(n, _)| n).is_err());
    }

    #[test]
    fn test_tools_count_matches_tool_list() {
        // list_tools() registers 15 tools (14 names + ... no, check count)
        // Verify TOOLS has exactly 14 entries
        assert_eq!(TOOLS.len(), 14);
        assert_eq!(all_tool_names().len(), 14);
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
    fn test_all_tool_names_valid_params() {
        for name in all_tool_names() {
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
}
