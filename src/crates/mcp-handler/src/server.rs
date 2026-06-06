use application::auth::service::AuthService;
use application::usecase::workflow::WorkflowUsecase;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult, Meta,
    ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ErrorData;
use std::sync::Arc;
use tracing::error;

use crate::tools::workflow;

fn resolve_auth(
    auth_service: &AuthService,
    meta: &Meta,
) -> Result<application::auth::context::AuthContext, ErrorData> {
    if let Some(token_value) = meta.get("authorization") {
        let token = token_value
            .as_str()
            .ok_or_else(|| ErrorData::invalid_params("invalid authorization metadata", None))?;
        let token = token.strip_prefix("Bearer ").unwrap_or(token);
        auth_service
            .verify_token(token)
            .map_err(|e| ErrorData::invalid_params(format!("auth failed: {}", e), None))
    } else {
        Err(ErrorData::invalid_params(
            "missing authorization in MCP meta",
            None,
        ))
    }
}

fn tool_schema(props: serde_json::Value) -> std::sync::Arc<rmcp::model::JsonObject> {
    if let serde_json::Value::Object(map) = props {
        std::sync::Arc::new(map)
    } else {
        std::sync::Arc::new(serde_json::Map::new())
    }
}

#[derive(Clone)]
pub struct McpServer {
    auth_service: Arc<AuthService>,
    workflow_usecase: Arc<WorkflowUsecase>,
}

impl McpServer {
    pub fn new(auth_service: Arc<AuthService>, workflow_usecase: Arc<WorkflowUsecase>) -> Self {
        Self {
            auth_service,
            workflow_usecase,
        }
    }
}

async fn dispatch_tool(
    usecase: &WorkflowUsecase,
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
) -> Result<CallToolResult, ErrorData> {
    match request.name.as_ref() {
        "list_workflow_instances" => handle_list_instances(usecase, auth, request).await,
        _name => Err(ErrorData::invalid_params(
            format!("unknown tool: {}", request.name),
            None,
        )),
    }
}

fn parse_list_instances_params(
    arguments: &Option<rmcp::model::JsonObject>,
) -> Result<workflow::ListInstancesParams, ErrorData> {
    match arguments {
        Some(args) => serde_json::from_value(serde_json::Value::Object(args.clone()))
            .map_err(|e| ErrorData::invalid_params(e.to_string(), None)),
        None => Ok(workflow::ListInstancesParams {
            status: None,
            workflow_meta_id: None,
            version: None,
            page: 1,
            page_size: 10,
        }),
    }
}

async fn handle_list_instances(
    usecase: &WorkflowUsecase,
    auth: &application::auth::context::AuthContext,
    request: &CallToolRequestParams,
) -> Result<CallToolResult, ErrorData> {
    let params = parse_list_instances_params(&request.arguments)?;
    let query = params.into_query(&auth.tenant_id);
    let result = usecase
        .list_instances(auth, query)
        .await
        .map_err(|e| {
            error!(error = %e, "failed to list workflow instances");
            ErrorData::internal_error(e, None)
        })?;

    let content = Content::json(&result)?;
    Ok(CallToolResult::success(vec![content]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_list_instances_params_defaults() {
        let params = parse_list_instances_params(&None).unwrap();
        assert_eq!(params.page, 1);
        assert_eq!(params.page_size, 10);
        assert!(params.status.is_none());
    }

    #[test]
    fn test_parse_list_instances_params_with_args() {
        let args = serde_json::json!({
            "status": "Failed",
            "page": 2,
            "page_size": 20
        });
        let json_obj = args.as_object().cloned();
        let params = parse_list_instances_params(&json_obj).unwrap();
        assert_eq!(params.page, 2);
        assert_eq!(params.page_size, 20);
    }
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
                        "properties": {
                            "instance_id": { "type": "string" }
                        },
                        "required": ["instance_id"]
                    })),
                ),
            ],
            meta: None,
            next_cursor: None,
        }))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, ErrorData>> + Send + '_ {
        async move {
            let auth = resolve_auth(&self.auth_service, &context.meta)?;
            dispatch_tool(&self.workflow_usecase, &auth, &request).await
        }
    }
}
