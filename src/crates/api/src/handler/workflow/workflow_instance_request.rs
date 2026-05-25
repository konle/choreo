use common::pagination::{Pagination, SortQuery};
use domain::workflow::entity::query::{WorkflowInstanceFilter, WorkflowInstanceQuery};
use serde::Deserialize;
use serde_json::Value as JsonValue;
#[derive(Deserialize)]
pub struct SkipWorkflowNodeRequest {
    pub node_id: String,
    #[serde(default)]
    pub child_task_id: Option<String>,
    pub output: JsonValue,
}

#[derive(Deserialize)]
pub struct RetryWorkflowNodeRequest {
    pub node_id: String,
    #[serde(default)]
    pub child_task_id: Option<String>,
}

#[derive(Deserialize)]
pub struct ResumeNodeRequest {
    pub node_id: String,
}

#[derive(Deserialize)]
pub struct CreateWorkflowInstanceRequest {
    pub workflow_meta_id: String,
    pub version: u32,
    #[serde(default)]
    pub context: JsonValue,
}

fn default_page() -> Option<u64> {
    Some(1)
}
fn default_page_size() -> Option<u64> {
    Some(10)
}

#[derive(Deserialize)]
pub struct ListWorkflowInstancesRequest {
    pub workflow_meta_id: Option<String>,
    pub version: Option<u32>,
    pub status: Option<domain::shared::workflow::WorkflowInstanceStatus>,
    #[serde(default = "default_page")]
    pub page: Option<u64>,
    #[serde(default = "default_page_size")]
    pub page_size: Option<u64>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
}

impl From<ListWorkflowInstancesRequest> for WorkflowInstanceQuery {
    fn from(request: ListWorkflowInstancesRequest) -> Self {
        let pagination = Pagination::new(
            request.page.unwrap_or(Pagination::default().page),
            request.page_size.unwrap_or(Pagination::default().page_size),
        );
        let sort = SortQuery::new(
            request.sort_by.unwrap_or(SortQuery::default().sort_by),
            request
                .sort_order
                .unwrap_or(SortQuery::default().sort_order),
        );
        let filter = WorkflowInstanceFilter {
            workflow_meta_id: request.workflow_meta_id,
            version: request.version,
            status: request.status,
        };
        Self {
            tenant_id: "".to_string(),
            filter,
            pagination,
            sort,
        }
    }
}
