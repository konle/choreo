use common::pagination::{Pagination, SortQuery};
use domain::task::entity::query::{TaskInstanceFilter, TaskInstanceQuery};
use serde::Deserialize;

fn default_page() -> Option<u64> {
    Some(1)
}
fn default_page_size() -> Option<u64> {
    Some(10)
}
#[derive(Deserialize)]
pub struct ListTaskInstancesRequest {
    pub task_id: Option<String>,
    pub status: Option<domain::shared::workflow::TaskInstanceStatus>,
    #[serde(default = "default_page")]
    pub page: Option<u64>,
    #[serde(default = "default_page_size")]
    pub page_size: Option<u64>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
}

impl From<ListTaskInstancesRequest> for TaskInstanceQuery {
    fn from(request: ListTaskInstancesRequest) -> Self {
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
        let filter = TaskInstanceFilter {
            task_id: request.task_id,
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
