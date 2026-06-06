use common::pagination::Pagination;
use common::pagination::SortQuery;
use domain::workflow::entity::query::{WorkflowInstanceFilter, WorkflowInstanceQuery};
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListInstancesParams {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub workflow_meta_id: Option<String>,
    #[serde(default)]
    pub version: Option<u32>,
    #[serde(default = "default_page")]
    pub page: u64,
    #[serde(default = "default_page_size")]
    pub page_size: u64,
}

fn default_page() -> u64 {
    1
}
fn default_page_size() -> u64 {
    10
}

impl ListInstancesParams {
    pub fn into_query(self, tenant_id: &str) -> WorkflowInstanceQuery {
        let pagination = Pagination::new(self.page, self.page_size);
        let sort = SortQuery::new(
            SortQuery::default().sort_by,
            SortQuery::default().sort_order,
        );
        let filter = WorkflowInstanceFilter {
            workflow_meta_id: self.workflow_meta_id,
            version: self.version,
            status: self.status.and_then(|s| {
                serde_json::from_str(&format!("\"{}\"", s)).ok()
            }),
        };
        WorkflowInstanceQuery {
            tenant_id: tenant_id.to_string(),
            filter,
            pagination,
            sort,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetInstanceParams {
    pub instance_id: String,
}
