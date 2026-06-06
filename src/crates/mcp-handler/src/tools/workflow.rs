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

impl Default for ListInstancesParams {
    fn default() -> Self {
        Self {
            status: None,
            workflow_meta_id: None,
            version: None,
            page: 1,
            page_size: 10,
        }
    }
}

fn default_page() -> u64 {
    1
}
fn default_page_size() -> u64 {
    10
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetInstanceParams {
    pub instance_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExecuteInstanceParams {
    pub instance_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CancelInstanceParams {
    pub instance_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RetryInstanceParams {
    pub instance_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SkipNodeParams {
    pub instance_id: String,
    pub node_id: String,
    #[serde(default)]
    pub output: serde_json::Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetDefinitionParams {
    pub meta_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TaskInstanceParams {
    pub instance_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListTaskInstancesParams {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub page: u64,
    #[serde(default = "default_page_size")]
    pub page_size: u64,
}

impl Default for ListTaskInstancesParams {
    fn default() -> Self {
        Self {
            status: None,
            page: 1,
            page_size: 10,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExecuteTaskParams {
    pub task_name: String,
    #[serde(default)]
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DecideApprovalParams {
    pub approval_id: String,
    pub decision: String,
    #[serde(default)]
    pub comment: Option<String>,
}
