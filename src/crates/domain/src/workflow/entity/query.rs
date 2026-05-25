use chrono::{DateTime, Utc};

use crate::shared::workflow::WorkflowInstanceStatus;
use common::pagination::{Pagination, SortQuery};

#[derive(Debug, Clone)]
pub struct WorkflowInstanceQuery {
    pub tenant_id: String,              // 必须 租户隔离
    pub filter: WorkflowInstanceFilter, // 可选 过滤条件
    pub pagination: Pagination,         // 可选 分页条件
    pub sort: SortQuery,                // 可选 排序条件
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowInstanceFilter {
    pub workflow_meta_id: Option<String>, // 工作流元数据ID
    pub version: Option<u32>,             // 工作流版本号
    pub status: Option<WorkflowInstanceStatus>, // 工作流实例状态
                                          //pub created_at: Option<DateTime<Utc>>, // 创建时间
}
