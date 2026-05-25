use crate::shared::workflow::TaskInstanceStatus;
use common::pagination::{Pagination, SortQuery};
#[derive(Debug, Clone)]
pub struct TaskInstanceQuery {
    pub tenant_id: String,          // 必须 租户隔离
    pub filter: TaskInstanceFilter, // 可选 过滤条件
    pub pagination: Pagination,     // 可选 分页条件
    pub sort: SortQuery,            // 可选 排序条件
}

#[derive(Debug, Clone, Default)]
pub struct TaskInstanceFilter {
    pub task_id: Option<String>, // 任务ID
    pub status: Option<TaskInstanceStatus>, // 工作流实例状态
                                 //pub created_at: Option<DateTime<Utc>>, // 创建时间
}
