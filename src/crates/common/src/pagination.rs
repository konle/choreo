use serde::{Deserialize, Serialize};

fn default_page() -> u64 {
    1
}
fn default_page_size() -> u64 {
    10
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Pagination {
    #[serde(default = "default_page")]
    pub page: u64,
    #[serde(default = "default_page_size")]
    pub page_size: u64,
}
// 响应侧：包裹分页结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedData<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub page: u64,
    pub page_size: u64,
}

// 2. 排序请求参数
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SortQuery {
    #[serde(default = "default_sort_field")]
    pub sort_by: String,

    #[serde(default = "default_sort_order")]
    pub sort_order: String, // "asc" 或 "desc"
}

fn default_sort_field() -> String {
    "created_at".to_string()
}
fn default_sort_order() -> String {
    "desc".to_string()
}

impl Default for SortQuery {
    fn default() -> Self {
        Self {
            sort_by: default_sort_field(),
            sort_order: default_sort_order(),
        }
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: default_page(),
            page_size: default_page_size(),
        }
    }
}

impl Pagination {
    pub fn new(page: u64, page_size: u64) -> Self {
        Self { page, page_size }
    }
}

impl SortQuery {
    pub fn new(sort_by: String, sort_order: String) -> Self {
        Self {
            sort_by,
            sort_order,
        }
    }
}
