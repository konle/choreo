use crate::shared::form::Form;
use crate::shared::workflow::{TaskInstanceStatus, TaskStatus, TaskType};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt::{self, Display};

// 任务模板枚举 用于表示任务的模板 如http、grpc、审批等
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TaskTemplate {
    Http(TaskHttpTemplate),
    Grpc,
    Approval(ApprovalTemplate),
    IfCondition(IfConditionTemplate),
    ContextRewrite(ContextRewriteTemplate),
    Parallel(ParallelTemplate),
    ForkJoin(ForkJoinTemplate),
    SubWorkflow(SubWorkflowTemplate),
    Pause(PauseTemplate),
    Llm(LlmTemplate),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParallelMode {
    /// 滚动执行（类似线程池）：只要有完成的，就立刻从队列里拿新的补上，保持并发度跑满
    Rolling,
    /// 批量执行（类似分批）：以 `concurrency` 为一批，这批全部完成后，再启动下一批
    Batch,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParallelTemplate {
    /// 迭代目标路径 (例如: "data.users")，引擎会用类似 JsonPath 提取出一个 JSON 数组
    pub items_path: String,

    /// 迭代变量名 (例如: "user")。在子任务的模板中，可以通过 {{ user.name }} 和 {{ user.age }} 引用
    pub item_alias: String,
    /// 真正要被重复执行的那个原子任务的模板配置
    pub task_template: Box<TaskTemplate>,
    /// 并发度，即同时最多有多少个子任务在执行，默认为 10
    pub concurrency: u32,

    /// 并发模式：滚动(Rolling) 或 批量(Batch)
    pub mode: ParallelMode,
    /// 最大容忍的失败数量。
    /// - 若设为 0，则任何一个子任务失败，整个并发容器立刻判定为失败，并尝试取消其他进行中的任务。
    /// - 若设为 N，则允许最多 N 个任务失败。超过 N 时整体失败。
    /// - 若为 None，则无论失败多少个，都坚持执行完所有任务。
    pub max_failures: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkJoinTemplate {
    pub tasks: Vec<ForkJoinTaskItem>,
    pub concurrency: u32,
    pub mode: ParallelMode,
    pub max_failures: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkJoinTaskItem {
    pub task_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub name: String,
    pub task_template: TaskTemplate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubWorkflowTemplate {
    pub workflow_meta_id: String,
    pub workflow_version: u32,
    #[serde(default)]
    pub form: Vec<Form>,
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PauseMode {
    Auto,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PauseTemplate {
    pub wait_seconds: u64,
    pub mode: PauseMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LlmResponseFormat {
    Text,
    JsonObject,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmTemplate {
    pub base_url: String,
    pub model: String,
    pub api_key_ref: String,
    pub system_prompt: Option<String>,
    pub user_prompt: String,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u32>,
    pub timeout: u32,
    pub retry_count: u32,
    pub retry_delay: u32,
    pub response_format: Option<LlmResponseFormat>,
    #[serde(default)]
    pub form: Vec<Form>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskHttpTemplate {
    pub url: String,
    pub method: HttpMethod,
    #[serde(default)]
    pub headers: Vec<Form>,
    #[serde(default)]
    pub body: Vec<Form>,
    #[serde(default)]
    pub form: Vec<Form>,
    pub retry_count: u32,
    pub retry_delay: u32,
    pub timeout: u32,
    pub success_condition: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IfConditionTemplate {
    pub condition: String,
    pub name: String,
    pub then_task: Option<String>,
    pub else_task: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeMode {
    Merge,
    Replace,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRewriteTemplate {
    pub name: String,
    pub script: String,
    #[serde(default = "default_merge_mode")]
    pub merge_mode: MergeMode,
}

fn default_merge_mode() -> MergeMode {
    MergeMode::Merge
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalTemplate {
    pub name: String,
    pub title: String,
    pub description: Option<String>,
    pub approvers: Vec<ApproverRule>,
    #[serde(default = "default_approval_mode")]
    pub approval_mode: ApprovalMode,
    pub timeout: Option<u64>,
    #[serde(default = "default_self_approval_policy")]
    pub self_approval: SelfApprovalPolicy,
}

fn default_approval_mode() -> ApprovalMode {
    ApprovalMode::Any
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelfApprovalPolicy {
    Allow,
    Skip,
}

fn default_self_approval_policy() -> SelfApprovalPolicy {
    SelfApprovalPolicy::Skip
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ApproverRule {
    User(String),
    Role(String),
    ContextVariable(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApprovalMode {
    Any,
    All,
    Majority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Head,
}

impl Display for TaskTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TaskTemplate {
    /// Task type of this template leaf (used when materializing child task instances, e.g. Parallel/ForkJoin).
    pub fn task_type(&self) -> TaskType {
        match self {
            TaskTemplate::Http(_) => TaskType::Http,
            TaskTemplate::Grpc => TaskType::Grpc,
            TaskTemplate::Approval(_) => TaskType::Approval,
            TaskTemplate::IfCondition(_) => TaskType::IfCondition,
            TaskTemplate::ContextRewrite(_) => TaskType::ContextRewrite,
            TaskTemplate::Parallel(_) => TaskType::Parallel,
            TaskTemplate::ForkJoin(_) => TaskType::ForkJoin,
            TaskTemplate::SubWorkflow(_) => TaskType::SubWorkflow,
            TaskTemplate::Pause(_) => TaskType::Pause,
            TaskTemplate::Llm(_) => TaskType::Llm,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskEntity {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub task_type: TaskType,
    pub task_template: TaskTemplate,
    pub description: String,
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl TaskEntity {
    pub fn new(
        id: String,
        tenant_id: String,
        name: String,
        task_type: TaskType,
        task_template: TaskTemplate,
        description: String,
        status: TaskStatus,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        deleted_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            tenant_id,
            name,
            task_type,
            task_template,
            description,
            status,
            created_at,
            updated_at,
            deleted_at,
        }
    }
}

use crate::shared::job::WorkflowCallerContext;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskTransitionFields {
    pub output: Option<JsonValue>,
    pub input: Option<JsonValue>,
    pub error_message: Option<String>,
    pub execution_duration: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskInstanceEntity {
    pub id: String,
    pub tenant_id: String,
    pub task_id: String,
    pub task_name: String,
    pub task_type: TaskType,
    pub task_template: TaskTemplate,
    pub task_status: TaskInstanceStatus,
    pub task_instance_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub input: Option<JsonValue>, // 保存请求的输入数据 比如HTTP渲染之后的http请求等
    pub output: Option<JsonValue>, // 保存请求响应的结果
    pub error_message: Option<String>,
    pub execution_duration: Option<u64>, // 执行时间 单位：毫秒
    pub caller_context: Option<WorkflowCallerContext>,
}
