use std::fmt;

use crate::shared::form::Form;
use crate::shared::job::WorkflowCallerContext;
use crate::shared::workflow::WorkflowStatus;
use crate::shared::workflow::{TaskType, WorkflowInstanceStatus};
use crate::task::entity::task_definition::{TaskInstanceEntity, TaskTemplate};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowMetaEntity {
    pub workflow_meta_id: String, // 工作流模板ID workflow_meta_id 组成唯一性索引
    pub tenant_id: String,        // 租户ID tenant_id + workflow_meta_id 组成索引
    pub name: String,
    pub description: String,               // 工作流模板描述
    pub created_at: DateTime<Utc>,         // 创建时间
    pub updated_at: DateTime<Utc>,         // 更新时间
    pub deleted_at: Option<DateTime<Utc>>, // 删除时间
    pub status: WorkflowStatus,            // 工作流模板状态
    pub form: Vec<Form>,                   // 工作流模板表单
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEntity {
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub version: u32,             // 工作流版本
    pub workflow_meta_id: String, // 工作流模板ID + version组成唯一性索引
    pub status: WorkflowStatus,
    pub entry_node: String, // 入口节点
    pub nodes: Vec<WorkflowNodeEntity>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowNodeEntity {
    pub node_id: String,
    pub node_type: TaskType,
    #[serde(default)]
    pub task_id: Option<String>,
    pub config: TaskTemplate,
    pub context: JsonValue,
    pub next_node: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowInstanceEntity {
    pub workflow_instance_id: String,
    pub tenant_id: String,
    pub workflow_meta_id: String,
    pub workflow_version: u32,
    pub status: WorkflowInstanceStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub context: JsonValue,
    pub entry_node: String,   // 入口节点
    pub current_node: String, // 当前节点
    pub nodes: Vec<WorkflowNodeInstanceEntity>,
    pub epoch: u64,
    pub locked_by: Option<String>,
    pub locked_duration: Option<u64>, // milliseconds
    pub locked_at: Option<DateTime<Utc>>,
    pub parent_context: Option<WorkflowCallerContext>,
    pub depth: u32,
    #[serde(default)]
    pub created_by: Option<String>,
}

impl fmt::Display for WorkflowInstanceEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WorkflowInstanceEntity(instance_id={}, tenant_id={}, workflow_meta_id={}, version={}, status={:?}, current_node={}, entry_node={}, epoch={}, depth={}, nodes={:?})",
            self.workflow_instance_id,
            self.tenant_id,
            self.workflow_meta_id,
            self.workflow_version,
            self.status,
            self.current_node,
            self.entry_node,
            self.epoch,
            self.depth,
            self.nodes
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeExecutionStatus {
    Pending,
    Running,
    Success,
    Failed,
    Suspended,
    Await,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowNodeInstanceEntity {
    pub node_id: String,
    pub node_type: TaskType,
    pub task_instance: TaskInstanceEntity,
    pub context: JsonValue,
    pub next_node: Option<String>,
    pub status: NodeExecutionStatus,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl WorkflowInstanceEntity {
    pub fn get_current_node(&self) -> String {
        if self.current_node.is_empty() {
            self.entry_node.clone()
        } else {
            self.current_node.clone()
        }
    }
    pub fn get_node_by_id(&self, node_id: &str) -> Option<&WorkflowNodeInstanceEntity> {
        self.nodes.iter().find(|node| node.node_id == node_id)
    }

    pub fn is_completed(&self) -> bool {
        self.status == WorkflowInstanceStatus::Completed
    }
    pub fn is_failed(&self) -> bool {
        self.status == WorkflowInstanceStatus::Failed
    }
    pub fn is_suspended(&self) -> bool {
        self.status == WorkflowInstanceStatus::Suspended
    }
    pub fn is_canceled(&self) -> bool {
        self.status == WorkflowInstanceStatus::Canceled
    }
    pub fn is_running(&self) -> bool {
        self.status == WorkflowInstanceStatus::Running
    }
    pub fn is_pending(&self) -> bool {
        self.status == WorkflowInstanceStatus::Pending
    }
}
