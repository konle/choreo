use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserStatus {
    Active,
    Disabled,
}

impl Display for UserStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum TenantRole {
    TenantAdmin,
    Developer,
    Operator,
    Viewer,
}

impl Display for TenantRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TenantRole {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "TenantAdmin" => Some(TenantRole::TenantAdmin),
            "Developer" => Some(TenantRole::Developer),
            "Operator" => Some(TenantRole::Operator),
            "Viewer" => Some(TenantRole::Viewer),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Permission {
    TenantManage,
    UserManage,
    TemplateWrite,
    InstanceExecute,
    ApprovalAdmin,
    ApprovalDecide,
    ReadOnly,
}

impl TenantRole {
    pub fn has_permission(&self, perm: &Permission) -> bool {
        match perm {
            Permission::TenantManage => false,
            Permission::UserManage => matches!(self, TenantRole::TenantAdmin),
            Permission::TemplateWrite => {
                matches!(self, TenantRole::TenantAdmin | TenantRole::Developer)
            }
            Permission::InstanceExecute | Permission::ApprovalDecide => matches!(
                self,
                TenantRole::TenantAdmin | TenantRole::Developer | TenantRole::Operator
            ),
            Permission::ApprovalAdmin => matches!(self, TenantRole::TenantAdmin),
            Permission::ReadOnly => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEntity {
    pub user_id: String,
    pub username: String,
    pub email: String,
    pub password_hash: String,
    pub is_super_admin: bool,
    pub status: UserStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTenantRole {
    pub user_id: String,
    pub tenant_id: String,
    pub role: TenantRole,
    pub created_at: DateTime<Utc>,
}
