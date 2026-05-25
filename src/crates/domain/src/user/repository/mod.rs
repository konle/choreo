use crate::user::entity::{TenantRole, UserEntity, UserTenantRole};
use async_trait::async_trait;

pub type RepositoryError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create(&self, entity: &UserEntity) -> Result<UserEntity, RepositoryError>;
    async fn get_by_id(&self, user_id: &str) -> Result<UserEntity, RepositoryError>;
    async fn get_by_username(&self, username: &str) -> Result<UserEntity, RepositoryError>;
    async fn update(&self, entity: &UserEntity) -> Result<(), RepositoryError>;
}

#[async_trait]
pub trait UserTenantRoleRepository: Send + Sync {
    async fn assign_role(
        &self,
        user_id: &str,
        tenant_id: &str,
        role: &TenantRole,
    ) -> Result<UserTenantRole, RepositoryError>;
    async fn get_role(
        &self,
        user_id: &str,
        tenant_id: &str,
    ) -> Result<UserTenantRole, RepositoryError>;
    async fn list_by_tenant(&self, tenant_id: &str)
    -> Result<Vec<UserTenantRole>, RepositoryError>;
    async fn list_by_user(&self, user_id: &str) -> Result<Vec<UserTenantRole>, RepositoryError>;
    async fn remove_role(&self, user_id: &str, tenant_id: &str) -> Result<(), RepositoryError>;
    async fn list_users_by_role(
        &self,
        tenant_id: &str,
        role: &str,
    ) -> Result<Vec<UserTenantRole>, RepositoryError>;
}
