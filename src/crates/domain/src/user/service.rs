use crate::user::entity::{TenantRole, UserEntity, UserStatus, UserTenantRole};
use crate::user::repository::{RepositoryError, UserRepository, UserTenantRoleRepository};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct UserService {
    pub user_repo: Arc<dyn UserRepository>,
    pub role_repo: Arc<dyn UserTenantRoleRepository>,
}

impl UserService {
    pub fn new(
        user_repo: Arc<dyn UserRepository>,
        role_repo: Arc<dyn UserTenantRoleRepository>,
    ) -> Self {
        Self {
            user_repo,
            role_repo,
        }
    }

    pub async fn create_user(
        &self,
        username: String,
        email: String,
        password_hash: String,
        is_super_admin: bool,
    ) -> Result<UserEntity, RepositoryError> {
        if self.user_repo.get_by_username(&username).await.is_ok() {
            return Err(format!("Username already exists: {}", username).into());
        }
        let now = Utc::now();
        let entity = UserEntity {
            user_id: Uuid::new_v4().to_string(),
            username,
            email,
            password_hash,
            is_super_admin,
            status: UserStatus::Active,
            created_at: now,
            updated_at: now,
        };
        self.user_repo.create(&entity).await
    }

    pub async fn get_user(&self, user_id: &str) -> Result<UserEntity, RepositoryError> {
        self.user_repo.get_by_id(user_id).await
    }

    pub async fn get_user_by_username(
        &self,
        username: &str,
    ) -> Result<UserEntity, RepositoryError> {
        self.user_repo.get_by_username(username).await
    }

    pub async fn change_password(
        &self,
        user_id: &str,
        new_password_hash: String,
    ) -> Result<(), RepositoryError> {
        let mut user = self.user_repo.get_by_id(user_id).await?;
        user.password_hash = new_password_hash;
        user.updated_at = Utc::now();
        self.user_repo.update(&user).await
    }

    pub async fn assign_role(
        &self,
        user_id: &str,
        tenant_id: &str,
        role: &TenantRole,
    ) -> Result<UserTenantRole, RepositoryError> {
        self.role_repo.assign_role(user_id, tenant_id, role).await
    }

    pub async fn get_role(
        &self,
        user_id: &str,
        tenant_id: &str,
    ) -> Result<UserTenantRole, RepositoryError> {
        self.role_repo.get_role(user_id, tenant_id).await
    }

    pub async fn list_tenant_users(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<UserTenantRole>, RepositoryError> {
        self.role_repo.list_by_tenant(tenant_id).await
    }

    pub async fn list_user_tenants(
        &self,
        user_id: &str,
    ) -> Result<Vec<UserTenantRole>, RepositoryError> {
        self.role_repo.list_by_user(user_id).await
    }

    pub async fn remove_role(&self, user_id: &str, tenant_id: &str) -> Result<(), RepositoryError> {
        self.role_repo.remove_role(user_id, tenant_id).await
    }
}
