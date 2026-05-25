use crate::tenant::entity::TenantEntity;
use async_trait::async_trait;

pub type RepositoryError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
pub trait TenantRepository: Send + Sync {
    async fn create(&self, entity: &TenantEntity) -> Result<TenantEntity, RepositoryError>;
    async fn get_by_id(&self, tenant_id: &str) -> Result<TenantEntity, RepositoryError>;
    async fn list(&self) -> Result<Vec<TenantEntity>, RepositoryError>;
    async fn update(&self, entity: &TenantEntity) -> Result<(), RepositoryError>;
    async fn delete(&self, tenant_id: &str) -> Result<(), RepositoryError>;
}
