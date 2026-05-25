use crate::tenant::entity::{TenantEntity, TenantStatus};
use crate::tenant::repository::{RepositoryError, TenantRepository};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct TenantService {
    pub repository: Arc<dyn TenantRepository>,
}

impl TenantService {
    pub fn new(repository: Arc<dyn TenantRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_tenant(
        &self,
        name: String,
        description: String,
    ) -> Result<TenantEntity, RepositoryError> {
        let now = Utc::now();
        let entity = TenantEntity {
            tenant_id: Uuid::new_v4().to_string(),
            name,
            description,
            status: TenantStatus::Active,
            max_workflows: None,
            max_instances: None,
            created_at: now,
            updated_at: now,
        };
        self.repository.create(&entity).await
    }

    pub async fn get_tenant(&self, tenant_id: &str) -> Result<TenantEntity, RepositoryError> {
        self.repository.get_by_id(tenant_id).await
    }

    pub async fn list_tenants(&self) -> Result<Vec<TenantEntity>, RepositoryError> {
        self.repository.list().await
    }

    pub async fn update_tenant(&self, entity: &TenantEntity) -> Result<(), RepositoryError> {
        self.repository.update(entity).await
    }

    pub async fn suspend_tenant(&self, tenant_id: &str) -> Result<(), RepositoryError> {
        let mut entity = self.repository.get_by_id(tenant_id).await?;
        entity.status = TenantStatus::Suspended;
        entity.updated_at = Utc::now();
        self.repository.update(&entity).await
    }

    pub async fn delete_tenant(&self, tenant_id: &str) -> Result<(), RepositoryError> {
        self.repository.delete(tenant_id).await
    }
}
