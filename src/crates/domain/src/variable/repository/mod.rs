use crate::variable::entity::{VariableEntity, VariableScope};
use async_trait::async_trait;
use std::error::Error;

pub type RepositoryError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait VariableRepository: Send + Sync {
    async fn create(&self, entity: &VariableEntity) -> Result<VariableEntity, RepositoryError>;
    async fn get_by_id(&self, tenant_id: &str, id: &str)
    -> Result<VariableEntity, RepositoryError>;
    async fn update(&self, entity: &VariableEntity) -> Result<VariableEntity, RepositoryError>;
    async fn delete(&self, tenant_id: &str, id: &str) -> Result<(), RepositoryError>;

    async fn list_by_scope(
        &self,
        tenant_id: &str,
        scope: &VariableScope,
        scope_id: &str,
    ) -> Result<Vec<VariableEntity>, RepositoryError>;

    async fn get_by_key(
        &self,
        tenant_id: &str,
        scope: &VariableScope,
        scope_id: &str,
        key: &str,
    ) -> Result<Option<VariableEntity>, RepositoryError>;
}
