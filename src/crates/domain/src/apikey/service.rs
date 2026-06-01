use std::sync::Arc;

use chrono::Utc;
use rand::Rng;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::apikey::entity::{ApiKeyEntity, ApiKeyStatus};
use crate::apikey::repository::{ApiKeyRepository, RepositoryError};
use crate::user::entity::TenantRole;

const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

fn hash_api_key(full_key: &str) -> String {
    let digest = Sha256::digest(full_key.as_bytes());
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_consistency() {
        let h1 = hash_api_key("test-key-123");
        let h2 = hash_api_key("test-key-123");
        assert_eq!(h1, h2);
    }
}

fn generate_secret_tail() -> String {
    let mut rng = rand::thread_rng();
    (0..32u8)
        .map(|_| BASE62[rng.gen_range(0..BASE62.len())] as char)
        .collect()
}

#[derive(Clone)]
pub struct ApiKeyService {
    pub repository: Arc<dyn ApiKeyRepository>,
}

impl ApiKeyService {
    pub fn new(repository: Arc<dyn ApiKeyRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_api_key(
        &self,
        tenant_id: &str,
        name: &str,
        role: TenantRole,
        expires_at: Option<chrono::DateTime<Utc>>,
        token_ttl_secs: u32,
        created_by: &str,
    ) -> Result<(ApiKeyEntity, String), RepositoryError> {
        let tail = generate_secret_tail();
        let full_key = format!("wfk_{}", tail);
        let key_prefix = full_key.chars().take(8).collect::<String>();
        let key_hash = hash_api_key(&full_key);
        let now = Utc::now();
        let entity = ApiKeyEntity {
            id: Uuid::new_v4().to_string(),
            tenant_id: tenant_id.to_string(),
            name: name.to_string(),
            key_prefix,
            key_hash,
            role,
            expires_at,
            token_ttl_secs,
            last_used_at: None,
            status: ApiKeyStatus::Active,
            created_by: created_by.to_string(),
            created_at: now,
            updated_at: now,
        };
        self.repository.create(&entity).await?;
        Ok((entity, full_key))
    }

    pub async fn authenticate(&self, full_key: &str) -> Result<ApiKeyEntity, RepositoryError> {
        let prefix: String = full_key.chars().take(8).collect();
        if prefix.len() < 8 {
            return Err("invalid api key".into());
        }
        let entity = self.repository.get_by_prefix(&prefix).await?;
        let h = hash_api_key(full_key);
        if h != entity.key_hash {
            return Err("invalid api key".into());
        }
        if entity.status != ApiKeyStatus::Active {
            return Err("api key is not active".into());
        }
        if let Some(exp) = entity.expires_at {
            if exp < Utc::now() {
                return Err("api key has expired".into());
            }
        }
        let mut updated = entity;
        updated.last_used_at = Some(Utc::now());
        updated.updated_at = Utc::now();
        self.repository.update(&updated).await
    }

    pub async fn revoke(&self, tenant_id: &str, id: &str) -> Result<(), RepositoryError> {
        let mut entity = self.repository.get_by_id(tenant_id, id).await?;
        entity.status = ApiKeyStatus::Revoked;
        entity.updated_at = Utc::now();
        self.repository.update(&entity).await?;
        Ok(())
    }

    pub async fn list(&self, tenant_id: &str) -> Result<Vec<ApiKeyEntity>, RepositoryError> {
        self.repository.list_by_tenant(tenant_id).await
    }
}
