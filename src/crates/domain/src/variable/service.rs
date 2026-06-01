use chrono::Utc;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

use crate::variable::entity::{VariableEntity, VariableScope, VariableType};
use crate::variable::repository::{RepositoryError, VariableRepository};

const SECRET_MASK: &str = "******";

pub fn merge_json_into_map(
    map: &mut serde_json::Map<String, JsonValue>,
    source: &JsonValue,
) {
    if let Some(obj) = source.as_object() {
        for (k, v) in obj {
            map.insert(k.clone(), v.clone());
        }
    }
}

#[derive(Clone)]
pub struct VariableService {
    pub repository: Arc<dyn VariableRepository>,
    encrypt_key: Arc<String>,
}

pub fn parse_variable_value(value_type: &VariableType, raw: &str) -> Result<JsonValue, String> {
    match value_type {
        VariableType::String | VariableType::Secret => Ok(JsonValue::String(raw.to_string())),
        VariableType::Number => {
            let n: f64 = raw.parse().map_err(|_| "value is not a valid number".to_string())?;
            Ok(serde_json::json!(n))
        }
        VariableType::Bool => {
            let b: bool = raw.parse().map_err(|_| "value is not a valid bool".to_string())?;
            Ok(JsonValue::Bool(b))
        }
        VariableType::Json => {
            let v: JsonValue = serde_json::from_str(raw)
                .map_err(|e| format!("value is not valid JSON: {}", e))?;
            Ok(v)
        }
    }
}

impl VariableService {
    pub fn new(repository: Arc<dyn VariableRepository>, encrypt_key: String) -> Self {
        Self {
            repository,
            encrypt_key: Arc::new(encrypt_key),
        }
    }

    pub async fn create(
        &self,
        mut entity: VariableEntity,
    ) -> Result<VariableEntity, RepositoryError> {
        entity.id = Uuid::new_v4().to_string();
        entity.created_at = Utc::now();
        entity.updated_at = Utc::now();

        if let Some(existing) = self
            .repository
            .get_by_key(
                &entity.tenant_id,
                &entity.scope,
                &entity.scope_id,
                &entity.key,
            )
            .await?
        {
            return Err(format!(
                "variable key '{}' already exists in scope {:?}/{} (id={})",
                entity.key, entity.scope, entity.scope_id, existing.id
            )
            .into());
        }

        if entity.variable_type.is_secret() {
            entity.value = self.encrypt(&entity.value)?;
        }

        self.repository.create(&entity).await
    }

    pub async fn get_by_id(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<VariableEntity, RepositoryError> {
        let mut entity = self.repository.get_by_id(tenant_id, id).await?;
        if entity.variable_type.is_secret() {
            entity.value = SECRET_MASK.to_string();
        }
        Ok(entity)
    }

    pub async fn update(
        &self,
        mut entity: VariableEntity,
    ) -> Result<VariableEntity, RepositoryError> {
        entity.updated_at = Utc::now();
        if entity.variable_type.is_secret() {
            entity.value = self.encrypt(&entity.value)?;
        }
        self.repository.update(&entity).await
    }

    pub async fn delete(&self, tenant_id: &str, id: &str) -> Result<(), RepositoryError> {
        self.repository.delete(tenant_id, id).await
    }

    pub async fn list_by_scope(
        &self,
        tenant_id: &str,
        scope: &VariableScope,
        scope_id: &str,
    ) -> Result<Vec<VariableEntity>, RepositoryError> {
        let mut entities = self
            .repository
            .list_by_scope(tenant_id, scope, scope_id)
            .await?;
        for e in &mut entities {
            if e.variable_type.is_secret() {
                e.value = SECRET_MASK.to_string();
            }
        }
        Ok(entities)
    }

    /// Merge all variable scopes into a single JSON context.
    /// Priority (low → high): tenant vars → workflow meta vars → instance context → node context.
    pub async fn resolve_variables(
        &self,
        tenant_id: &str,
        workflow_meta_id: &str,
        instance_context: &JsonValue,
        node_context: &JsonValue,
    ) -> Result<JsonValue, RepositoryError> {
        let mut merged = serde_json::Map::new();

        let tenant_vars = self
            .repository
            .list_by_scope(tenant_id, &VariableScope::Tenant, tenant_id)
            .await?;
        for var in tenant_vars {
            let val = self.to_json_value(&var)?;
            merged.insert(var.key, val);
        }

        let meta_vars = self
            .repository
            .list_by_scope(tenant_id, &VariableScope::WorkflowMeta, workflow_meta_id)
            .await?;
        for var in meta_vars {
            let val = self.to_json_value(&var)?;
            merged.insert(var.key, val);
        }

        merge_json_into_map(&mut merged, instance_context);
        merge_json_into_map(&mut merged, node_context);

        Ok(JsonValue::Object(merged))
    }

    /// Resolve variables for standalone task execution (no workflow context).
    /// Merges: tenant variables → user-provided context (overwrites).
    pub async fn resolve_standalone_context(
        &self,
        tenant_id: &str,
        user_context: &JsonValue,
    ) -> Result<JsonValue, RepositoryError> {
        let mut merged = serde_json::Map::new();

        let tenant_vars = self
            .repository
            .list_by_scope(tenant_id, &VariableScope::Tenant, tenant_id)
            .await?;
        for var in tenant_vars {
            let val = self.to_json_value(&var)?;
            merged.insert(var.key, val);
        }

        merge_json_into_map(&mut merged, user_context);

        Ok(JsonValue::Object(merged))
    }

    fn to_json_value(&self, var: &VariableEntity) -> Result<JsonValue, RepositoryError> {
        let raw = if var.variable_type.is_secret() {
            self.decrypt(&var.value)?
        } else {
            var.value.clone()
        };
        parse_variable_value(&var.variable_type, &raw).map_err(RepositoryError::from)
    }

    fn encrypt(&self, plaintext: &str) -> Result<String, RepositoryError> {
        use aes_gcm::aead::rand_core::RngCore;
        use aes_gcm::aead::{Aead, KeyInit, OsRng};
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;

        let key_bytes = self.derive_key();
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);

        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|e| format!("encryption failed: {}", e))?;

        let mut combined = Vec::with_capacity(12 + ciphertext.len());
        combined.extend_from_slice(&nonce_bytes);
        combined.extend_from_slice(&ciphertext);

        Ok(STANDARD.encode(&combined))
    }

    fn decrypt(&self, encoded: &str) -> Result<String, RepositoryError> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;

        let combined = STANDARD
            .decode(encoded)
            .map_err(|e| format!("base64 decode failed: {}", e))?;

        if combined.len() < 12 {
            return Err("invalid encrypted data".into());
        }

        let (nonce_bytes, ciphertext) = combined.split_at(12);
        let key_bytes = self.derive_key();
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);

        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| format!("decryption failed: {}", e))?;

        String::from_utf8(plaintext)
            .map_err(|e| format!("decrypted data is not valid UTF-8: {}", e).into())
    }

    fn derive_key(&self) -> [u8; 32] {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut key = [0u8; 32];
        let src = self.encrypt_key.as_bytes();
        if src.len() >= 32 {
            key.copy_from_slice(&src[..32]);
        } else {
            // Stretch short keys via repeated hashing
            let mut hasher = DefaultHasher::new();
            src.hash(&mut hasher);
            let h1 = hasher.finish().to_le_bytes();
            src.hash(&mut hasher);
            let h2 = hasher.finish().to_le_bytes();
            src.hash(&mut hasher);
            let h3 = hasher.finish().to_le_bytes();
            src.hash(&mut hasher);
            let h4 = hasher.finish().to_le_bytes();
            key[..8].copy_from_slice(&h1);
            key[8..16].copy_from_slice(&h2);
            key[16..24].copy_from_slice(&h3);
            key[24..32].copy_from_slice(&h4);
        }
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variable::entity::VariableType;

    #[test]
    fn parse_string_value() {
        let result = parse_variable_value(&VariableType::String, "hello").unwrap();
        assert_eq!(result, serde_json::json!("hello"));
    }

    #[test]
    fn parse_secret_value() {
        let result = parse_variable_value(&VariableType::Secret, "s3cret").unwrap();
        assert_eq!(result, serde_json::json!("s3cret"));
    }

    #[test]
    fn parse_number_valid() {
        let result = parse_variable_value(&VariableType::Number, "42.5").unwrap();
        assert_eq!(result, serde_json::json!(42.5));
    }

    #[test]
    fn parse_number_invalid() {
        let result = parse_variable_value(&VariableType::Number, "abc");
        assert!(result.is_err());
    }

    #[test]
    fn parse_bool_valid() {
        let result = parse_variable_value(&VariableType::Bool, "true").unwrap();
        assert_eq!(result, serde_json::json!(true));
    }

    #[test]
    fn parse_bool_invalid() {
        let result = parse_variable_value(&VariableType::Bool, "yes");
        assert!(result.is_err());
    }

    #[test]
    fn parse_json_valid() {
        let result = parse_variable_value(&VariableType::Json, r#"{"a":1}"#).unwrap();
        assert_eq!(result, serde_json::json!({"a": 1}));
    }

    #[test]
    fn parse_json_invalid() {
        let result = parse_variable_value(&VariableType::Json, "not json");
        assert!(result.is_err());
    }

    #[test]
    fn merge_json_into_map_inserts_keys() {
        let mut map = serde_json::Map::new();
        merge_json_into_map(&mut map, &serde_json::json!({"a": 1, "b": "x"}));
        assert_eq!(map.len(), 2);
        assert_eq!(map["a"], serde_json::json!(1));
        assert_eq!(map["b"], serde_json::json!("x"));
    }

    #[test]
    fn merge_json_into_map_non_object_noop() {
        let mut map = serde_json::Map::new();
        merge_json_into_map(&mut map, &serde_json::json!("not_an_object"));
        assert!(map.is_empty());
    }
}
