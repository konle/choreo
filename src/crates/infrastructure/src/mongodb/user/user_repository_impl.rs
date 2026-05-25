use async_trait::async_trait;
use chrono::Utc;
use domain::user::entity::{TenantRole, UserEntity, UserTenantRole};
use domain::user::repository::{RepositoryError, UserRepository, UserTenantRoleRepository};
use mongodb::bson::doc;
use mongodb::{Client, Collection, Database};

pub struct UserRepositoryImpl {
    collection: Collection<UserEntity>,
}

impl UserRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database: Database = client.database("workflow");
        let collection = database.collection("users");
        Self { collection }
    }
}

#[async_trait]
impl UserRepository for UserRepositoryImpl {
    async fn create(&self, entity: &UserEntity) -> Result<UserEntity, RepositoryError> {
        self.collection.insert_one(entity).await?;
        Ok(entity.clone())
    }

    async fn get_by_id(&self, user_id: &str) -> Result<UserEntity, RepositoryError> {
        self.collection
            .find_one(doc! { "user_id": user_id })
            .await?
            .ok_or_else(|| format!("user not found: {}", user_id).into())
    }

    async fn get_by_username(&self, username: &str) -> Result<UserEntity, RepositoryError> {
        self.collection
            .find_one(doc! { "username": username })
            .await?
            .ok_or_else(|| format!("user not found: {}", username).into())
    }

    async fn update(&self, entity: &UserEntity) -> Result<(), RepositoryError> {
        let filter = doc! { "user_id": &entity.user_id };
        self.collection.replace_one(filter, entity).await?;
        Ok(())
    }
}

pub struct UserTenantRoleRepositoryImpl {
    collection: Collection<UserTenantRole>,
}

impl UserTenantRoleRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database: Database = client.database("workflow");
        let collection = database.collection("user_tenant_roles");
        Self { collection }
    }
}

#[async_trait]
impl UserTenantRoleRepository for UserTenantRoleRepositoryImpl {
    async fn assign_role(
        &self,
        user_id: &str,
        tenant_id: &str,
        role: &TenantRole,
    ) -> Result<UserTenantRole, RepositoryError> {
        let filter = doc! { "user_id": user_id, "tenant_id": tenant_id };
        self.collection.delete_many(filter).await?;

        let entity = UserTenantRole {
            user_id: user_id.to_string(),
            tenant_id: tenant_id.to_string(),
            role: role.clone(),
            created_at: Utc::now(),
        };
        self.collection.insert_one(&entity).await?;
        Ok(entity)
    }

    async fn get_role(
        &self,
        user_id: &str,
        tenant_id: &str,
    ) -> Result<UserTenantRole, RepositoryError> {
        self.collection
            .find_one(doc! { "user_id": user_id, "tenant_id": tenant_id })
            .await?
            .ok_or_else(|| {
                format!(
                    "role not found for user {} in tenant {}",
                    user_id, tenant_id
                )
                .into()
            })
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<UserTenantRole>, RepositoryError> {
        use futures::TryStreamExt;
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id })
            .await?;
        Ok(cursor.try_collect().await?)
    }

    async fn list_by_user(&self, user_id: &str) -> Result<Vec<UserTenantRole>, RepositoryError> {
        use futures::TryStreamExt;
        let cursor = self.collection.find(doc! { "user_id": user_id }).await?;
        Ok(cursor.try_collect().await?)
    }

    async fn remove_role(&self, user_id: &str, tenant_id: &str) -> Result<(), RepositoryError> {
        self.collection
            .delete_one(doc! { "user_id": user_id, "tenant_id": tenant_id })
            .await?;
        Ok(())
    }

    async fn list_users_by_role(
        &self,
        tenant_id: &str,
        role: &str,
    ) -> Result<Vec<UserTenantRole>, RepositoryError> {
        use futures::TryStreamExt;
        let cursor = self
            .collection
            .find(doc! { "tenant_id": tenant_id, "role": role })
            .await?;
        Ok(cursor.try_collect().await?)
    }
}
