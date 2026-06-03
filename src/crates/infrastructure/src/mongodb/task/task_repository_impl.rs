use async_trait::async_trait;
use common::pagination::PaginatedData;
use domain::shared::workflow::TaskInstanceStatus;
use domain::task::entity::query::TaskInstanceQuery;
use domain::task::entity::task_definition::{TaskEntity, TaskInstanceEntity, TaskTransitionFields};
use domain::task::repository::{
    RepositoryError, TaskEntityRepository, TaskInstanceEntityRepository,
};
use futures::TryStreamExt;
use mongodb::bson::{Document, doc};
use mongodb::options::FindOptions;
use mongodb::{Client, Collection, Database};

pub struct TaskRepositoryImpl {
    pub client: Client,
    pub database: Database,
    pub collection: Collection<TaskEntity>,
}

pub struct TaskInstanceRepositoryImpl {
    pub client: Client,
    pub database: Database,
    pub collection: Collection<TaskInstanceEntity>,
}

impl TaskRepositoryImpl {
    pub fn new(client: Client) -> Self {
        let database = client.database("workflow");
        let collection = database.collection("tasks");
        Self {
            client,
            database,
            collection,
        }
    }
}

impl TaskInstanceRepositoryImpl {
    // 避免排序注入
    const ALLOWED_SORT_FIELDS: &[&str] = &["created_at", "updated_at", "status", "task_id"];
    fn validate_sort_field(field: &str) -> Result<(), RepositoryError> {
        if !Self::ALLOWED_SORT_FIELDS.contains(&field) {
            return Err(format!("invalid sort field: {}", field).into());
        }
        Ok(())
    }

    fn build_sort_doc(sort_by: &str, sort_order: &str) -> Document {
        let order: i32 = if sort_order == "asc" { 1 } else { -1 };
        doc! { sort_by: order }
    }

    fn build_filter(&self, query: &TaskInstanceQuery) -> Document {
        let mut filter = doc! {"tenant_id": &query.tenant_id};
        if let Some(task_id) = &query.filter.task_id {
            filter.insert("task_id", task_id);
        }
        if let Some(status) = &query.filter.status
            && let Ok(bson_val) = mongodb::bson::to_bson(status)
        {
            filter.insert("task_status", bson_val);
        }
        filter
    }

    pub fn new(client: Client) -> Self {
        let database = client.database("workflow");
        let collection = database.collection("task_instances");
        Self {
            client,
            database,
            collection,
        }
    }

    fn serialize_bson_field(
        value: &serde_json::Value,
        field_name: &str,
    ) -> std::result::Result<mongodb::bson::Bson, String> {
        mongodb::bson::to_bson(value).map_err(|e| format!("serialize {field_name}: {e}"))
    }

    fn insert_serialized_field(
        doc: &mut Document,
        key: &str,
        value: Option<&serde_json::Value>,
    ) -> std::result::Result<(), String> {
        if let Some(v) = value {
            doc.insert(key, Self::serialize_bson_field(v, key)?);
        }
        Ok(())
    }

    fn insert_str_field(doc: &mut Document, key: &str, value: Option<&String>) {
        if let Some(ref v) = value {
            doc.insert(key, v);
        }
    }

    fn insert_i64_field(doc: &mut Document, key: &str, value: Option<i64>) {
        if let Some(v) = value {
            doc.insert(key, v);
        }
    }

    fn build_transfer_set_fields(
        fields: TaskTransitionFields,
        to_bson: &mongodb::bson::Bson,
    ) -> std::result::Result<Document, String> {
        let mut set_fields = doc! {
            "task_status": to_bson,
            "updated_at": chrono::Utc::now().to_rfc3339(),
        };
        Self::insert_serialized_field(&mut set_fields, "output", fields.output.as_ref())?;
        Self::insert_serialized_field(&mut set_fields, "input", fields.input.as_ref())?;
        Self::insert_str_field(&mut set_fields, "error_message", fields.error_message.as_ref());
        Self::insert_i64_field(&mut set_fields, "execution_duration", fields.execution_duration.map(|d| d as i64));
        Ok(set_fields)
    }

    async fn paginated_task_query(
        &self,
        filter: Document,
        page: u64,
        page_size: u64,
        sort_doc: Document,
    ) -> Result<PaginatedData<TaskInstanceEntity>, RepositoryError> {
        let skip = (page - 1) * page_size;
        let total = self.collection.count_documents(filter.clone()).await?;
        let find_options = FindOptions::builder()
            .skip(skip)
            .limit(page_size as i64)
            .sort(sort_doc)
            .build();
        let cursor = self.collection.find(filter).with_options(find_options).await?;
        let items: Vec<TaskInstanceEntity> = cursor.try_collect().await?;
        Ok(PaginatedData { items, total, page, page_size })
    }

    async fn execute_cas_update(
        &self,
        task_instance_id: &str,
        from_status: &TaskInstanceStatus,
        filter: Document,
        update: Document,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        let result = self
            .collection
            .find_one_and_update(filter, update)
            .return_document(mongodb::options::ReturnDocument::After)
            .await?
            .ok_or_else(|| {
                format!(
                    "CAS failed: task instance {} not in expected state {:?}",
                    task_instance_id, from_status
                )
            })?;
        Ok(result)
    }
}

#[async_trait]
impl TaskInstanceEntityRepository for TaskInstanceRepositoryImpl {
    async fn create_task_instance_entity(
        &self,
        task_instance_entity: TaskInstanceEntity,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        self.collection.insert_one(&task_instance_entity).await?;
        Ok(task_instance_entity)
    }

    async fn get_task_instance_entity(
        &self,
        id: String,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        let task_instance_entity = self
            .collection
            .find_one(doc! {"task_instance_id": &id})
            .await?
            .ok_or_else(|| format!("task instance entity not found: {}", id))?;
        Ok(task_instance_entity)
    }

    async fn get_task_instance_entity_scoped(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        let entity = self
            .collection
            .find_one(doc! {"tenant_id": tenant_id, "task_instance_id": id})
            .await?
            .ok_or_else(|| format!("task instance not found: {} in tenant {}", id, tenant_id))?;
        Ok(entity)
    }

    async fn list_task_instance_entities(
        &self,
        query: &TaskInstanceQuery,
    ) -> Result<PaginatedData<TaskInstanceEntity>, RepositoryError> {
        let filter = self.build_filter(query);
        let page = query.pagination.page;
        let page_size = query.pagination.page_size;
        Self::validate_sort_field(&query.sort.sort_by)?;
        let sort_doc = Self::build_sort_doc(&query.sort.sort_by, &query.sort.sort_order);
        self.paginated_task_query(filter, page, page_size, sort_doc).await
    }

    async fn update_task_instance_entity(
        &self,
        task_instance_entity: TaskInstanceEntity,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        let filter = doc! {"task_instance_id": &task_instance_entity.task_instance_id};
        self.collection
            .replace_one(filter, &task_instance_entity)
            .await?;
        Ok(task_instance_entity)
    }

    async fn transfer_status_with_fields(
        &self,
        task_instance_id: &str,
        from_status: &TaskInstanceStatus,
        to_status: &TaskInstanceStatus,
        fields: TaskTransitionFields,
    ) -> Result<TaskInstanceEntity, RepositoryError> {
        let from_bson = Self::serialize_bson_field(
            &serde_json::to_value(from_status).unwrap_or_default(),
            "from_status",
        )?;
        let to_bson = Self::serialize_bson_field(
            &serde_json::to_value(to_status).unwrap_or_default(),
            "to_status",
        )?;

        let filter = doc! {
            "task_instance_id": task_instance_id,
            "task_status": from_bson,
        };
        let set_fields = Self::build_transfer_set_fields(fields, &to_bson)?;
        let update = doc! { "$set": set_fields };

        self.execute_cas_update(task_instance_id, from_status, filter, update).await
    }
}

#[async_trait]
impl TaskEntityRepository for TaskRepositoryImpl {
    async fn create_task_entity(
        &self,
        task_entity: TaskEntity,
    ) -> Result<TaskEntity, RepositoryError> {
        self.collection.insert_one(&task_entity).await?;
        Ok(task_entity)
    }

    async fn get_task_entity(&self, id: String) -> Result<TaskEntity, RepositoryError> {
        let task_entity = self
            .collection
            .find_one(doc! {"id": &id})
            .await?
            .ok_or_else(|| format!("task entity not found: {}", id))?;
        Ok(task_entity)
    }

    async fn get_task_entity_scoped(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<TaskEntity, RepositoryError> {
        let entity = self
            .collection
            .find_one(doc! {"tenant_id": tenant_id, "id": id})
            .await?
            .ok_or_else(|| format!("task entity not found: {} in tenant {}", id, tenant_id))?;
        Ok(entity)
    }

    async fn list_task_entities(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<TaskEntity>, RepositoryError> {
        let cursor = self.collection.find(doc! {"tenant_id": tenant_id}).await?;
        let results: Vec<TaskEntity> = cursor.try_collect().await?;
        Ok(results)
    }

    async fn list_task_entities_by_type(
        &self,
        tenant_id: &str,
        task_type: &str,
    ) -> Result<Vec<TaskEntity>, RepositoryError> {
        let cursor = self
            .collection
            .find(doc! {"tenant_id": tenant_id, "task_type": task_type})
            .await?;
        let results: Vec<TaskEntity> = cursor.try_collect().await?;
        Ok(results)
    }

    async fn update_task_entity(
        &self,
        task_entity: TaskEntity,
    ) -> Result<TaskEntity, RepositoryError> {
        let filter = doc! {"tenant_id": &task_entity.tenant_id, "id": &task_entity.id};
        self.collection.replace_one(filter, &task_entity).await?;
        Ok(task_entity)
    }

    async fn delete_task_entity(&self, tenant_id: &str, id: &str) -> Result<(), RepositoryError> {
        self.collection
            .delete_one(doc! {"tenant_id": tenant_id, "id": id})
            .await?;
        Ok(())
    }
}
