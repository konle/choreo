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
    fn normalize_sort_order(sort_order: &str) -> i32 {
        if sort_order == "asc" { 1 } else { -1 }
    }

    fn build_filter(&self, query: &TaskInstanceQuery) -> Document {
        let mut filter = doc! {"tenant_id": &query.tenant_id};
        if let Some(task_id) = &query.filter.task_id {
            filter.insert("task_id", task_id);
        }
        if let Some(status) = &query.filter.status {
            if let Ok(bson_val) = mongodb::bson::to_bson(status) {
                filter.insert("task_status", bson_val);
            }
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
        let skip = (page - 1) * page_size;
        Self::validate_sort_field(&query.sort.sort_by)?;
        let sort_order: i32 = if query.sort.sort_order == "asc" {
            1
        } else {
            -1
        };
        let sort_doc = doc! { &query.sort.sort_by: sort_order };
        // let find_options = FindOptions::builder()
        //     .skip(skip as u64)
        //     .limit(page_size as i64)
        //     .sort(sort_doc)
        //     .build();
        let total = self.collection.count_documents(filter.clone()).await?;

        let find_options = FindOptions::builder()
            .skip(skip as u64)
            .limit(page_size as i64)
            .sort(sort_doc)
            .build();
        let cursor = self
            .collection
            .find(filter)
            .with_options(find_options)
            .await?;
        let items: Vec<TaskInstanceEntity> = cursor.try_collect().await?;

        Ok(PaginatedData {
            items,
            total,
            page,
            page_size,
        })
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
        let from_bson = mongodb::bson::to_bson(from_status)
            .map_err(|e| format!("serialize from_status: {e}"))?;
        let to_bson =
            mongodb::bson::to_bson(to_status).map_err(|e| format!("serialize to_status: {e}"))?;

        let filter = doc! {
            "task_instance_id": task_instance_id,
            "task_status": from_bson,
        };
        let mut set_fields = doc! {
            "task_status": to_bson,
            "updated_at": chrono::Utc::now().to_rfc3339(),
        };
        if let Some(ref out) = fields.output {
            set_fields.insert(
                "output",
                mongodb::bson::to_bson(out).map_err(|e| format!("serialize output: {e}"))?,
            );
        }
        if let Some(ref inp) = fields.input {
            set_fields.insert(
                "input",
                mongodb::bson::to_bson(inp).map_err(|e| format!("serialize input: {e}"))?,
            );
        }
        if let Some(ref err) = fields.error_message {
            set_fields.insert("error_message", err);
        }
        if let Some(execution_duration) = fields.execution_duration {
            set_fields.insert("execution_duration", execution_duration as i64);
        }
        let update = doc! { "$set": set_fields };

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
