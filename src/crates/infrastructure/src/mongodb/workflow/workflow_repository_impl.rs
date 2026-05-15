use async_trait::async_trait;
use common::pagination::PaginatedData;
use domain::shared::workflow::{WorkflowInstanceStatus, WorkflowStatus};
use domain::workflow::entity::query::WorkflowInstanceQuery;
use domain::workflow::entity::workflow_definition::{WorkflowEntity, WorkflowInstanceEntity, WorkflowMetaEntity};
use domain::workflow::repository::{
    RepositoryError, WorkflowDefinitionRepository, WorkflowInstanceRepository,
};
use futures::TryStreamExt;
use mongodb::bson::{Document, doc};
use mongodb::options::{FindOneOptions, FindOptions};
use mongodb::{Client, Collection, Database};
use tracing::info;

pub struct WorkflowDefinitionRepositoryImpl {
    pub client: Client,
    pub database: Database,
    pub collection: Collection<WorkflowEntity>,
    pub workflow_meta_collection: Collection<WorkflowMetaEntity>,
}

impl WorkflowDefinitionRepositoryImpl {
    // 避免排序注入
    // const ALLOWED_SORT_FIELDS: &[&str] = &[
    //     "created_at", "updated_at", "status", "workflow_meta_id", "workflow_version",
    // ];
    // fn validate_sort_field(field: &str) -> Result<(), RepositoryError> {
    //     if !Self::ALLOWED_SORT_FIELDS.contains(&field) {
    //         return Err(format!("invalid sort field: {}", field).into());
    //     }
    //     Ok(())
    // }
    pub fn new(client: Client) -> Self {
        let database = client.database("workflow");
        let collection = database.collection("workflow_entities");
        let workflow_meta_collection = database.collection("workflow_meta_entities");
        Self {
            client,
            database,
            collection,
            workflow_meta_collection,
        }
    }
}

#[async_trait]
impl WorkflowDefinitionRepository for WorkflowDefinitionRepositoryImpl {
    async fn get_workflow_entity(
        &self,
        workflow_meta_id: String,
        version: u32,
    ) -> Result<WorkflowEntity, RepositoryError> {
        let workflow_entity = self
            .collection
            .find_one(doc! {"workflow_meta_id": &workflow_meta_id, "version": &version})
            .await?
            .ok_or_else(|| {
                format!(
                    "workflow entity not found: {} version: {}",
                    workflow_meta_id, version
                )
            })?;
        Ok(workflow_entity)
    }

    async fn list_workflow_entities(
        &self,
        workflow_meta_id: &str,
    ) -> Result<Vec<WorkflowEntity>, RepositoryError> {
        let cursor = self.collection
            .find(doc! {"workflow_meta_id": workflow_meta_id, "status":{"$ne": WorkflowStatus::Deleted.to_string()}})
            .await?;
        let results: Vec<WorkflowEntity> = cursor.try_collect().await?;
        Ok(results)
    }

    async fn save_workflow_entity(&self, entity: &WorkflowEntity) -> Result<(), RepositoryError> {
        let filter = doc! {
            "workflow_meta_id": &entity.workflow_meta_id,
            "version": entity.version as i64,
        };

        let existing = self.collection.find_one(filter.clone()).await?;
        if let Some(ref existing_entity) = existing {
            if existing_entity.status != WorkflowStatus::Draft {
                return Err(format!(
                    "cannot update workflow version {} (status: {:?}), only Draft versions can be modified",
                    entity.version, existing_entity.status
                ).into());
            }
        }

        let update = doc! {
            "$set": {
                "nodes": mongodb::bson::to_bson(&entity.nodes).map_err(|e| format!("serialize nodes: {}", e))?,
                "status": mongodb::bson::to_bson(&entity.status).map_err(|e| format!("serialize status: {}", e))?,
                "updated_at": mongodb::bson::to_bson(&entity.updated_at).map_err(|e| format!("serialize updated_at: {}", e))?,
                "entry_node": &entity.entry_node,
            },
            "$setOnInsert": {
                "workflow_meta_id": &entity.workflow_meta_id,
                "version": entity.version as i64,
                "created_at": mongodb::bson::to_bson(&entity.created_at).map_err(|e| format!("serialize created_at: {}", e))?,
                "deleted_at": mongodb::bson::Bson::Null,
            }
        };

        self.collection
            .update_one(filter, update)
            .upsert(true)
            .await?;
        Ok(())
    }

    // async fn publish_workflow_entity(&self, workflow_meta_id: &str, version: u32) -> Result<(), RepositoryError> {
    //     let filter = doc! {
    //         "workflow_meta_id": workflow_meta_id,
    //         "version": version as i64,
    //         "status": mongodb::bson::to_bson(&WorkflowStatus::Draft).map_err(|e| format!("serialize status: {}", e))?,
    //     };
    //     let update = doc! {
    //         "$set": {
    //             "status": mongodb::bson::to_bson(&WorkflowStatus::Published).map_err(|e| format!("serialize status: {}", e))?,
    //             "updated_at": mongodb::bson::to_bson(&chrono::Utc::now()).map_err(|e| format!("serialize: {}", e))?,
    //         }
    //     };
    //     let result = self.collection.update_one(filter, update).await?;
    //     if result.matched_count == 0 {
    //         return Err(format!(
    //             "cannot publish workflow version {}: not found or not in Draft status",
    //             version
    //         ).into());
    //     }
    //     Ok(())
    // }

    // async fn delete_workflow_entity(&self, workflow_meta_id: String, version: u32) -> Result<(), RepositoryError> {
    //     let workflow_status = WorkflowStatus::Archived.to_string();
    //     self.collection.update_one(doc! {"workflow_meta_id": &workflow_meta_id, "version": &version, "status": &workflow_status}, doc! {"$set": {"status": &WorkflowStatus::Deleted.to_string()}}).await?;
    //     Ok(())
    // }

    async fn get_workflow_meta_entity(
        &self,
        workflow_meta_id: String,
    ) -> Result<WorkflowMetaEntity, RepositoryError> {
        let workflow_meta_entity = self
            .workflow_meta_collection
            .find_one(doc! {"workflow_meta_id": &workflow_meta_id})
            .await?
            .ok_or_else(|| format!("workflow meta entity not found: {}", &workflow_meta_id))?;
        Ok(workflow_meta_entity)
    }

    async fn save_workflow_meta_entity(
        &self,
        entity: &WorkflowMetaEntity,
    ) -> Result<(), RepositoryError> {
        let filter = doc! { "workflow_meta_id": &entity.workflow_meta_id };
        let update = doc! {
            "$set": {
                "name": &entity.name,
                "description": &entity.description,
                "status": mongodb::bson::to_bson(&entity.status).map_err(|e| format!("serialize status: {}", e))?,
                "form": mongodb::bson::to_bson(&entity.form).map_err(|e| format!("serialize form: {}", e))?,
                "updated_at": mongodb::bson::to_bson(&entity.updated_at).map_err(|e| format!("serialize updated_at: {}", e))?,
            },
            "$setOnInsert": {
                "workflow_meta_id": &entity.workflow_meta_id,
                "tenant_id": &entity.tenant_id,
                "created_at": mongodb::bson::to_bson(&entity.created_at).map_err(|e| format!("serialize created_at: {}", e))?,
                "deleted_at": mongodb::bson::Bson::Null,
            }
        };
        self.workflow_meta_collection
            .update_one(filter, update)
            .upsert(true)
            .await?;
        Ok(())
    }

    async fn get_workflow_meta_entity_scoped(
        &self,
        tenant_id: &str,
        workflow_meta_id: &str,
    ) -> Result<WorkflowMetaEntity, RepositoryError> {
        let entity = self
            .workflow_meta_collection
            .find_one(doc! {"tenant_id": tenant_id, "workflow_meta_id": workflow_meta_id})
            .await?
            .ok_or_else(|| {
                format!(
                    "workflow meta entity not found: {} in tenant {}",
                    workflow_meta_id, tenant_id
                )
            })?;
        Ok(entity)
    }

    async fn list_workflow_meta_entities(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<WorkflowMetaEntity>, RepositoryError> {
        let cursor = self
            .workflow_meta_collection
            .find(doc! {"tenant_id": tenant_id})
            .await?;
        let results: Vec<WorkflowMetaEntity> = cursor.try_collect().await?;
        Ok(results)
    }

    async fn delete_workflow_meta_entity(
        &self,
        tenant_id: &str,
        workflow_meta_id: &str,
    ) -> Result<(), RepositoryError> {
        self.workflow_meta_collection
            .delete_one(doc! {"tenant_id": tenant_id, "workflow_meta_id": workflow_meta_id})
            .await?;
        Ok(())
    }

    async fn transition_status(
        &self,
        workflow_meta_id: String,
        version: u32,
        from_status: &WorkflowStatus,
        to_status: &WorkflowStatus,
    ) -> Result<(), RepositoryError> {
        let from_bson = mongodb::bson::to_bson(from_status).map_err(|e| format!("serialize status: {e}"))?;
        let to_bson = mongodb::bson::to_bson(to_status).map_err(|e| format!("serialize status: {e}"))?;
        let result = self.collection.update_one(doc! {"workflow_meta_id": &workflow_meta_id, "version": &version, "status": from_bson}, doc! {"$set": {"status": to_bson, "updated_at": mongodb::bson::to_bson(&chrono::Utc::now()).map_err(|e| format!("serialize updated_at: {e}"))?}}).await?;
        if result.matched_count == 0 {
            return Err(format!(
                "cannot transition workflow version {}: not found or not in expected status",
                version
            )
            .into());
        }
        if result.modified_count == 0 {
            return Err(format!(
                "cannot transition workflow version {}: not modified",
                version
            )
            .into());
        }
        Ok(())
    }

    async fn create_workflow_meta_entity(
        &self,
        workflow_meta_entity: &WorkflowMetaEntity,
    ) -> Result<WorkflowMetaEntity, RepositoryError> {
        self.workflow_meta_collection
            .insert_one(workflow_meta_entity)
            .await?;
        Ok(workflow_meta_entity.clone())
    }
    async fn max_version(&self, workflow_meta_id: String) -> Result<u32, RepositoryError> {
        let options = FindOneOptions::builder().sort(doc! {"version": -1}).build();
        let result = self
            .collection
            .find_one(doc! {"workflow_meta_id": &workflow_meta_id})
            .with_options(options)
            .await?;
        let max_version = result.map(|entity| entity.version).unwrap_or(0);
        Ok(max_version)
    }
}

pub struct WorkflowInstanceRepositoryImpl {
    pub client: Client,
    pub database: Database,
    pub workflow_instance_collection: Collection<WorkflowInstanceEntity>,
}

impl WorkflowInstanceRepositoryImpl {
    // 避免排序注入
    const ALLOWED_SORT_FIELDS: &[&str] = &[
        "created_at", "updated_at", "status", "workflow_meta_id", "workflow_version",
    ];
    fn validate_sort_field(field: &str) -> Result<(), RepositoryError> {
        if !Self::ALLOWED_SORT_FIELDS.contains(&field) {
            return Err(format!("invalid sort field: {}", field).into());
        }
        Ok(())
    }
    pub fn new(client: Client) -> Self {
        let database = client.database("workflow");
        let workflow_instance_collection = database.collection("workflow_instances");
        Self {
            client,
            database,
            workflow_instance_collection,
        }
    }
}




impl WorkflowInstanceRepositoryImpl{
    fn build_filter(&self, query: &WorkflowInstanceQuery) -> Document {
        let mut filter = doc! {"tenant_id": &query.tenant_id};
        if let Some(workflow_meta_id) = &query.filter.workflow_meta_id {
            filter.insert("workflow_meta_id", workflow_meta_id);
        }
        if let Some(version) = &query.filter.version {
            filter.insert("workflow_version", version);
        }
        if let Some(status) = &query.filter.status {
            if let Ok(bson_val) = mongodb::bson::to_bson(status) {
                filter.insert("status", bson_val);
            }
        }
        // TODO: sort by
        filter
    }
}

#[async_trait]
impl WorkflowInstanceRepository for WorkflowInstanceRepositoryImpl {
    async fn get_workflow_instance(
        &self,
        id: String,
    ) -> Result<WorkflowInstanceEntity, RepositoryError> {
        let workflow_instance = self
            .workflow_instance_collection
            .find_one(doc! {"workflow_instance_id": &id})
            .await?
            .ok_or_else(|| format!("workflow instance not found: {}", id))?;
        Ok(workflow_instance)
    }

    async fn get_workflow_instance_scoped(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<WorkflowInstanceEntity, RepositoryError> {
        let instance = self
            .workflow_instance_collection
            .find_one(doc! {"tenant_id": tenant_id, "workflow_instance_id": id})
            .await?
            .ok_or_else(|| {
                format!(
                    "workflow instance not found: {} in tenant {}",
                    id, tenant_id
                )
            })?;
        Ok(instance)
    }


    async fn list_workflow_instances(
        &self,
        _tenant_id: &str,
        query: &WorkflowInstanceQuery,
    ) -> Result<PaginatedData<WorkflowInstanceEntity>, RepositoryError> {
        let filter = self.build_filter(query);
        let page = query.pagination.page;
        let page_size = query.pagination.page_size;
        let skip = (page - 1) * page_size;
        Self::validate_sort_field(&query.sort.sort_by)?;
        info!("list_workflow_instances filter: {:?} tenant_id: {} page: {} page_size: {}", filter, _tenant_id, page, page_size);
        let sort_order:i32 = if query.sort.sort_order == "asc" { 1 } else { -1 };
        let sort_doc = doc! { &query.sort.sort_by: sort_order };

        let total = self
            .workflow_instance_collection
            .count_documents(filter.clone())
            .await?;

        let find_options = FindOptions::builder()
            .skip(skip as u64)
            .limit(page_size as i64)
            .sort(sort_doc)
            .build();
        let cursor = self
            .workflow_instance_collection
            .find(filter)
            .with_options(find_options)
            .await?;
        let items: Vec<WorkflowInstanceEntity> = cursor.try_collect().await?;

        Ok(PaginatedData {
            items,
            total,
            page,
            page_size,
        })
    }

    async fn transfer_status(
        &self,
        workflow_instance_id: &str,
        from_status: &WorkflowInstanceStatus,
        to_status: &WorkflowInstanceStatus,
    ) -> Result<WorkflowInstanceEntity, RepositoryError> {
        let from_bson = mongodb::bson::to_bson(from_status)
            .map_err(|e| format!("serialize from_status: {e}"))?;
        let to_bson = mongodb::bson::to_bson(to_status)
            .map_err(|e| format!("serialize to_status: {e}"))?;

        let filter = doc! {
            "workflow_instance_id": workflow_instance_id,
            "status": from_bson,
        };
        let update = doc! {
            "$set": {
                "status": to_bson,
                "updated_at": mongodb::bson::to_bson(&chrono::Utc::now()).map_err(|e| format!("serialize now: {e}"))?,
            }
        };

        let result = self
            .workflow_instance_collection
            .find_one_and_update(filter, update)
            .return_document(mongodb::options::ReturnDocument::After)
            .await?
            .ok_or_else(|| {
                format!(
                    "CAS failed: instance {} not in expected state {:?}",
                    workflow_instance_id, from_status
                )
            })?;

        Ok(result)
    }
    async fn acquire_lock(
        &self,
        workflow_instance_id: &str,
        worker_id: &str,
        duration_ms: u64,
    ) -> Result<WorkflowInstanceEntity, RepositoryError> {
        let now = chrono::Utc::now();
        let now_bson = mongodb::bson::to_bson(&now).map_err(|e| format!("serialize now: {e}"))?;
        let expiration = now - chrono::Duration::milliseconds(duration_ms as i64);
        let expiration_bson = mongodb::bson::to_bson(&expiration).map_err(|e| format!("serialize expiration: {e}"))?;

        let filter = doc! {
            "workflow_instance_id": workflow_instance_id,
            "$or": [
                { "locked_at": mongodb::bson::Bson::Null },
                { "locked_at": { "$lt": expiration_bson } }
            ]
        };

        let update_doc = doc! {
            "$set": {
                "locked_by": worker_id,
                "locked_duration": duration_ms as i64,
                "locked_at": now_bson.clone(),
                "updated_at": now_bson,
            },
            "$inc": { "epoch": 1 }
        };

        let result = self
            .workflow_instance_collection
            .find_one_and_update(filter, update_doc)
            .return_document(mongodb::options::ReturnDocument::After)
            .await?
            .ok_or_else(|| {
                format!(
                    "failed to acquire lock for instance {}",
                    workflow_instance_id
                )
            })?;

        Ok(result)
    }

    async fn release_lock(
        &self,
        workflow_instance_id: &str,
        worker_id: &str,
        expected_epoch: u64,
    ) -> Result<(), RepositoryError> {
        let now_bson = mongodb::bson::to_bson(&chrono::Utc::now()).map_err(|e| format!("serialize now: {e}"))?;

        let filter = doc! {
            "workflow_instance_id": workflow_instance_id,
            "locked_by": worker_id,
            "epoch": expected_epoch as i64,
        };

        let update_doc = doc! {
            "$set": {
                "locked_by": mongodb::bson::Bson::Null,
                "locked_duration": mongodb::bson::Bson::Null,
                "locked_at": mongodb::bson::Bson::Null,
                "updated_at": now_bson,
            },
            "$inc": { "epoch": 1 }
        };

        let result = self
            .workflow_instance_collection
            .update_one(filter, update_doc)
            .await?;

        if result.matched_count == 0 {
            return Err(format!(
                "failed to release lock for instance {} (not held by {} or epoch mismatch)",
                workflow_instance_id, worker_id
            )
            .into());
        }

        Ok(())
    }

    async fn create_workflow_instance(
        &self,
        instance: &WorkflowInstanceEntity,
    ) -> Result<WorkflowInstanceEntity, RepositoryError> {
        self.workflow_instance_collection
            .insert_one(instance)
            .await?;
        Ok(instance.clone())
    }

    async fn save_workflow_instance(
        &self,
        instance: &WorkflowInstanceEntity,
    ) -> Result<(), RepositoryError> {
        let current_epoch = instance.epoch as i64;
        let filter = doc! {
            "workflow_instance_id": &instance.workflow_instance_id,
            "epoch": current_epoch, // CAS check
        };

        let mut update_instance = instance.clone();
        update_instance.epoch += 1;
        update_instance.updated_at = chrono::Utc::now();

        let update_doc = mongodb::bson::to_document(&update_instance)
            .map_err(|e| format!("Failed to serialize instance: {}", e))?;

        let update = doc! {
            "$set": update_doc
        };

        let result = self
            .workflow_instance_collection
            .update_one(filter.clone(), update)
            .await?;

        if result.matched_count == 0 {
            // Check if it exists at all (might be an insert or a CAS failure)
            let exists = self
                .workflow_instance_collection
                .count_documents(doc! { "workflow_instance_id": &instance.workflow_instance_id })
                .await?;

            if exists == 0 {
                // If it doesn't exist, it's an initial insert, which is valid for save()
                self.workflow_instance_collection
                    .insert_one(update_instance)
                    .await?;
                return Ok(());
            }

            return Err(format!(
                "Optimistic lock failed for workflow {}: expected epoch {}",
                instance.workflow_instance_id, current_epoch
            )
            .into());
        }

        Ok(())
    }

    async fn scan_zombie_instances(
        &self,
        limit: u32,
    ) -> Result<Vec<WorkflowInstanceEntity>, RepositoryError> {
        let now = chrono::Utc::now();
        let now_bson = mongodb::bson::to_bson(&now).map_err(|e| format!("serialize now: {e}"))?;
        let status_running = mongodb::bson::to_bson(&WorkflowInstanceStatus::Running)
            .map_err(|e| format!("serialize Running: {e}"))?;
        let status_await = mongodb::bson::to_bson(&WorkflowInstanceStatus::Await)
            .map_err(|e| format!("serialize Await: {e}"))?;

        let filter = doc! {
            "status": { "$in": [status_running, status_await] },
            "$or": [
                { "locked_at": mongodb::bson::Bson::Null },
                { "$expr": {
                    "$lt": [
                        { "$dateAdd": {
                            "startDate": "$locked_at",
                            "unit": "millisecond",
                            "amount": "$locked_duration"
                        }},
                        now_bson
                    ]
                }}
            ]
        };

        let mut cursor = self
            .workflow_instance_collection
            .find(filter)
            .limit(limit as i64)
            .await?;

        let mut results = Vec::new();
        while cursor.advance().await? {
            results.push(cursor.deserialize_current()?);
        }
        Ok(results)
    }

    async fn force_clear_lock(
        &self,
        workflow_instance_id: &str,
        expected_epoch: u64,
    ) -> Result<(), RepositoryError> {
        let now_bson = mongodb::bson::to_bson(&chrono::Utc::now()).map_err(|e| format!("serialize now: {e}"))?;
        let filter = doc! {
            "workflow_instance_id": workflow_instance_id,
            "epoch": expected_epoch as i64,
        };
        let update = doc! {
            "$set": {
                "locked_by": mongodb::bson::Bson::Null,
                "locked_at": mongodb::bson::Bson::Null,
                "locked_duration": mongodb::bson::Bson::Null,
                "updated_at": now_bson,
            },
            "$inc": { "epoch": 1 }
        };

        let result = self
            .workflow_instance_collection
            .update_one(filter, update)
            .await?;

        if result.matched_count == 0 {
            return Err(format!(
                "CAS failed: instance {} epoch {} was already modified",
                workflow_instance_id, expected_epoch
            ).into());
        }
        Ok(())
    }

    async fn scan_instances_by_status(
        &self,
        status: &WorkflowInstanceStatus,
        limit: u32,
    ) -> Result<Vec<WorkflowInstanceEntity>, RepositoryError> {
        let status_bson = mongodb::bson::to_bson(status)
            .map_err(|e| format!("serialize status: {e}"))?;
        let filter = doc! { "status": status_bson };

        let mut cursor = self
            .workflow_instance_collection
            .find(filter)
            .limit(limit as i64)
            .await?;

        let mut results = Vec::new();
        while cursor.advance().await? {
            results.push(cursor.deserialize_current()?);
        }
        Ok(results)
    }
}
