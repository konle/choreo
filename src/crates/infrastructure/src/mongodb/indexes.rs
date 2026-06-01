use mongodb::Database;
use mongodb::bson::doc;

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub key_doc: mongodb::bson::Document,
    pub name: &'static str,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct CollectionIndexes {
    pub collection: &'static str,
    pub indexes: Vec<IndexDef>,
}

pub fn index_definitions() -> Vec<CollectionIndexes> {
    vec![
        CollectionIndexes {
            collection: "workflow_entities",
            indexes: vec![IndexDef {
                key_doc: doc! { "workflow_meta_id": 1, "version": 1 },
                name: "uk_meta_id_version",
                unique: true,
            }],
        },
        CollectionIndexes {
            collection: "workflow_meta_entities",
            indexes: vec![
                IndexDef { key_doc: doc! { "workflow_meta_id": 1 }, name: "uk_workflow_meta_id", unique: true },
                IndexDef { key_doc: doc! { "tenant_id": 1 }, name: "idx_tenant_id", unique: false },
            ],
        },
        CollectionIndexes {
            collection: "workflow_instances",
            indexes: vec![
                IndexDef { key_doc: doc! { "workflow_instance_id": 1 }, name: "uk_workflow_instance_id", unique: true },
                IndexDef { key_doc: doc! { "tenant_id": 1 }, name: "idx_tenant_id", unique: false },
                IndexDef { key_doc: doc! { "workflow_meta_id": 1 }, name: "idx_workflow_meta_id", unique: false },
                IndexDef { key_doc: doc! { "tenant_id": 1, "created_by": 1 }, name: "idx_tenant_id_created_by", unique: false },
            ],
        },
        CollectionIndexes {
            collection: "tasks",
            indexes: vec![
                IndexDef { key_doc: doc! { "id": 1 }, name: "uk_id", unique: true },
                IndexDef { key_doc: doc! { "tenant_id": 1 }, name: "idx_tenant_id", unique: false },
                IndexDef { key_doc: doc! { "tenant_id": 1, "task_type": 1 }, name: "idx_tenant_id_task_type", unique: false },
            ],
        },
        CollectionIndexes {
            collection: "task_instances",
            indexes: vec![
                IndexDef { key_doc: doc! { "task_instance_id": 1 }, name: "uk_task_instance_id", unique: true },
                IndexDef { key_doc: doc! { "tenant_id": 1 }, name: "idx_tenant_id", unique: false },
            ],
        },
        CollectionIndexes {
            collection: "tenants",
            indexes: vec![
                IndexDef { key_doc: doc! { "tenant_id": 1 }, name: "uk_tenant_id", unique: true },
                IndexDef { key_doc: doc! { "name": 1 }, name: "uk_name", unique: true },
            ],
        },
        CollectionIndexes {
            collection: "users",
            indexes: vec![
                IndexDef { key_doc: doc! { "user_id": 1 }, name: "uk_user_id", unique: true },
                IndexDef { key_doc: doc! { "username": 1 }, name: "uk_username", unique: true },
            ],
        },
        CollectionIndexes {
            collection: "user_tenant_roles",
            indexes: vec![
                IndexDef { key_doc: doc! { "user_id": 1, "tenant_id": 1 }, name: "uk_user_id_tenant_id", unique: true },
                IndexDef { key_doc: doc! { "tenant_id": 1 }, name: "idx_tenant_id", unique: false },
            ],
        },
        CollectionIndexes {
            collection: "approval_instances",
            indexes: vec![
                IndexDef { key_doc: doc! { "tenant_id": 1, "id": 1 }, name: "uk_tenant_id_id", unique: true },
                IndexDef { key_doc: doc! { "tenant_id": 1 }, name: "idx_tenant_id", unique: false },
            ],
        },
        CollectionIndexes {
            collection: "variables",
            indexes: vec![
                IndexDef { key_doc: doc! { "tenant_id": 1, "id": 1 }, name: "uk_tenant_id_id", unique: true },
                IndexDef { key_doc: doc! { "tenant_id": 1, "scope": 1, "scope_id": 1 }, name: "idx_tenant_scope", unique: false },
            ],
        },
        CollectionIndexes {
            collection: "api_keys",
            indexes: vec![
                IndexDef { key_doc: doc! { "tenant_id": 1, "id": 1 }, name: "uk_tenant_id_id", unique: true },
                IndexDef { key_doc: doc! { "key_prefix": 1 }, name: "uk_key_prefix", unique: true },
                IndexDef { key_doc: doc! { "tenant_id": 1, "name": 1 }, name: "uk_tenant_id_name", unique: true },
            ],
        },
    ]
}

pub async fn ensure_all_indexes(
    db: &Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for coll in index_definitions() {
        let indexes: Vec<mongodb::bson::Document> = coll
            .indexes
            .iter()
            .map(|idx| {
                doc! {
                    "key": idx.key_doc.clone(),
                    "name": idx.name,
                    "unique": idx.unique,
                }
            })
            .collect();
        db.run_command(doc! {
            "createIndexes": coll.collection,
            "indexes": indexes,
        })
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_definitions_are_not_empty() {
        let defs = index_definitions();
        assert!(!defs.is_empty());
        assert_eq!(defs.len(), 11);
    }

    #[test]
    fn all_collections_have_at_least_one_index() {
        for coll in index_definitions() {
            assert!(
                !coll.indexes.is_empty(),
                "collection {} has no indexes",
                coll.collection
            );
        }
    }

    #[test]
    fn unique_indexes_have_correct_flag() {
        for coll in index_definitions() {
            for idx in &coll.indexes {
                if idx.name.starts_with("uk_") {
                    assert!(idx.unique, "{}:{} should be unique", coll.collection, idx.name);
                }
            }
        }
    }
}
