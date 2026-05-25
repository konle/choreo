use mongodb::Database;
use mongodb::bson::doc;

pub async fn ensure_all_indexes(
    db: &Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // ---- workflow_entities: (workflow_meta_id, version) unique ----
    db.run_command(doc! {
        "createIndexes": "workflow_entities",
        "indexes": [
            { "key": { "workflow_meta_id": 1, "version": 1 }, "name": "uk_meta_id_version", "unique": true },
        ],
    }).await?;

    // ---- workflow_meta_entities: (workflow_meta_id) unique + (tenant_id) ----
    db.run_command(doc! {
        "createIndexes": "workflow_meta_entities",
        "indexes": [
            { "key": { "workflow_meta_id": 1 }, "name": "uk_workflow_meta_id", "unique": true },
            { "key": { "tenant_id": 1 }, "name": "idx_tenant_id" },
        ],
    })
    .await?;

    // ---- workflow_instances: (workflow_instance_id) unique + (tenant_id) + (workflow_meta_id) ----
    db.run_command(doc! {
        "createIndexes": "workflow_instances",
        "indexes": [
            { "key": { "workflow_instance_id": 1 }, "name": "uk_workflow_instance_id", "unique": true },
            { "key": { "tenant_id": 1 }, "name": "idx_tenant_id" },
            { "key": { "workflow_meta_id": 1 }, "name": "idx_workflow_meta_id" },
            { "key": { "tenant_id": 1, "created_by": 1 }, "name": "idx_tenant_id_created_by" },
        ],
    }).await?;

    // ---- tasks: (id) unique + (tenant_id) + (tenant_id, task_type) ----
    db.run_command(doc! {
        "createIndexes": "tasks",
        "indexes": [
            { "key": { "id": 1 }, "name": "uk_id", "unique": true },
            { "key": { "tenant_id": 1 }, "name": "idx_tenant_id" },
            { "key": { "tenant_id": 1, "task_type": 1 }, "name": "idx_tenant_id_task_type" },
        ],
    })
    .await?;

    // ---- task_instances: (task_instance_id) unique + (tenant_id) ----
    db.run_command(doc! {
        "createIndexes": "task_instances",
        "indexes": [
            { "key": { "task_instance_id": 1 }, "name": "uk_task_instance_id", "unique": true },
            { "key": { "tenant_id": 1 }, "name": "idx_tenant_id" },
        ],
    })
    .await?;

    // ---- tenants: (tenant_id) unique + (name) unique ----
    db.run_command(doc! {
        "createIndexes": "tenants",
        "indexes": [
            { "key": { "tenant_id": 1 }, "name": "uk_tenant_id", "unique": true },
            { "key": { "name": 1 }, "name": "uk_name", "unique": true },
        ],
    })
    .await?;

    // ---- users: (user_id) unique + (username) unique ----
    db.run_command(doc! {
        "createIndexes": "users",
        "indexes": [
            { "key": { "user_id": 1 }, "name": "uk_user_id", "unique": true },
            { "key": { "username": 1 }, "name": "uk_username", "unique": true },
        ],
    })
    .await?;

    // ---- user_tenant_roles: (user_id, tenant_id) unique + (tenant_id) ----
    db.run_command(doc! {
        "createIndexes": "user_tenant_roles",
        "indexes": [
            { "key": { "user_id": 1, "tenant_id": 1 }, "name": "uk_user_id_tenant_id", "unique": true },
            { "key": { "tenant_id": 1 }, "name": "idx_tenant_id" },
        ],
    }).await?;

    // ---- approval_instances: (tenant_id, id) unique + (tenant_id) ----
    db.run_command(doc! {
        "createIndexes": "approval_instances",
        "indexes": [
            { "key": { "tenant_id": 1, "id": 1 }, "name": "uk_tenant_id_id", "unique": true },
            { "key": { "tenant_id": 1 }, "name": "idx_tenant_id" },
        ],
    })
    .await?;

    // ---- variables: (tenant_id, id) unique + (tenant_id, scope, scope_id) ----
    db.run_command(doc! {
        "createIndexes": "variables",
        "indexes": [
            { "key": { "tenant_id": 1, "id": 1 }, "name": "uk_tenant_id_id", "unique": true },
            { "key": { "tenant_id": 1, "scope": 1, "scope_id": 1 }, "name": "idx_tenant_scope" },
        ],
    })
    .await?;

    // ---- api_keys: (tenant_id, id) unique + (key_prefix) unique + (tenant_id, name) unique ----
    db.run_command(doc! {
        "createIndexes": "api_keys",
        "indexes": [
            { "key": { "tenant_id": 1, "id": 1 }, "name": "uk_tenant_id_id", "unique": true },
            { "key": { "key_prefix": 1 }, "name": "uk_key_prefix", "unique": true },
            { "key": { "tenant_id": 1, "name": 1 }, "name": "uk_tenant_id_name", "unique": true },
        ],
    })
    .await?;

    Ok(())
}
