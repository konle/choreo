//! Execution-time JSON context: merged variables plus `nodes` (prior successful or skipped outputs).
//!
//! After `VariableService::resolve_variables`, call [`augment_merged_context_with_nodes`]
//! so templates and Rhai see `nodes.<node_id>.output` for completed or skipped nodes.

use crate::workflow::entity::workflow_definition::{NodeExecutionStatus, WorkflowInstanceEntity};
use serde_json::{Map, Value as JsonValue, json};

/// Build the `nodes` object: only **other** nodes (not `current_node_id`) that are
/// `Success` or **`Skipped` with persisted `task_instance.output`** (including `{}`).
///
/// Parallel/ForkJoin **child** task rows are not workflow graph nodes; they do not appear here.
/// Container nodes appear as a single entry with their own `task_instance.output`.
pub fn build_nodes_object(instance: &WorkflowInstanceEntity, current_node_id: &str) -> JsonValue {
    let mut nodes = Map::new();
    for n in &instance.nodes {
        if n.node_id == current_node_id {
            continue;
        }
        let include = matches!(
            n.status,
            NodeExecutionStatus::Success | NodeExecutionStatus::Skipped
        );
        if !include {
            continue;
        }
        let Some(output) = &n.task_instance.output else {
            continue;
        };
        nodes.insert(
            n.node_id.clone(),
            json!({
                "output": output.clone(),
            }),
        );
    }
    JsonValue::Object(nodes)
}

/// Insert/replace top-level key `nodes` with [`build_nodes_object`].
/// System-injected `nodes` wins over any `nodes` key coming from merged variables / instance context.
pub fn augment_merged_context_with_nodes(
    instance: &WorkflowInstanceEntity,
    current_node_id: &str,
    merged: JsonValue,
) -> JsonValue {
    let mut map = merged.as_object().cloned().unwrap_or_default();
    map.insert(
        "nodes".to_string(),
        build_nodes_object(instance, current_node_id),
    );
    JsonValue::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::workflow::TaskType;
    use crate::task::entity::task_definition::TaskInstanceEntity;
    use crate::workflow::entity::workflow_definition::WorkflowNodeInstanceEntity;
    use chrono::Utc;

    fn minimal_task_instance(node_id: &str, output: Option<JsonValue>) -> TaskInstanceEntity {
        let now = Utc::now();
        TaskInstanceEntity {
            id: format!("task-{}", node_id),
            tenant_id: "t1".into(),
            task_id: "".into(),
            task_name: "".into(),
            task_type: TaskType::Http,
            task_template: crate::task::entity::task_definition::TaskTemplate::Http(
                crate::task::entity::task_definition::TaskHttpTemplate {
                    url: "/".into(),
                    method: crate::task::entity::task_definition::HttpMethod::Get,
                    headers: vec![],
                    body: vec![],
                    form: vec![],
                    retry_count: 0,
                    retry_delay: 0,
                    timeout: 0,
                    success_condition: None,
                },
            ),
            task_status: crate::shared::workflow::TaskInstanceStatus::Completed,
            task_instance_id: format!("ti-{}", node_id),
            created_at: now,
            updated_at: now,
            deleted_at: None,
            input: None,
            output,
            error_message: None,
            execution_duration: None,
            caller_context: None,
        }
    }

    fn node(
        id: &str,
        status: NodeExecutionStatus,
        output: Option<JsonValue>,
    ) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: id.into(),
            node_type: TaskType::Http,
            task_instance: minimal_task_instance(id, output),
            context: JsonValue::Object(Map::new()),
            next_node: None,
            status,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn nodes_only_success_with_output_excludes_current() {
        let instance = WorkflowInstanceEntity {
            workflow_instance_id: "wf1".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status: crate::shared::workflow::WorkflowInstanceStatus::Running,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
            context: JsonValue::Object(Map::new()),
            entry_node: "a".into(),
            current_node: "c".into(),
            nodes: vec![
                node("a", NodeExecutionStatus::Success, Some(json!({"x": 1}))),
                node("b", NodeExecutionStatus::Failed, Some(json!({"y": 2}))),
                node("c", NodeExecutionStatus::Running, None),
            ],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 0,
            created_by: None,
        };

        let nodes = build_nodes_object(&instance, "c");
        let obj = nodes.as_object().unwrap();
        assert!(obj.contains_key("a"));
        assert!(!obj.contains_key("b"));
        assert!(!obj.contains_key("c"));
        assert_eq!(obj["a"]["output"]["x"], json!(1));
    }
    #[test]
    fn skipped_with_output_in_nodes() {
        let instance = WorkflowInstanceEntity {
            workflow_instance_id: "wf1".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status: crate::shared::workflow::WorkflowInstanceStatus::Running,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
            context: JsonValue::Object(Map::new()),
            entry_node: "a".into(),
            current_node: "b".into(),
            nodes: vec![node("a", NodeExecutionStatus::Skipped, Some(json!({})))],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 0,
            created_by: None,
        };

        let nodes = build_nodes_object(&instance, "b");
        assert_eq!(nodes["a"]["output"], json!({}));
    }

    #[test]
    fn augment_overwrites_nodes_key() {
        let instance = WorkflowInstanceEntity {
            workflow_instance_id: "wf1".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status: crate::shared::workflow::WorkflowInstanceStatus::Running,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
            context: JsonValue::Object(Map::new()),
            entry_node: "a".into(),
            current_node: "b".into(),
            nodes: vec![node("a", NodeExecutionStatus::Success, Some(json!("done")))],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 0,
            created_by: None,
        };

        let merged = json!({
            "foo": 1,
            "nodes": { "fake": { "output": "user" } }
        });
        let out = augment_merged_context_with_nodes(&instance, "b", merged);
        assert_eq!(out["foo"], json!(1));
        assert_eq!(out["nodes"]["a"]["output"], json!("done"));
        assert!(out["nodes"]["fake"].is_null());
    }
}
