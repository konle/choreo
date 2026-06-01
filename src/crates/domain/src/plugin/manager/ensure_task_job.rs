//! Materialize `task_instances` rows for async jobs (Parallel / ForkJoin children use inner template + type).
//! Graph HTTP nodes: copy `WorkflowNodeInstanceEntity.task_instance.input` from `run_node` so the task
//! worker does not re-resolve HTTP templates with an empty context.

use super::PluginManager;
use crate::shared::job::ExecuteTaskJob;
use crate::shared::workflow::TaskInstanceStatus;
use crate::task::entity::task_definition::{TaskInstanceEntity, TaskTemplate};
use crate::workflow::entity::workflow_definition::WorkflowInstanceEntity;
use tracing::warn;

impl PluginManager {
    fn resolve_http_child_input(
        tpl: &crate::task::entity::task_definition::TaskHttpTemplate,
        parent: &TaskInstanceEntity,
        job: &ExecuteTaskJob,
        parent_node_ctx: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        match &parent.task_template {
            TaskTemplate::Parallel(pt) => {
                let idx = job
                    .caller_context
                    .as_ref()
                    .and_then(|c| c.item_index)
                    .unwrap_or(0);
                let ctx = crate::task::http_template_resolve::context_with_parallel_item(
                    parent_node_ctx,
                    &pt.items_path,
                    &pt.item_alias,
                    idx,
                );
                Some(crate::task::http_template_resolve::resolved_http_request_snapshot(tpl, &ctx))
            }
            TaskTemplate::ForkJoin(_) => Some(
                crate::task::http_template_resolve::resolved_http_request_snapshot(
                    tpl,
                    parent_node_ctx,
                ),
            ),
            TaskTemplate::Http(_) => {
                let has_resolved_url = parent
                    .input
                    .as_ref()
                    .and_then(|i| i.get("url"))
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.is_empty());
                if has_resolved_url {
                    parent.input.clone()
                } else {
                    Some(
                        crate::task::http_template_resolve::resolved_http_request_snapshot(
                            tpl,
                            parent_node_ctx,
                        ),
                    )
                }
            }
            _ => Some(
                crate::task::http_template_resolve::resolved_http_request_snapshot(
                    tpl,
                    parent_node_ctx,
                ),
            ),
        }
    }

    fn resolve_llm_child_input(
        tpl: &crate::task::entity::task_definition::LlmTemplate,
        parent: &TaskInstanceEntity,
        job: &ExecuteTaskJob,
        parent_node_ctx: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        if let TaskTemplate::Llm(_) = &parent.task_template {
            if parent.input.is_some() {
                return parent.input.clone();
            }
        }

        let ctx = match &parent.task_template {
            TaskTemplate::Parallel(pt) => {
                let idx = job
                    .caller_context
                    .as_ref()
                    .and_then(|c| c.item_index)
                    .unwrap_or(0);
                crate::task::http_template_resolve::context_with_parallel_item(
                    parent_node_ctx,
                    &pt.items_path,
                    &pt.item_alias,
                    idx,
                )
            }
            _ => parent_node_ctx.clone(),
        };
        Some(super::workflow::resolved_llm_request_snapshot(tpl, &ctx))
    }

    /// Derive the expected (child_template, child_task_type, child_task_id) for a dispatched
    /// job based on the parent node's template. Parallel/ForkJoin children use their inner
    /// template; all others inherit the parent's template as-is.
    ///
    /// Returns `(template, task_type, task_id_override)`. When `task_id_override` is `Some`,
    /// it should replace the parent's `task_id` on the materialised child instance.
    fn derive_child_template(
        parent: &TaskInstanceEntity,
        job: &ExecuteTaskJob,
    ) -> anyhow::Result<(
        TaskTemplate,
        crate::shared::workflow::TaskType,
        Option<String>,
    )> {
        match &parent.task_template {
            TaskTemplate::Parallel(_pt) => {
                let inner = (*_pt.task_template).clone();
                let tt = inner.task_type();
                Ok((inner, tt, None))
            }
            TaskTemplate::ForkJoin(fj) => {
                let idx = job
                    .caller_context
                    .as_ref()
                    .and_then(|c| c.item_index)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "ForkJoin dispatch job missing item_index in caller_context"
                        )
                    })?;
                let item = fj.tasks.get(idx).ok_or_else(|| {
                    anyhow::anyhow!(
                        "ForkJoin item_index {} out of range (len {})",
                        idx,
                        fj.tasks.len()
                    )
                })?;
                let inner = item.task_template.clone();
                let tt = inner.task_type();
                let child_task_id = item.task_id.clone();
                Ok((inner, tt, child_task_id))
            }
            _ => Ok((parent.task_template.clone(), parent.task_type.clone(), None)),
        }
    }

    pub(super) async fn ensure_task_instance_for_job(
        &self,
        instance: &WorkflowInstanceEntity,
        node_index: usize,
        job: &ExecuteTaskJob,
    ) -> anyhow::Result<()> {
        let Some(task_svc) = &self.task_instance_svc else {
            return Ok(());
        };

        let parent = &instance.nodes[node_index].task_instance;
        let (child_template, child_task_type, task_id_override) =
            Self::derive_child_template(parent, job)?;

        let effective_task_id = task_id_override.unwrap_or_else(|| parent.task_id.clone());

        if let Ok(existing) = task_svc
            .get_task_instance_entity(job.task_instance_id.clone())
            .await
        {
            if existing.task_status.is_terminal() {
                warn!(
                    task_instance_id = %job.task_instance_id,
                    status = ?existing.task_status,
                    "task instance in terminal state, refusing to overwrite"
                );
                return Ok(());
            }
            if existing.task_type != child_task_type {
                warn!(
                    task_instance_id = %job.task_instance_id,
                    existing_type = ?existing.task_type,
                    expected_type = ?child_task_type,
                    "task instance has wrong task_type, correcting"
                );
                let mut corrected = existing;
                corrected.task_type = child_task_type;
                corrected.task_template = child_template;
                corrected.task_id = effective_task_id;
                task_svc
                    .update_task_instance_entity(corrected)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
            return Ok(());
        }

        let now = chrono::Utc::now();
        let parent_node_ctx = &instance.nodes[node_index].context;

        let mut task_instance: TaskInstanceEntity = parent.clone();
        task_instance.task_template = child_template;
        task_instance.task_type = child_task_type;
        task_instance.id = job.task_instance_id.clone();
        task_instance.task_id = effective_task_id;
        task_instance.task_instance_id = job.task_instance_id.clone();
        task_instance.tenant_id = job.tenant_id.clone();
        task_instance.caller_context = job.caller_context.clone();
        task_instance.created_at = now;
        task_instance.updated_at = now;
        task_instance.input = None;
        task_instance.output = None;
        task_instance.error_message = None;
        task_instance.execution_duration = None;
        task_instance.task_status = TaskInstanceStatus::Pending;

        task_instance.input = match &task_instance.task_template {
            TaskTemplate::Http(tpl) => {
                Self::resolve_http_child_input(tpl, parent, job, parent_node_ctx)
            }
            TaskTemplate::Llm(tpl) => {
                Self::resolve_llm_child_input(tpl, parent, job, parent_node_ctx)
            }
            _ => None,
        };

        task_svc
            .create_task_instance_entity(task_instance)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::job::WorkflowCallerContext;
    use crate::shared::workflow::TaskType;
    use crate::task::entity::task_definition::{
        ForkJoinTaskItem, ForkJoinTemplate, LlmTemplate, ParallelMode, ParallelTemplate,
        TaskHttpTemplate, TaskTemplate,
    };
    use chrono::Utc;
    use serde_json::json;

    fn make_parent(task_template: TaskTemplate, input: Option<serde_json::Value>) -> TaskInstanceEntity {
        let now = Utc::now();
        TaskInstanceEntity {
            id: "parent-1".into(),
            tenant_id: "t1".into(),
            task_id: "task-def".into(),
            task_name: "test".into(),
            task_type: TaskType::Http,
            task_template,
            task_status: crate::shared::workflow::TaskInstanceStatus::Pending,
            task_instance_id: "parent-inst-1".into(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
            input,
            output: None,
            error_message: None,
            execution_duration: None,
            caller_context: None,
        }
    }

    fn make_job(item_index: Option<usize>) -> ExecuteTaskJob {
        ExecuteTaskJob {
            task_instance_id: "child-1".into(),
            tenant_id: "t1".into(),
            caller_context: Some(WorkflowCallerContext {
                workflow_instance_id: "wf-1".into(),
                node_id: "node1".into(),
                parent_task_instance_id: None,
                item_index,
            }),
        }
    }

    fn dummy_http_template() -> TaskHttpTemplate {
        TaskHttpTemplate {
            url: "https://example.com/api".into(),
            method: crate::task::entity::task_definition::HttpMethod::Post,
            headers: vec![],
            body: vec![],
            form: vec![],
            retry_count: 0,
            retry_delay: 0,
            timeout: 10,
            success_condition: None,
        }
    }

    fn dummy_llm_template() -> LlmTemplate {
        LlmTemplate {
            base_url: "https://api.llm.com".into(),
            model: "gpt-4".into(),
            api_key_ref: "key-1".into(),
            system_prompt: None,
            user_prompt: "hello".into(),
            temperature: None,
            max_tokens: None,
            timeout: 30,
            retry_count: 0,
            retry_delay: 0,
            response_format: None,
            form: vec![],
        }
    }

    #[test]
    fn test_resolve_http_child_input_simple_parent() {
        let parent = make_parent(TaskTemplate::Http(dummy_http_template()), None);
        let job = make_job(None);
        let ctx = json!({"key": "val"});
        let result = PluginManager::resolve_http_child_input(
            &dummy_http_template(),
            &parent,
            &job,
            &ctx,
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_http_child_input_parent_has_resolved_url() {
        let parent = make_parent(
            TaskTemplate::Http(dummy_http_template()),
            Some(json!({"url": "https://resolved.example.com", "body": "ok"})),
        );
        let job = make_job(None);
        let ctx = json!({});
        let result = PluginManager::resolve_http_child_input(
            &dummy_http_template(),
            &parent,
            &job,
            &ctx,
        );
        assert!(result.is_some());
        let output = result.unwrap();
        assert_eq!(output["url"], "https://resolved.example.com");
    }

    #[test]
    fn test_resolve_http_child_input_parallel_parent() {
        let inner_template = Box::new(TaskTemplate::Http(dummy_http_template()));
        let pt = ParallelTemplate {
            task_template: inner_template,
            items_path: "items".into(),
            item_alias: "item".into(),
            concurrency: 1,
            mode: ParallelMode::Rolling,
            max_failures: Some(0),
        };
        let parent = make_parent(TaskTemplate::Parallel(pt), None);
        let job = make_job(Some(0));
        let ctx = json!({"items": [{"name": "a"}]});
        let result = PluginManager::resolve_http_child_input(
            &dummy_http_template(),
            &parent,
            &job,
            &ctx,
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_llm_child_input_simple_parent() {
        let parent = make_parent(TaskTemplate::Http(dummy_http_template()), None);
        let job = make_job(None);
        let ctx = json!({"key": "val"});
        let result = PluginManager::resolve_llm_child_input(
            &dummy_llm_template(),
            &parent,
            &job,
            &ctx,
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_llm_child_input_parent_has_resolved() {
        let parent = make_parent(
            TaskTemplate::Llm(dummy_llm_template()),
            Some(json!({"system_prompt": "sys", "user_prompt": "hi"})),
        );
        let job = make_job(None);
        let ctx = json!({});
        let result = PluginManager::resolve_llm_child_input(
            &dummy_llm_template(),
            &parent,
            &job,
            &ctx,
        );
        assert_eq!(result, Some(json!({"system_prompt": "sys", "user_prompt": "hi"})));
    }

    #[test]
    fn test_resolve_llm_child_input_parallel_parent() {
        let inner_template = Box::new(TaskTemplate::Llm(dummy_llm_template()));
        let pt = ParallelTemplate {
            task_template: inner_template,
            items_path: "items".into(),
            item_alias: "item".into(),
            concurrency: 1,
            mode: ParallelMode::Rolling,
            max_failures: Some(0),
        };
        let parent = make_parent(TaskTemplate::Parallel(pt), None);
        let job = make_job(Some(0));
        let ctx = json!({"items": [{"name": "a"}]});
        let result = PluginManager::resolve_llm_child_input(
            &dummy_llm_template(),
            &parent,
            &job,
            &ctx,
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_derive_child_template_parallel() {
        let inner = Box::new(TaskTemplate::Http(dummy_http_template()));
        let pt = ParallelTemplate {
            task_template: inner,
            items_path: "items".into(),
            item_alias: "item".into(),
            concurrency: 1,
            mode: ParallelMode::Rolling,
            max_failures: Some(0),
        };
        let parent = make_parent(TaskTemplate::Parallel(pt), None);
        let job = make_job(Some(0));
        let (template, tt, override_id) =
            PluginManager::derive_child_template(&parent, &job).unwrap();
        assert!(matches!(template, TaskTemplate::Http(_)));
        assert_eq!(tt, TaskType::Http);
        assert_eq!(override_id, None);
    }

    #[test]
    fn test_derive_child_template_simple() {
        let parent = make_parent(TaskTemplate::Http(dummy_http_template()), None);
        let job = make_job(None);
        let (template, tt, override_id) =
            PluginManager::derive_child_template(&parent, &job).unwrap();
        assert!(matches!(template, TaskTemplate::Http(_)));
        assert_eq!(tt, TaskType::Http);
        assert_eq!(override_id, None);
    }

    #[test]
    fn test_derive_child_template_forkjoin() {
        let item = ForkJoinTaskItem {
            task_key: "key1".into(),
            task_id: Some("fj-task-1".into()),
            name: "task1".into(),
            task_template: TaskTemplate::Http(dummy_http_template()),
        };
        let fj = ForkJoinTemplate {
            tasks: vec![item],
            concurrency: 1,
            mode: ParallelMode::Batch,
            max_failures: Some(0),
        };
        let parent = make_parent(TaskTemplate::ForkJoin(fj), None);
        let job = make_job(Some(0));
        let (template, tt, override_id) =
            PluginManager::derive_child_template(&parent, &job).unwrap();
        assert!(matches!(template, TaskTemplate::Http(_)));
        assert_eq!(tt, TaskType::Http);
        assert_eq!(override_id, Some("fj-task-1".into()));
    }
}
