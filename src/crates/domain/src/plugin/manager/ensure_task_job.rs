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

        match &task_instance.task_template {
            TaskTemplate::Http(tpl) => match &parent.task_template {
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
                    task_instance.input = Some(
                        crate::task::http_template_resolve::resolved_http_request_snapshot(
                            tpl, &ctx,
                        ),
                    );
                }
                TaskTemplate::ForkJoin(_) => {
                    task_instance.input = Some(
                        crate::task::http_template_resolve::resolved_http_request_snapshot(
                            tpl,
                            parent_node_ctx,
                        ),
                    );
                }
                TaskTemplate::Http(_) => {
                    let has_resolved_url = parent
                        .input
                        .as_ref()
                        .and_then(|i| i.get("url"))
                        .and_then(|v| v.as_str())
                        .is_some_and(|s| !s.is_empty());
                    task_instance.input = if has_resolved_url {
                        parent.input.clone()
                    } else {
                        Some(
                            crate::task::http_template_resolve::resolved_http_request_snapshot(
                                tpl,
                                parent_node_ctx,
                            ),
                        )
                    };
                }
                _ => {
                    task_instance.input = Some(
                        crate::task::http_template_resolve::resolved_http_request_snapshot(
                            tpl,
                            parent_node_ctx,
                        ),
                    );
                }
            },
            TaskTemplate::Llm(tpl) => {
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
                    TaskTemplate::Llm(_) => {
                        if parent.input.is_some() {
                            task_instance.input = parent.input.clone();
                            // already resolved by run_node
                            task_instance.input.clone().unwrap_or_default();
                        }
                        parent_node_ctx.clone()
                    }
                    _ => parent_node_ctx.clone(),
                };
                if task_instance.input.is_none() {
                    task_instance.input =
                        Some(super::workflow::resolved_llm_request_snapshot(tpl, &ctx));
                }
            }
            _ => {}
        }

        task_svc
            .create_task_instance_entity(task_instance)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}
