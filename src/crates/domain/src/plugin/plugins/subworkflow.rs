use async_trait::async_trait;
use tracing::{error, info};

use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::job::{ExecuteWorkflowJob, WorkflowCallerContext, WorkflowEvent};
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::TaskTemplate;
use crate::workflow::entity::workflow_definition::{
    WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};
use crate::workflow::service::{WorkflowDefinitionService, WorkflowInstanceService};

const MAX_SUB_WORKFLOW_DEPTH: u32 = 10;

pub struct SubWorkflowPlugin {
    definition_svc: WorkflowDefinitionService,
    instance_svc: WorkflowInstanceService,
}

impl SubWorkflowPlugin {
    pub fn new(
        definition_svc: WorkflowDefinitionService,
        instance_svc: WorkflowInstanceService,
    ) -> Self {
        Self {
            definition_svc,
            instance_svc,
        }
    }
}

#[async_trait]
impl PluginInterface for SubWorkflowPlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::SubWorkflow(t) => t,
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for SubWorkflowPlugin");
                return Err(anyhow::anyhow!(
                    "Invalid task template for SubWorkflowPlugin"
                ));
            }
        };

        // Re-evaluation: check if a child workflow instance already exists
        if let Some(existing_child_id) = node_instance
            .task_instance
            .output
            .as_ref()
            .and_then(|o| o.get("child_workflow_instance_id"))
            .and_then(|v| v.as_str())
        {
            match self
                .instance_svc
                .get_workflow_instance(existing_child_id.to_string())
                .await
            {
                Ok(child) => {
                    use crate::shared::workflow::WorkflowInstanceStatus;
                    use crate::workflow::entity::workflow_definition::NodeExecutionStatus;
                    return match child.status {
                        WorkflowInstanceStatus::Completed => {
                            info!(
                                parent_workflow_id = %workflow_instance.workflow_instance_id,
                                child_workflow_id = %existing_child_id,
                                "sub-workflow re-evaluation: child already completed"
                            );
                            node_instance.task_instance.output = Some(serde_json::json!({
                                "child_workflow_instance_id": existing_child_id,
                            }));
                            Ok(ExecutionResult::success(None))
                        }
                        WorkflowInstanceStatus::Failed => {
                            info!(
                                parent_workflow_id = %workflow_instance.workflow_instance_id,
                                child_workflow_id = %existing_child_id,
                                "sub-workflow re-evaluation: child still failed"
                            );
                            Ok(ExecutionResult::failed())
                        }
                        _ => {
                            info!(
                                parent_workflow_id = %workflow_instance.workflow_instance_id,
                                child_workflow_id = %existing_child_id,
                                child_status = ?child.status,
                                "sub-workflow re-evaluation: child still running, awaiting callback"
                            );
                            Ok(ExecutionResult {
                                status: NodeExecutionStatus::Await,
                                dispatch_jobs: vec![],
                                dispatch_workflow_jobs: vec![],
                                jump_to_node: None,
                            })
                        }
                    };
                }
                Err(_) => {
                    // Child instance not found — fall through to create a new one
                }
            }
        }

        let child_depth = workflow_instance.depth + 1;
        if child_depth > MAX_SUB_WORKFLOW_DEPTH {
            error!(
                workflow_instance_id = %workflow_instance.workflow_instance_id,
                depth = child_depth,
                max = MAX_SUB_WORKFLOW_DEPTH,
                "sub-workflow nesting depth exceeded"
            );
            return Err(anyhow::anyhow!(
                "Sub-workflow nesting depth exceeded maximum ({}), possible circular reference",
                MAX_SUB_WORKFLOW_DEPTH
            ));
        }

        let workflow_entity = self
            .definition_svc
            .get_workflow_entity(template.workflow_meta_id.clone(), template.workflow_version)
            .await
            .map_err(|e| {
                error!(
                    workflow_meta_id = %template.workflow_meta_id,
                    version = template.workflow_version,
                    error = %e,
                    "failed to load sub-workflow template"
                );
                anyhow::anyhow!("Failed to load sub-workflow template: {}", e)
            })?;

        let mut child_context = workflow_instance.context.clone();
        if !template.form.is_empty() {
            if let serde_json::Value::Object(ref mut ctx) = child_context {
                for field in &template.form {
                    ctx.insert(
                        field.key.clone(),
                        serde_json::to_value(&field.value).unwrap_or_default(),
                    );
                }
            }
        }

        let child_context_snapshot = child_context.clone();

        let parent_ctx = WorkflowCallerContext {
            workflow_instance_id: workflow_instance.workflow_instance_id.clone(),
            node_id: node_instance.node_id.clone(),
            parent_task_instance_id: None,
            item_index: None,
        };

        let child_instance = self
            .instance_svc
            .create_instance(
                &workflow_instance.tenant_id,
                &workflow_entity,
                child_context,
                Some(parent_ctx),
                child_depth,
                workflow_instance.created_by.clone(),
            )
            .await
            .map_err(|e| {
                error!(
                    parent_workflow_id = %workflow_instance.workflow_instance_id,
                    error = %e,
                    "failed to create sub-workflow instance"
                );
                anyhow::anyhow!("Failed to create sub-workflow instance: {}", e)
            })?;

        info!(
            parent_workflow_id = %workflow_instance.workflow_instance_id,
            child_workflow_id = %child_instance.workflow_instance_id,
            depth = child_depth,
            "sub-workflow created"
        );

        node_instance.task_instance.input = Some(serde_json::json!({
            "workflow_meta_id": template.workflow_meta_id,
            "workflow_version": template.workflow_version,
            "child_context": child_context_snapshot,
        }));

        node_instance.task_instance.output = Some(serde_json::json!({
            "child_workflow_instance_id": child_instance.workflow_instance_id,
        }));

        let job = ExecuteWorkflowJob {
            workflow_instance_id: child_instance.workflow_instance_id,
            tenant_id: workflow_instance.tenant_id.clone(),
            event: WorkflowEvent::Start,
        };

        Ok(ExecutionResult::async_dispatch_workflow(job))
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::SubWorkflow
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::interface::PluginExecutor;
    use crate::plugin::interface::PluginInterface;
    use crate::shared::job::WorkflowEvent;
    use crate::shared::workflow::{
        TaskInstanceStatus, TaskType as TaskType_, WorkflowInstanceStatus,
    };
    use crate::task::entity::task_definition::{
        SubWorkflowTemplate, TaskInstanceEntity, TaskTemplate as TTemplate,
    };
    use crate::task::repository::RepositoryError as TaskRepoError;
    use crate::task::repository::TaskInstanceEntityRepository;
    use crate::workflow::entity::workflow_definition::{
        NodeExecutionStatus, WorkflowEntity, WorkflowInstanceEntity, WorkflowNodeEntity,
        WorkflowNodeInstanceEntity,
    };
    use crate::workflow::repository::{
        RepositoryError as WfRepoError, WorkflowDefinitionRepository, WorkflowInstanceRepository,
    };
    use chrono::Utc;
    use common::pagination::PaginatedData;
    use std::sync::Arc;
    use std::sync::Mutex;

    struct StubExecutor;

    #[async_trait::async_trait]
    impl PluginExecutor for StubExecutor {
        async fn execute_node_instance(
            &self,
            _: &mut WorkflowNodeInstanceEntity,
            _: &mut WorkflowInstanceEntity,
        ) -> anyhow::Result<ExecutionResult> {
            unreachable!()
        }
        async fn handle_node_callback(
            &self,
            _: &mut WorkflowNodeInstanceEntity,
            _: &mut WorkflowInstanceEntity,
            _: &str,
            _: &NodeExecutionStatus,
            _: &Option<serde_json::Value>,
            _: &Option<String>,
            _: &Option<serde_json::Value>,
        ) -> anyhow::Result<ExecutionResult> {
            unreachable!()
        }
        async fn resolve_child_status(
            &self,
            _: &str,
            _: &TTemplate,
        ) -> crate::plugin::interface::ChildStatus {
            unreachable!()
        }
    }

    struct MockDefRepo {
        workflow_entity: Mutex<Option<WorkflowEntity>>,
    }

    #[async_trait::async_trait]
    impl WorkflowDefinitionRepository for MockDefRepo {
        async fn get_workflow_entity(
            &self,
            _workflow_meta_id: String,
            _version: u32,
        ) -> Result<WorkflowEntity, WfRepoError> {
            self.workflow_entity
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| "not found".into())
        }
        async fn list_workflow_entities(
            &self,
            _: &str,
        ) -> Result<Vec<WorkflowEntity>, WfRepoError> {
            Ok(vec![])
        }
        async fn save_workflow_entity(&self, _: &WorkflowEntity) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn max_version(&self, _: String) -> Result<u32, WfRepoError> {
            Ok(1)
        }
        async fn transition_status(
            &self,
            _: String,
            _: u32,
            _: &crate::shared::workflow::WorkflowStatus,
            _: &crate::shared::workflow::WorkflowStatus,
        ) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn get_workflow_meta_entity(
            &self,
            _: String,
        ) -> Result<crate::workflow::entity::workflow_definition::WorkflowMetaEntity, WfRepoError>
        {
            unreachable!()
        }
        async fn get_workflow_meta_entity_scoped(
            &self,
            _: &str,
            _: &str,
        ) -> Result<crate::workflow::entity::workflow_definition::WorkflowMetaEntity, WfRepoError>
        {
            unreachable!()
        }
        async fn list_workflow_meta_entities(
            &self,
            _: &str,
        ) -> Result<
            Vec<crate::workflow::entity::workflow_definition::WorkflowMetaEntity>,
            WfRepoError,
        > {
            Ok(vec![])
        }
        async fn save_workflow_meta_entity(
            &self,
            _: &crate::workflow::entity::workflow_definition::WorkflowMetaEntity,
        ) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn delete_workflow_meta_entity(&self, _: &str, _: &str) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn create_workflow_meta_entity(
            &self,
            _: &crate::workflow::entity::workflow_definition::WorkflowMetaEntity,
        ) -> Result<crate::workflow::entity::workflow_definition::WorkflowMetaEntity, WfRepoError>
        {
            unreachable!()
        }
    }

    struct MockInstanceRepo {
        created: Mutex<Option<WorkflowInstanceEntity>>,
        get_response: Mutex<Option<Result<WorkflowInstanceEntity, WfRepoError>>>,
    }

    #[async_trait::async_trait]
    impl WorkflowInstanceRepository for MockInstanceRepo {
        async fn get_workflow_instance(
            &self,
            _id: String,
        ) -> Result<WorkflowInstanceEntity, WfRepoError> {
            self.get_response
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| Err("not found".into()))
        }
        async fn get_workflow_instance_scoped(
            &self,
            _: &str,
            _: &str,
        ) -> Result<WorkflowInstanceEntity, WfRepoError> {
            unreachable!()
        }
        async fn list_workflow_instances(
            &self,
            _: &str,
            _: &crate::workflow::entity::query::WorkflowInstanceQuery,
        ) -> Result<PaginatedData<WorkflowInstanceEntity>, WfRepoError> {
            unreachable!()
        }
        async fn transfer_status(
            &self,
            _: &str,
            _: &WorkflowInstanceStatus,
            _: &WorkflowInstanceStatus,
        ) -> Result<WorkflowInstanceEntity, WfRepoError> {
            unreachable!()
        }
        async fn acquire_lock(
            &self,
            _: &str,
            _: &str,
            _: u64,
        ) -> Result<WorkflowInstanceEntity, WfRepoError> {
            unreachable!()
        }
        async fn release_lock(&self, _: &str, _: &str) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn create_workflow_instance(
            &self,
            instance: &WorkflowInstanceEntity,
        ) -> Result<WorkflowInstanceEntity, WfRepoError> {
            let mut created = self.created.lock().unwrap();
            *created = Some(instance.clone());
            Ok(instance.clone())
        }
        async fn save_workflow_instance(
            &self,
            _: &WorkflowInstanceEntity,
        ) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn scan_zombie_instances(
            &self,
            _: u32,
        ) -> Result<Vec<WorkflowInstanceEntity>, WfRepoError> {
            Ok(vec![])
        }
        async fn force_clear_lock(&self, _: &str, _: u64) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn scan_instances_by_status(
            &self,
            _: &WorkflowInstanceStatus,
            _: u32,
        ) -> Result<Vec<WorkflowInstanceEntity>, WfRepoError> {
            Ok(vec![])
        }
    }

    struct MockTaskInstanceRepo;

    #[async_trait::async_trait]
    impl TaskInstanceEntityRepository for MockTaskInstanceRepo {
        async fn create_task_instance_entity(
            &self,
            _: TaskInstanceEntity,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
        async fn get_task_instance_entity(
            &self,
            _: String,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
        async fn get_task_instance_entity_scoped(
            &self,
            _: &str,
            _: &str,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
        async fn list_task_instance_entities(
            &self,
            _: &crate::task::entity::query::TaskInstanceQuery,
        ) -> Result<PaginatedData<TaskInstanceEntity>, TaskRepoError> {
            unreachable!()
        }
        async fn update_task_instance_entity(
            &self,
            _: TaskInstanceEntity,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
        async fn transfer_status_with_fields(
            &self,
            _: &str,
            _: &TaskInstanceStatus,
            _: &TaskInstanceStatus,
            _: crate::task::entity::task_definition::TaskTransitionFields,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
    }

    fn make_workflow_entity() -> WorkflowEntity {
        WorkflowEntity {
            workflow_meta_id: "child-meta".into(),
            version: 1,
            status: crate::shared::workflow::WorkflowStatus::Published,
            nodes: vec![WorkflowNodeEntity {
                node_id: "child-start".into(),
                node_type: TaskType_::Http,
                task_id: None,
                config: TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                    url: "/child".into(),
                    method: crate::task::entity::task_definition::HttpMethod::Get,
                    headers: vec![],
                    body: vec![],
                    form: vec![],
                    retry_count: 0,
                    retry_delay: 0,
                    timeout: 30,
                    success_condition: None,
                }),
                context: serde_json::json!({}),
                next_node: None,
            }],
            entry_node: "child-start".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
        }
    }

    fn make_node(
        _plugin: &SubWorkflowPlugin,
        wf: &WorkflowInstanceEntity,
        node_id: &str,
    ) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.to_string(),
            node_type: TaskType::SubWorkflow,
            task_instance: TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: wf.tenant_id.clone(),
                task_id: "".into(),
                task_name: "subwf".to_string(),
                task_type: TaskType::SubWorkflow,
                task_template: TTemplate::SubWorkflow(SubWorkflowTemplate {
                    workflow_meta_id: "child-meta".into(),
                    workflow_version: 1,
                    form: vec![],
                    timeout: None,
                }),
                task_status: TaskInstanceStatus::Pending,
                task_instance_id: format!("{}-{}", wf.workflow_instance_id, node_id),
                created_at: now,
                updated_at: now,
                deleted_at: None,
                input: None,
                output: None,
                error_message: None,
                execution_duration: None,
                caller_context: None,
            },
            context: serde_json::json!({}),
            next_node: None,
            status: NodeExecutionStatus::Pending,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_instance(depth: u32) -> WorkflowInstanceEntity {
        let now = Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: "wf-parent".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "parent-meta".into(),
            workflow_version: 1,
            status: WorkflowInstanceStatus::Running,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "s1".into(),
            current_node: "s1".into(),
            nodes: vec![],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth,
            created_by: Some("user1".into()),
        }
    }

    #[tokio::test]
    async fn execute_creates_child_workflow() {
        let def_repo = Arc::new(MockDefRepo {
            workflow_entity: Mutex::new(Some(make_workflow_entity())),
        });
        let inst_repo = Arc::new(MockInstanceRepo {
            created: Mutex::new(None),
            get_response: Mutex::new(None),
        });
        let ti_repo = Arc::new(MockTaskInstanceRepo);
        let ti_svc = Arc::new(crate::task::service::TaskInstanceService::new(ti_repo));
        let def_svc = crate::workflow::service::WorkflowDefinitionService::new(def_repo);
        let inst_svc =
            crate::workflow::service::WorkflowInstanceService::new(inst_repo.clone(), ti_svc);
        let plugin = SubWorkflowPlugin::new(def_svc, inst_svc);
        let mut wf = make_instance(0);
        let mut node = make_node(&plugin, &wf, "s1");

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        assert_eq!(result.dispatch_workflow_jobs.len(), 1);
        let job = &result.dispatch_workflow_jobs[0];
        assert_eq!(job.tenant_id, "t1");
        assert!(matches!(job.event, WorkflowEvent::Start));

        let created = inst_repo.created.lock().unwrap().take().unwrap();
        assert_eq!(created.tenant_id, "t1");
        assert_eq!(created.workflow_meta_id, "child-meta");
        assert_eq!(created.depth, 1);
        assert!(created.parent_context.is_some());

        let output = node.task_instance.output.as_ref().unwrap();
        assert_eq!(
            output["child_workflow_instance_id"],
            created.workflow_instance_id
        );
    }

    #[tokio::test]
    async fn execute_depth_exceeded_returns_error() {
        let def_repo = Arc::new(MockDefRepo {
            workflow_entity: Mutex::new(Some(make_workflow_entity())),
        });
        let inst_repo = Arc::new(MockInstanceRepo {
            created: Mutex::new(None),
            get_response: Mutex::new(None),
        });
        let ti_repo = Arc::new(MockTaskInstanceRepo);
        let ti_svc = Arc::new(crate::task::service::TaskInstanceService::new(ti_repo));
        let def_svc = crate::workflow::service::WorkflowDefinitionService::new(def_repo);
        let inst_svc = crate::workflow::service::WorkflowInstanceService::new(inst_repo, ti_svc);
        let plugin = SubWorkflowPlugin::new(def_svc, inst_svc);
        let mut wf = make_instance(10);
        let mut node = make_node(&plugin, &wf, "s_deep");

        let result = plugin.execute(&StubExecutor, &mut node, &mut wf).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("depth exceeded"));
    }

    #[tokio::test]
    async fn reevaluation_child_completed_returns_success() {
        let def_repo = Arc::new(MockDefRepo {
            workflow_entity: Mutex::new(Some(make_workflow_entity())),
        });
        let child = WorkflowInstanceEntity {
            workflow_instance_id: "child-wf".into(),
            status: WorkflowInstanceStatus::Completed,
            ..make_instance(0)
        };
        let inst_repo = Arc::new(MockInstanceRepo {
            created: Mutex::new(None),
            get_response: Mutex::new(Some(Ok(child))),
        });
        let ti_repo = Arc::new(MockTaskInstanceRepo);
        let ti_svc = Arc::new(crate::task::service::TaskInstanceService::new(ti_repo));
        let def_svc = crate::workflow::service::WorkflowDefinitionService::new(def_repo);
        let inst_svc = crate::workflow::service::WorkflowInstanceService::new(inst_repo, ti_svc);
        let plugin = SubWorkflowPlugin::new(def_svc, inst_svc);
        let mut wf = make_instance(0);
        let mut node = make_node(&plugin, &wf, "s2");
        node.task_instance.output =
            Some(serde_json::json!({"child_workflow_instance_id": "child-wf"}));

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
    }

    #[tokio::test]
    async fn reevaluation_child_failed_returns_failed() {
        let def_repo = Arc::new(MockDefRepo {
            workflow_entity: Mutex::new(Some(make_workflow_entity())),
        });
        let child = WorkflowInstanceEntity {
            workflow_instance_id: "child-wf".into(),
            status: WorkflowInstanceStatus::Failed,
            ..make_instance(0)
        };
        let inst_repo = Arc::new(MockInstanceRepo {
            created: Mutex::new(None),
            get_response: Mutex::new(Some(Ok(child))),
        });
        let ti_repo = Arc::new(MockTaskInstanceRepo);
        let ti_svc = Arc::new(crate::task::service::TaskInstanceService::new(ti_repo));
        let def_svc = crate::workflow::service::WorkflowDefinitionService::new(def_repo);
        let inst_svc = crate::workflow::service::WorkflowInstanceService::new(inst_repo, ti_svc);
        let plugin = SubWorkflowPlugin::new(def_svc, inst_svc);
        let mut wf = make_instance(0);
        let mut node = make_node(&plugin, &wf, "s3");
        node.task_instance.output =
            Some(serde_json::json!({"child_workflow_instance_id": "child-wf"}));

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
    }

    #[tokio::test]
    async fn reevaluation_child_running_returns_await() {
        let def_repo = Arc::new(MockDefRepo {
            workflow_entity: Mutex::new(Some(make_workflow_entity())),
        });
        let child = WorkflowInstanceEntity {
            workflow_instance_id: "child-wf".into(),
            status: WorkflowInstanceStatus::Running,
            ..make_instance(0)
        };
        let inst_repo = Arc::new(MockInstanceRepo {
            created: Mutex::new(None),
            get_response: Mutex::new(Some(Ok(child))),
        });
        let ti_repo = Arc::new(MockTaskInstanceRepo);
        let ti_svc = Arc::new(crate::task::service::TaskInstanceService::new(ti_repo));
        let def_svc = crate::workflow::service::WorkflowDefinitionService::new(def_repo);
        let inst_svc = crate::workflow::service::WorkflowInstanceService::new(inst_repo, ti_svc);
        let plugin = SubWorkflowPlugin::new(def_svc, inst_svc);
        let mut wf = make_instance(0);
        let mut node = make_node(&plugin, &wf, "s4");
        node.task_instance.output =
            Some(serde_json::json!({"child_workflow_instance_id": "child-wf"}));

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
    }

    #[tokio::test]
    async fn reevaluation_child_not_found_creates_new() {
        let def_repo = Arc::new(MockDefRepo {
            workflow_entity: Mutex::new(Some(make_workflow_entity())),
        });
        let inst_repo = Arc::new(MockInstanceRepo {
            created: Mutex::new(None),
            get_response: Mutex::new(Some(Err("not found".into()))),
        });
        let ti_repo = Arc::new(MockTaskInstanceRepo);
        let ti_svc = Arc::new(crate::task::service::TaskInstanceService::new(ti_repo));
        let def_svc = crate::workflow::service::WorkflowDefinitionService::new(def_repo);
        let inst_svc =
            crate::workflow::service::WorkflowInstanceService::new(inst_repo.clone(), ti_svc);
        let plugin = SubWorkflowPlugin::new(def_svc, inst_svc);
        let mut wf = make_instance(0);
        let mut node = make_node(&plugin, &wf, "s5");
        node.task_instance.output =
            Some(serde_json::json!({"child_workflow_instance_id": "child-wf"}));

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Await);
        let created = inst_repo.created.lock().unwrap().take().unwrap();
        assert_eq!(created.depth, 1);
    }

    #[test]
    fn plugin_type_is_subworkflow() {
        let def_repo = Arc::new(MockDefRepo {
            workflow_entity: Mutex::new(None),
        });
        let inst_repo = Arc::new(MockInstanceRepo {
            created: Mutex::new(None),
            get_response: Mutex::new(None),
        });
        let ti_repo = Arc::new(MockTaskInstanceRepo);
        let ti_svc = Arc::new(crate::task::service::TaskInstanceService::new(ti_repo));
        let def_svc = crate::workflow::service::WorkflowDefinitionService::new(def_repo);
        let inst_svc = crate::workflow::service::WorkflowInstanceService::new(inst_repo, ti_svc);
        assert_eq!(
            SubWorkflowPlugin::new(def_svc, inst_svc).plugin_type(),
            TaskType::SubWorkflow
        );
    }
}
