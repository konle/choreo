#[cfg(test)]
mod integration_tests {
    use crate::plugin::manager::PluginManager;
    use crate::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob, TaskDispatcher, WorkflowEvent};
    use crate::shared::workflow::{TaskInstanceStatus, TaskType, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::TaskInstanceEntity;
    use crate::task::repository::TaskInstanceEntityRepository;
    use crate::task::service::TaskInstanceService;
    use crate::workflow::entity::workflow_definition::{
        NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
    };
    use crate::workflow::repository::WorkflowInstanceRepository;
    use crate::workflow::entity::query::WorkflowInstanceQuery;
    use async_trait::async_trait;
    use common::pagination::PaginatedData;
    use std::sync::{Arc, Mutex};

    type WfRepoError = crate::workflow::repository::RepositoryError;
    type TaskRepoError = crate::task::repository::RepositoryError;

    // ── MockDispatcher ──

    #[derive(Clone)]
    struct MockDispatcher {
        dispatched_workflows: Arc<Mutex<Vec<ExecuteWorkflowJob>>>,
        dispatched_tasks: Arc<Mutex<Vec<ExecuteTaskJob>>>,
    }

    impl MockDispatcher {
        fn new() -> Self {
            Self {
                dispatched_workflows: Arc::new(Mutex::new(vec![])),
                dispatched_tasks: Arc::new(Mutex::new(vec![])),
            }
        }

        fn take_workflow_jobs(&self) -> Vec<ExecuteWorkflowJob> {
            std::mem::take(&mut *self.dispatched_workflows.lock().unwrap())
        }

        fn workflow_job_count(&self) -> usize {
            self.dispatched_workflows.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl TaskDispatcher for MockDispatcher {
        async fn dispatch_task(&self, job: ExecuteTaskJob) -> anyhow::Result<()> {
            self.dispatched_tasks.lock().unwrap().push(job);
            Ok(())
        }
        async fn dispatch_workflow(&self, job: ExecuteWorkflowJob) -> anyhow::Result<()> {
            self.dispatched_workflows.lock().unwrap().push(job);
            Ok(())
        }
    }

    // ── MockWfRepo ──

    struct MockWfRepo {
        instances: Mutex<Vec<WorkflowInstanceEntity>>,
    }

    impl MockWfRepo {
        fn new(instances: Vec<WorkflowInstanceEntity>) -> Self {
            Self {
                instances: Mutex::new(instances),
            }
        }
    }

    #[async_trait]
    impl WorkflowInstanceRepository for MockWfRepo {
        async fn get_workflow_instance(&self, id: String) -> Result<WorkflowInstanceEntity, WfRepoError> {
            self.instances.lock().unwrap()
                .iter().find(|i| i.workflow_instance_id == id)
                .cloned()
                .ok_or_else(|| "not found".into())
        }
        async fn get_workflow_instance_scoped(&self, _: &str, _: &str) -> Result<WorkflowInstanceEntity, WfRepoError> {
            unreachable!()
        }
        async fn list_workflow_instances(&self, _: &str, _: &WorkflowInstanceQuery) -> Result<PaginatedData<WorkflowInstanceEntity>, WfRepoError> {
            unreachable!()
        }
        async fn transfer_status(&self, id: &str, from: &WorkflowInstanceStatus, to: &WorkflowInstanceStatus) -> Result<WorkflowInstanceEntity, WfRepoError> {
            let mut instances = self.instances.lock().unwrap();
            let inst = instances.iter_mut()
                .find(|i| i.workflow_instance_id == id)
                .ok_or_else(|| WfRepoError::from("not found"))?;
            if inst.status != *from {
                return Err("CAS conflict".into());
            }
            inst.status = to.clone();
            Ok(inst.clone())
        }
        async fn acquire_lock(&self, id: &str, worker: &str, _dur: u64) -> Result<WorkflowInstanceEntity, WfRepoError> {
            let mut instances = self.instances.lock().unwrap();
            let inst = instances.iter_mut()
                .find(|i| i.workflow_instance_id == id)
                .ok_or_else(|| WfRepoError::from("not found"))?;
            inst.locked_by = Some(worker.to_string());
            inst.locked_at = Some(chrono::Utc::now());
            inst.locked_duration = Some(60000);
            Ok(inst.clone())
        }
        async fn release_lock(&self, _: &str, _: &str) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn create_workflow_instance(&self, _: &WorkflowInstanceEntity) -> Result<WorkflowInstanceEntity, WfRepoError> {
            unreachable!()
        }
        async fn save_workflow_instance(&self, inst: &WorkflowInstanceEntity) -> Result<(), WfRepoError> {
            let mut instances = self.instances.lock().unwrap();
            if let Some(pos) = instances.iter().position(|i| i.workflow_instance_id == inst.workflow_instance_id) {
                instances[pos] = inst.clone();
            } else {
                instances.push(inst.clone());
            }
            Ok(())
        }
        async fn scan_zombie_instances(&self, _: u32) -> Result<Vec<WorkflowInstanceEntity>, WfRepoError> {
            unreachable!()
        }
        async fn force_clear_lock(&self, _: &str, _: u64) -> Result<(), WfRepoError> {
            Ok(())
        }
        async fn scan_instances_by_status(&self, _: &WorkflowInstanceStatus, _: u32) -> Result<Vec<WorkflowInstanceEntity>, WfRepoError> {
            unreachable!()
        }
    }

    // ── Mock TaskInstanceEntityRepository ──

    struct MockTaskRepo {
        instances: Mutex<Vec<TaskInstanceEntity>>,
    }

    impl MockTaskRepo {
        fn new() -> Self {
            Self { instances: Mutex::new(vec![]) }
        }
    }

    #[async_trait]
    impl TaskInstanceEntityRepository for MockTaskRepo {
        async fn create_task_instance_entity(&self, inst: TaskInstanceEntity) -> Result<TaskInstanceEntity, TaskRepoError> {
            self.instances.lock().unwrap().push(inst.clone());
            Ok(inst)
        }
        async fn get_task_instance_entity(&self, id: String) -> Result<TaskInstanceEntity, TaskRepoError> {
            self.instances.lock().unwrap()
                .iter().find(|i| i.task_instance_id == id)
                .cloned()
                .ok_or_else(|| "not found".into())
        }
        async fn get_task_instance_entity_scoped(&self, _: &str, _: &str) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
        async fn list_task_instance_entities(&self, _: &crate::task::entity::query::TaskInstanceQuery) -> Result<PaginatedData<TaskInstanceEntity>, TaskRepoError> {
            unreachable!()
        }
        async fn update_task_instance_entity(&self, inst: TaskInstanceEntity) -> Result<TaskInstanceEntity, TaskRepoError> {
            let mut instances = self.instances.lock().unwrap();
            if let Some(pos) = instances.iter().position(|i| i.task_instance_id == inst.task_instance_id) {
                instances[pos] = inst.clone();
            }
            Ok(inst)
        }
        async fn transfer_status_with_fields(
            &self,
            _task_instance_id: &str,
            _from_status: &TaskInstanceStatus,
            _to_status: &TaskInstanceStatus,
            _fields: crate::task::entity::task_definition::TaskTransitionFields,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
    }

    // ── Test Helpers ──

    fn make_pm(instances: Vec<WorkflowInstanceEntity>) -> (PluginManager, MockDispatcher) {
        let wf_repo: Arc<dyn WorkflowInstanceRepository> = Arc::new(MockWfRepo::new(instances));
        let ti_repo: Arc<dyn TaskInstanceEntityRepository> = Arc::new(MockTaskRepo::new());
        let ti_svc = Arc::new(TaskInstanceService::new(ti_repo));
        let wf_svc = Arc::new(
            crate::workflow::service::WorkflowInstanceService::new(wf_repo, ti_svc),
        );
        let dispatcher = MockDispatcher::new();
        let pm = PluginManager::new(
            wf_svc,
            Arc::new(dispatcher.clone()),
        );
        (pm, dispatcher)
    }

    fn make_pending_instance(id: &str) -> WorkflowInstanceEntity {
        let now = chrono::Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: id.into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status: WorkflowInstanceStatus::Pending,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "node0".into(),
            current_node: "node0".into(),
            nodes: vec![],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 1,
            created_by: None,
        }
    }

    fn make_node_instance(
        node_id: &str,
        node_type: TaskType,
        status: NodeExecutionStatus,
        next_node: Option<&str>,
    ) -> WorkflowNodeInstanceEntity {
        let now = chrono::Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.into(),
            node_type: node_type.clone(),
            task_instance: TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: "t1".into(),
                task_id: "td-1".into(),
                task_name: "test".into(),
                task_type: node_type,
                task_template: crate::task::entity::task_definition::TaskTemplate::Grpc,
                task_status: TaskInstanceStatus::Pending,
                task_instance_id: format!("tii-{}", node_id),
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
            next_node: next_node.map(String::from),
            status,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_instance_with_node(
        wf_id: &str,
        status: WorkflowInstanceStatus,
        current_node: &str,
        node: WorkflowNodeInstanceEntity,
    ) -> WorkflowInstanceEntity {
        let now = chrono::Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: wf_id.into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "node0".into(),
            current_node: current_node.into(),
            nodes: vec![node],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 1,
            created_by: None,
        }
    }

    #[tokio::test]
    #[ignore = "requires full VariableService mock setup"]
    async fn process_workflow_job_start_event_with_empty_nodes() {
        let inst = make_pending_instance("wf-1");
        let (pm, _dispatcher) = make_pm(vec![inst]);

        let result = pm
            .process_workflow_job(
                ExecuteWorkflowJob {
                    workflow_instance_id: "wf-1".into(),
                    tenant_id: "t1".into(),
                    event: WorkflowEvent::Start,
                },
                "worker-1",
            )
            .await;
        // Start finishes successfully even without plugins — empty node list
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn process_workflow_job_missing_instance_returns_err() {
        let (pm, _dispatcher) = make_pm(vec![]);

        let result = pm
            .process_workflow_job(
                ExecuteWorkflowJob {
                    workflow_instance_id: "wf-1".into(),
                    tenant_id: "t1".into(),
                    event: WorkflowEvent::Start,
                },
                "worker-1",
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn child_revived_failed_instance_dispatches_events() {
        use crate::shared::job::WorkflowCallerContext;

        let node = make_node_instance(&"n1", TaskType::Http, NodeExecutionStatus::Failed, None);
        let mut inst = make_instance_with_node("wf-1", WorkflowInstanceStatus::Failed, "n1", node);
        inst.parent_context = Some(WorkflowCallerContext {
            workflow_instance_id: "parent-wf".into(),
            node_id: "parent-node".into(),
            parent_task_instance_id: None,
            item_index: None,
        });

        let (pm, dispatcher) = make_pm(vec![inst]);

        let result = pm
            .process_workflow_job(
                ExecuteWorkflowJob {
                    workflow_instance_id: "wf-1".into(),
                    tenant_id: "t1".into(),
                    event: WorkflowEvent::ChildRevived {
                        node_id: "n1".into(),
                        child_id: "child-1".into(),
                    },
                },
                "worker-1",
            )
            .await;
        assert!(result.is_ok());
        let jobs = dispatcher.take_workflow_jobs();
        assert!(!jobs.is_empty(), "expected dispatch jobs");
    }

    #[tokio::test]
    async fn retry_container_child_failed_instance_dispatches_events() {
        let node = make_node_instance(
            "n1",
            TaskType::Http,
            NodeExecutionStatus::Failed,
            None,
        );
        let inst = make_instance_with_node("wf-1", WorkflowInstanceStatus::Failed, "n1", node);

        let (pm, dispatcher) = make_pm(vec![inst]);

        let result = pm
            .process_workflow_job(
                ExecuteWorkflowJob {
                    workflow_instance_id: "wf-1".into(),
                    tenant_id: "t1".into(),
                    event: WorkflowEvent::RetryContainerChild {
                        node_id: "n1".into(),
                        child_task_id: "child-1".into(),
                    },
                },
                "worker-1",
            )
            .await;
        assert!(result.is_ok());
        assert!(dispatcher.workflow_job_count() > 0);
    }

    #[tokio::test]
    async fn child_revived_await_instance_returns_ok() {
        let node = make_node_instance("n1", TaskType::Http, NodeExecutionStatus::Await, None);
        let inst = make_instance_with_node("wf-1", WorkflowInstanceStatus::Await, "n1", node);

        let (pm, _dispatcher) = make_pm(vec![inst]);

        let result = pm
            .process_workflow_job(
                ExecuteWorkflowJob {
                    workflow_instance_id: "wf-1".into(),
                    tenant_id: "t1".into(),
                    event: WorkflowEvent::ChildRevived {
                        node_id: "n1".into(),
                        child_id: "child-1".into(),
                    },
                },
                "worker-1",
            )
            .await;
        assert!(result.is_ok());
    }
}
