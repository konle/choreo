use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::approval::service::ApprovalService;
use crate::notification::dispatcher::NotificationDispatcher;
use crate::shared::job::{
    ExecuteTaskJob, ExecuteWorkflowJob, TaskDispatcher, WorkflowCallerContext, WorkflowEvent,
};
use crate::shared::workflow::{TaskInstanceStatus, TaskType, WorkflowInstanceStatus};
use crate::task::service::TaskInstanceService;
use crate::workflow::entity::workflow_definition::{NodeExecutionStatus, WorkflowInstanceEntity};
use crate::workflow::service::WorkflowInstanceService;

#[derive(Debug, Clone)]
pub struct SweeperConfig {
    pub interval_secs: u64,
    pub max_recover_per_cycle: u32,
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            interval_secs: 60,
            max_recover_per_cycle: 10,
        }
    }
}

pub struct Sweeper {
    workflow_instance_svc: Arc<WorkflowInstanceService>,
    task_instance_svc: Arc<TaskInstanceService>,
    approval_svc: Option<ApprovalService>,
    dispatcher: Arc<dyn TaskDispatcher>,
    notification_dispatcher: Option<Arc<dyn NotificationDispatcher>>,
    config: SweeperConfig,
}

impl Sweeper {
    pub fn new(
        workflow_instance_svc: Arc<WorkflowInstanceService>,
        task_instance_svc: Arc<TaskInstanceService>,
        dispatcher: Arc<dyn TaskDispatcher>,
        config: SweeperConfig,
    ) -> Self {
        Self {
            workflow_instance_svc,
            task_instance_svc,
            approval_svc: None,
            dispatcher,
            notification_dispatcher: None,
            config,
        }
    }

    pub fn with_approval_service(mut self, svc: ApprovalService) -> Self {
        self.approval_svc = Some(svc);
        self
    }

    pub fn with_notification_dispatcher(
        mut self,
        disp: Arc<dyn NotificationDispatcher>,
    ) -> Self {
        self.notification_dispatcher = Some(disp);
        self
    }

    pub async fn run_cycle(&self) {
        let zombies = match self
            .workflow_instance_svc
            .scan_zombie_instances(self.config.max_recover_per_cycle)
            .await
        {
            Ok(z) => z,
            Err(e) => {
                error!(error = %e, "sweeper: failed to scan zombie instances");
                Vec::new()
            }
        };

        let mut recovered_running = 0u32;
        let mut recovered_await = 0u32;
        let mut skipped_cas = 0u32;

        if zombies.is_empty() {
            debug!("sweeper: no zombie instances found");
        } else {
            for instance in &zombies {
                let id = &instance.workflow_instance_id;
                let epoch = instance.epoch;

                if let Err(_) = self.workflow_instance_svc.force_clear_lock(id, epoch).await {
                    debug!(workflow_instance_id = %id, epoch, "sweeper: CAS failed, skipping");
                    skipped_cas += 1;
                    continue;
                }

                match instance.status {
                    WorkflowInstanceStatus::Running => match self.recover_running(instance).await {
                        Ok(_) => recovered_running += 1,
                        Err(e) => warn!(
                            workflow_instance_id = %id,
                            error = %e,
                            "sweeper: failed to recover running instance"
                        ),
                    },
                    WorkflowInstanceStatus::Await => match self.recover_await(instance).await {
                        Ok(_) => recovered_await += 1,
                        Err(e) => warn!(
                            workflow_instance_id = %id,
                            error = %e,
                            "sweeper: failed to recover await instance"
                        ),
                    },
                    _ => {}
                }
            }
        }

        let expired_approvals = self.sweep_expired_approvals().await;
        let expired_pauses = self.sweep_expired_pause_nodes().await;

        info!(
            scanned = zombies.len(),
            recovered_running,
            recovered_await,
            skipped_cas,
            expired_approvals,
            expired_pauses,
            "sweeper cycle completed"
        );
    }

    /// Reset to Pending then dispatch Start.
    ///
    /// The Pending→Running transition is intentionally NOT done here.
    /// Per the architecture convention, only a lock-holding Worker may
    /// advance an instance to Running (inside `execute_workflow`). The
    /// sweeper's responsibility ends at returning the instance to the
    /// Pending safety boundary and queuing a Start event; the Worker
    /// that picks up the event will perform the Pending→Running step.
    async fn restart_via_start(&self, instance: &WorkflowInstanceEntity) -> anyhow::Result<()> {
        let id = &instance.workflow_instance_id;

        self.workflow_instance_svc
            .transfer_status_unchecked(id, &WorkflowInstanceStatus::Pending)
            .await
            .map_err(|e| anyhow::anyhow!("→Pending failed: {e}"))?;

        self.dispatcher
            .dispatch_workflow(ExecuteWorkflowJob {
                workflow_instance_id: id.clone(),
                tenant_id: instance.tenant_id.clone(),
                event: WorkflowEvent::Start,
            })
            .await?;

        info!(
            workflow_instance_id = %id,
            action = "restarted",
            "sweeper restarted instance via Start"
        );
        Ok(())
    }

    /// Phase 1: Running → recover.
    ///
    /// If the current node is in Await/Suspended (the engine was mid-callback when it crashed),
    /// a plain `Start` dispatch is useless — the workflow loop would stop immediately. Instead
    /// we transition to Await and delegate to `recover_await` which checks child task status
    /// and supplements missing callbacks or re-dispatches stale tasks.
    async fn recover_running(&self, instance: &WorkflowInstanceEntity) -> anyhow::Result<()> {
        let id = &instance.workflow_instance_id;

        let current_node = instance
            .nodes
            .iter()
            .find(|n| n.node_id == instance.current_node);

        let node_needs_callback = current_node
            .map(|n| {
                matches!(
                    n.status,
                    NodeExecutionStatus::Await | NodeExecutionStatus::Suspended
                )
            })
            .unwrap_or(false);

        if node_needs_callback {
            self.workflow_instance_svc
                .transfer_status_unchecked(id, &WorkflowInstanceStatus::Await)
                .await
                .map_err(|e| anyhow::anyhow!("Running→Await failed: {e}"))?;

            let refreshed = self
                .workflow_instance_svc
                .get_workflow_instance(id.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            info!(
                workflow_instance_id = %id,
                current_node = %instance.current_node,
                action = "delegated_to_recover_await",
                "sweeper: running instance has awaiting node, switching to recover_await"
            );
            return self.recover_await(&refreshed).await;
        }

        self.restart_via_start(instance).await
    }

    /// Phase 2: Await — look up child tasks, supplement missing callbacks or re-dispatch
    async fn recover_await(&self, instance: &WorkflowInstanceEntity) -> anyhow::Result<()> {
        let current_node_id = &instance.current_node;

        let node = instance
            .nodes
            .iter()
            .find(|n| n.node_id == *current_node_id)
            .ok_or_else(|| anyhow::anyhow!("current node {} not found", current_node_id))?;

        let task_template = &node.task_instance.task_template;

        use crate::task::entity::task_definition::TaskTemplate;

        match task_template {
            TaskTemplate::Parallel(_) | TaskTemplate::ForkJoin(_) => {
                self.recover_await_container(instance, node).await
            }
            _ => self.recover_await_single(instance, node).await,
        }
    }

    /// Recover a single-child Await node (Http, SubWorkflow, etc.)
    async fn recover_await_single(
        &self,
        instance: &WorkflowInstanceEntity,
        node: &crate::workflow::entity::workflow_definition::WorkflowNodeInstanceEntity,
    ) -> anyhow::Result<()> {
        let id = &instance.workflow_instance_id;
        let child_task_id = format!("{}-{}", id, node.node_id);

        match self
            .task_instance_svc
            .get_task_instance_entity(child_task_id.clone())
            .await
        {
            Ok(task) => match task.task_status {
                TaskInstanceStatus::Completed | TaskInstanceStatus::Failed => {
                    let status = if task.task_status == TaskInstanceStatus::Completed {
                        NodeExecutionStatus::Success
                    } else {
                        NodeExecutionStatus::Failed
                    };

                    self.supplement_callback(
                        instance,
                        &node.node_id,
                        &child_task_id,
                        status,
                        task.output.clone(),
                        task.error_message.clone(),
                        task.input.clone(),
                    )
                    .await?;

                    info!(
                        workflow_instance_id = %id,
                        child_task_id = %child_task_id,
                        action = "callback_supplemented",
                        "sweeper supplemented missing callback"
                    );
                }
                _ => {
                    self.redispatch_task(instance, &child_task_id, &node.node_id)
                        .await?;
                    info!(
                        workflow_instance_id = %id,
                        child_task_id = %child_task_id,
                        action = "task_redispatched",
                        "sweeper redispatched stale task"
                    );
                }
            },
            Err(_) => {
                self.restart_via_start(instance).await?;
            }
        }
        Ok(())
    }

    /// Recover a container Await node (Parallel / ForkJoin)
    async fn recover_await_container(
        &self,
        instance: &WorkflowInstanceEntity,
        node: &crate::workflow::entity::workflow_definition::WorkflowNodeInstanceEntity,
    ) -> anyhow::Result<()> {
        let id = &instance.workflow_instance_id;
        let state = node
            .task_instance
            .output
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no state in parallel/forkjoin node output"))?;

        let total_items = state["total_items"].as_u64().unwrap_or(0) as usize;
        let dispatched_count = state["dispatched_count"].as_u64().unwrap_or(0) as usize;
        let success_count = state["success_count"].as_u64().unwrap_or(0) as usize;
        let failed_count = state["failed_count"].as_u64().unwrap_or(0) as usize;
        let known_completed = success_count + failed_count;

        let mut supplemented = 0u32;
        let mut redispatched = 0u32;

        for index in 0..dispatched_count {
            let child_task_id = format!("{}-{}-{}", id, node.node_id, index);

            let task = match self
                .task_instance_svc
                .get_task_instance_entity(child_task_id.clone())
                .await
            {
                Ok(t) => t,
                Err(_) => continue,
            };

            match task.task_status {
                TaskInstanceStatus::Completed => {
                    self.supplement_callback(
                        instance,
                        &node.node_id,
                        &child_task_id,
                        NodeExecutionStatus::Success,
                        task.output.clone(),
                        task.error_message.clone(),
                        task.input.clone(),
                    )
                    .await?;
                    supplemented += 1;
                }
                TaskInstanceStatus::Failed => {
                    self.supplement_callback(
                        instance,
                        &node.node_id,
                        &child_task_id,
                        NodeExecutionStatus::Failed,
                        task.output.clone(),
                        task.error_message.clone(),
                        task.input.clone(),
                    )
                    .await?;
                    supplemented += 1;
                }
                TaskInstanceStatus::Pending | TaskInstanceStatus::Running => {
                    self.redispatch_task(instance, &child_task_id, &node.node_id)
                        .await?;
                    redispatched += 1;
                }
                _ => {}
            }
        }

        info!(
            workflow_instance_id = %id,
            node_id = %node.node_id,
            supplemented,
            redispatched,
            total_items,
            dispatched_count,
            known_completed,
            "sweeper recovered container node"
        );
        Ok(())
    }

    async fn supplement_callback(
        &self,
        instance: &WorkflowInstanceEntity,
        node_id: &str,
        child_task_id: &str,
        status: NodeExecutionStatus,
        output: Option<serde_json::Value>,
        error_message: Option<String>,
        input: Option<serde_json::Value>,
    ) -> anyhow::Result<()> {
        self.dispatcher
            .dispatch_workflow(ExecuteWorkflowJob {
                workflow_instance_id: instance.workflow_instance_id.clone(),
                tenant_id: instance.tenant_id.clone(),
                event: WorkflowEvent::NodeCallback {
                    node_id: node_id.to_string(),
                    child_task_id: child_task_id.to_string(),
                    status,
                    output,
                    error_message,
                    input,
                },
            })
            .await
    }

    async fn redispatch_task(
        &self,
        instance: &WorkflowInstanceEntity,
        task_instance_id: &str,
        node_id: &str,
    ) -> anyhow::Result<()> {
        self.dispatcher
            .dispatch_task(ExecuteTaskJob {
                task_instance_id: task_instance_id.to_string(),
                tenant_id: instance.tenant_id.clone(),
                caller_context: Some(WorkflowCallerContext {
                    workflow_instance_id: instance.workflow_instance_id.clone(),
                    node_id: node_id.to_string(),
                    parent_task_instance_id: None,
                    item_index: None,
                }),
            })
            .await
    }

    /// Phase 3: Scan expired approval instances and reject them + send NodeCallback(Failed).
    async fn sweep_expired_approvals(&self) -> u32 {
        let approval_svc = match &self.approval_svc {
            Some(s) => s,
            None => return 0,
        };

        let expired = match approval_svc
            .scan_expired_approvals(self.config.max_recover_per_cycle)
            .await
        {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "sweeper: failed to scan expired approvals");
                return 0;
            }
        };

        let mut count = 0u32;
        for approval in &expired {
            if let Err(e) = approval_svc.expire_approval(approval).await {
                warn!(
                    approval_id = %approval.id,
                    error = %e,
                    "sweeper: failed to expire approval"
                );
                continue;
            }

            if let Err(e) = self
                .dispatcher
                .dispatch_workflow(ExecuteWorkflowJob {
                    workflow_instance_id: approval.workflow_instance_id.clone(),
                    tenant_id: approval.tenant_id.clone(),
                    event: WorkflowEvent::NodeCallback {
                        node_id: approval.node_id.clone(),
                        child_task_id: approval.id.clone(),
                        status: NodeExecutionStatus::Failed,
                        output: Some(serde_json::json!({
                            "approval_expired": true,
                            "expires_at": approval.expires_at,
                        })),
                        error_message: Some("approval expired".to_string()),
                        input: None,
                    },
                })
                .await
            {
                warn!(
                    approval_id = %approval.id,
                    error = %e,
                    "sweeper: failed to dispatch expired approval callback"
                );
                continue;
            }

            info!(
                approval_id = %approval.id,
                workflow_instance_id = %approval.workflow_instance_id,
                node_id = %approval.node_id,
                "sweeper: expired approval → rejected + callback dispatched"
            );
            count += 1;
        }
        count
    }

    async fn sweep_expired_pause_nodes(&self) -> u32 {
        use crate::task::entity::task_definition::{PauseMode, TaskTemplate};

        let instances = match self
            .workflow_instance_svc
            .scan_instances_by_status(
                &WorkflowInstanceStatus::Suspended,
                self.config.max_recover_per_cycle,
            )
            .await
        {
            Ok(list) => list,
            Err(e) => {
                error!(error = %e, "sweeper: failed to scan suspended instances for pause");
                return 0;
            }
        };

        let now = chrono::Utc::now();
        let mut count = 0u32;

        for instance in &instances {
            for node in &instance.nodes {
                if node.node_type != TaskType::Pause {
                    continue;
                }
                if node.status != NodeExecutionStatus::Suspended {
                    continue;
                }

                let is_auto = matches!(
                    &node.task_instance.task_template,
                    TaskTemplate::Pause(t) if t.mode == PauseMode::Auto
                );
                if !is_auto {
                    continue;
                }

                let resume_at = node
                    .task_instance
                    .output
                    .as_ref()
                    .and_then(|o| o.get("resume_at"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());

                let expired = resume_at.map(|t| now >= t).unwrap_or(false);
                if !expired {
                    continue;
                }

                let child_task_id = format!("{}-{}", instance.workflow_instance_id, node.node_id);

                if let Err(e) = self
                    .supplement_callback(
                        instance,
                        &node.node_id,
                        &child_task_id,
                        NodeExecutionStatus::Success,
                        node.task_instance.output.clone(),
                        None,
                        None,
                    )
                    .await
                {
                    warn!(
                        workflow_instance_id = %instance.workflow_instance_id,
                        node_id = %node.node_id,
                        error = %e,
                        "sweeper: failed to dispatch pause auto callback"
                    );
                    continue;
                }

                info!(
                    workflow_instance_id = %instance.workflow_instance_id,
                    node_id = %node.node_id,
                    "sweeper: pause auto timer expired → Success callback dispatched"
                );
                count += 1;
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::entity::{ApprovalInstanceEntity, ApprovalStatus};
    use crate::approval::repository::ApprovalRepository;
    use crate::approval::repository::RepositoryError as ApprovalRepoError;
    use crate::shared::workflow::TaskType as TaskType_;
    use crate::task::entity::task_definition::{
        ApprovalMode, ApprovalTemplate, ApproverRule, PauseMode, PauseTemplate, SelfApprovalPolicy,
        TaskInstanceEntity, TaskTemplate as TTemplate, TaskTransitionFields,
    };
    use crate::task::repository::RepositoryError as TaskRepoError;
    use crate::task::repository::TaskInstanceEntityRepository;
    use crate::user::entity::TenantRole;
    use crate::user::entity::UserTenantRole;
    use crate::user::repository::UserTenantRoleRepository;
    use crate::workflow::entity::workflow_definition::{
        NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
    };
    use crate::workflow::repository::RepositoryError as WfRepoError;
    use crate::workflow::repository::WorkflowInstanceRepository;
    use chrono::{Duration, Utc};
    use common::pagination::PaginatedData;
    use std::sync::Arc;
    use std::sync::Mutex;

    // ── Mock Task Dispatcher ─────────────────────────────────────────

    #[derive(Clone)]
    struct MockDispatcher {
        dispatched_workflows: Arc<Mutex<Vec<ExecuteWorkflowJob>>>,
        dispatched_tasks: Arc<Mutex<Vec<ExecuteTaskJob>>>,
    }

    #[async_trait::async_trait]
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

    impl MockDispatcher {
        fn new() -> Self {
            Self {
                dispatched_workflows: Arc::new(Mutex::new(vec![])),
                dispatched_tasks: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    // ── Mock WorkflowInstanceRepository ──────────────────────────────

    struct MockWfRepo {
        instances: Mutex<Vec<WorkflowInstanceEntity>>,
        zombies: Mutex<Vec<WorkflowInstanceEntity>>,
        by_status: Mutex<Vec<WorkflowInstanceEntity>>,
        lock_cas_ok: Mutex<bool>,
    }

    #[async_trait::async_trait]
    impl WorkflowInstanceRepository for MockWfRepo {
        async fn get_workflow_instance(
            &self,
            id: String,
        ) -> Result<WorkflowInstanceEntity, WfRepoError> {
            let instances = self.instances.lock().unwrap();
            instances
                .iter()
                .find(|i| i.workflow_instance_id == id)
                .cloned()
                .ok_or_else(|| "not found".into())
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
            id: &str,
            _: &WorkflowInstanceStatus,
            _: &WorkflowInstanceStatus,
        ) -> Result<WorkflowInstanceEntity, WfRepoError> {
            let instances = self.instances.lock().unwrap();
            instances
                .iter()
                .find(|i| i.workflow_instance_id == id)
                .cloned()
                .ok_or_else(|| "not found".into())
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
            _: &WorkflowInstanceEntity,
        ) -> Result<WorkflowInstanceEntity, WfRepoError> {
            unreachable!()
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
            Ok(self.zombies.lock().unwrap().clone())
        }
        async fn force_clear_lock(&self, _: &str, _: u64) -> Result<(), WfRepoError> {
            if *self.lock_cas_ok.lock().unwrap() {
                Ok(())
            } else {
                Err("CAS conflict".into())
            }
        }
        async fn scan_instances_by_status(
            &self,
            _: &WorkflowInstanceStatus,
            _: u32,
        ) -> Result<Vec<WorkflowInstanceEntity>, WfRepoError> {
            Ok(self.by_status.lock().unwrap().clone())
        }
    }

    // ── Mock TaskInstanceEntityRepository ────────────────────────────

    struct MockTaskRepo {
        instances: Mutex<Vec<TaskInstanceEntity>>,
    }

    #[async_trait::async_trait]
    impl TaskInstanceEntityRepository for MockTaskRepo {
        async fn create_task_instance_entity(
            &self,
            _: TaskInstanceEntity,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
        async fn get_task_instance_entity(
            &self,
            id: String,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            let instances = self.instances.lock().unwrap();
            instances
                .iter()
                .find(|t| t.task_instance_id == id)
                .cloned()
                .ok_or_else(|| "task instance not found".into())
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
            _: TaskTransitionFields,
        ) -> Result<TaskInstanceEntity, TaskRepoError> {
            unreachable!()
        }
    }

    // ── Mock ApprovalRepository ──────────────────────────────────────

    struct MockApprovalRepo;

    #[async_trait::async_trait]
    impl ApprovalRepository for MockApprovalRepo {
        async fn create(
            &self,
            _: &ApprovalInstanceEntity,
        ) -> Result<ApprovalInstanceEntity, ApprovalRepoError> {
            unreachable!()
        }
        async fn get_by_id(
            &self,
            _: &str,
            _: &str,
        ) -> Result<ApprovalInstanceEntity, ApprovalRepoError> {
            unreachable!()
        }
        async fn update(
            &self,
            e: &ApprovalInstanceEntity,
        ) -> Result<ApprovalInstanceEntity, ApprovalRepoError> {
            Ok(e.clone())
        }
        async fn find_by_workflow_and_node(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<Option<ApprovalInstanceEntity>, ApprovalRepoError> {
            unreachable!()
        }
        async fn list_pending_by_approver(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<ApprovalInstanceEntity>, ApprovalRepoError> {
            Ok(vec![])
        }
        async fn list_by_tenant(
            &self,
            _: &str,
        ) -> Result<Vec<ApprovalInstanceEntity>, ApprovalRepoError> {
            Ok(vec![])
        }
        async fn scan_expired_approvals(
            &self,
            _: u32,
        ) -> Result<Vec<ApprovalInstanceEntity>, ApprovalRepoError> {
            Ok(vec![ApprovalInstanceEntity {
                id: "approval-1".into(),
                tenant_id: "t1".into(),
                workflow_instance_id: "wf1".into(),
                node_id: "a1".into(),
                title: "".into(),
                description: None,
                approval_mode: ApprovalMode::Any,
                approvers: vec![],
                decisions: vec![],
                status: ApprovalStatus::Pending,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                expires_at: Some(Utc::now() - Duration::hours(1)),
                applicant_id: None,
            }])
        }
    }

    // ── Mock UserTenantRoleRepository ────────────────────────────────

    struct MockRoleRepo;

    #[async_trait::async_trait]
    impl UserTenantRoleRepository for MockRoleRepo {
        async fn assign_role(
            &self,
            _: &str,
            _: &str,
            _: &TenantRole,
        ) -> Result<UserTenantRole, ApprovalRepoError> {
            unreachable!()
        }
        async fn get_role(&self, _: &str, _: &str) -> Result<UserTenantRole, ApprovalRepoError> {
            unreachable!()
        }
        async fn list_by_tenant(&self, _: &str) -> Result<Vec<UserTenantRole>, ApprovalRepoError> {
            Ok(vec![])
        }
        async fn list_by_user(&self, _: &str) -> Result<Vec<UserTenantRole>, ApprovalRepoError> {
            Ok(vec![])
        }
        async fn remove_role(&self, _: &str, _: &str) -> Result<(), ApprovalRepoError> {
            Ok(())
        }
        async fn list_users_by_role(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<UserTenantRole>, ApprovalRepoError> {
            Ok(vec![])
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn make_instance(
        id: &str,
        status: WorkflowInstanceStatus,
        current_node: &str,
        nodes: Vec<WorkflowNodeInstanceEntity>,
    ) -> WorkflowInstanceEntity {
        WorkflowInstanceEntity {
            workflow_instance_id: id.into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: current_node.into(),
            current_node: current_node.into(),
            nodes,
            epoch: 1,
            locked_by: Some("previous-worker".into()),
            locked_duration: Some(30000),
            locked_at: Some(Utc::now() - Duration::minutes(5)),
            parent_context: None,
            depth: 0,
            created_by: None,
        }
    }

    fn default_task_instance() -> TaskInstanceEntity {
        let now = Utc::now();
        TaskInstanceEntity {
            id: "".into(),
            tenant_id: "".into(),
            task_id: "".into(),
            task_name: "".into(),
            task_type: TaskType_::Http,
            task_template: TTemplate::Http(
                crate::task::entity::task_definition::TaskHttpTemplate {
                    url: "/x".into(),
                    method: crate::task::entity::task_definition::HttpMethod::Get,
                    headers: vec![],
                    body: vec![],
                    form: vec![],
                    retry_count: 0,
                    retry_delay: 0,
                    timeout: 30,
                    success_condition: None,
                },
            ),
            task_status: TaskInstanceStatus::Pending,
            task_instance_id: "".into(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
            input: None,
            output: None,
            error_message: None,
            execution_duration: None,
            caller_context: None,
        }
    }

    fn make_node(
        node_id: &str,
        node_status: NodeExecutionStatus,
        task_type: TaskType_,
        template: TTemplate,
    ) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.into(),
            node_type: task_type.clone(),
            task_instance: TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: "t1".into(),
                task_id: "".into(),
                task_name: node_id.into(),
                task_type,
                task_template: template,
                task_status: TaskInstanceStatus::Pending,
                task_instance_id: format!("wf1-{}", node_id),
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
            status: node_status,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_sweeper(
        wf_repo: Arc<MockWfRepo>,
        task_repo: Arc<MockTaskRepo>,
        dispatcher: Arc<MockDispatcher>,
        with_approval: bool,
    ) -> Sweeper {
        let ti_svc = Arc::new(TaskInstanceService::new(task_repo));
        let wf_svc = Arc::new(WorkflowInstanceService::new(wf_repo, ti_svc.clone()));
        let mut sweeper = Sweeper::new(
            wf_svc,
            ti_svc,
            dispatcher,
            SweeperConfig {
                interval_secs: 60,
                max_recover_per_cycle: 10,
            },
        );
        if with_approval {
            let approval_repo = Arc::new(MockApprovalRepo);
            let role_repo = Arc::new(MockRoleRepo);
            let approval_svc = ApprovalService::new(approval_repo, role_repo);
            sweeper = sweeper.with_approval_service(approval_svc);
        }
        sweeper
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_cycle_no_zombies_does_nothing() {
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![]),
            zombies: Mutex::new(vec![]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        assert!(dispatcher.dispatched_workflows.lock().unwrap().is_empty());
        assert!(dispatcher.dispatched_tasks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn run_cycle_cas_failure_skips_instance() {
        let node = make_node(
            "n1",
            NodeExecutionStatus::Pending,
            TaskType_::Http,
            TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                url: "/x".into(),
                method: crate::task::entity::task_definition::HttpMethod::Get,
                headers: vec![],
                body: vec![],
                form: vec![],
                retry_count: 0,
                retry_delay: 0,
                timeout: 30,
                success_condition: None,
            }),
        );
        let instance = make_instance("wf1", WorkflowInstanceStatus::Running, "n1", vec![node]);
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance]),
            zombies: Mutex::new(vec![make_instance(
                "wf1",
                WorkflowInstanceStatus::Running,
                "n1",
                vec![],
            )]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(false),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        assert!(dispatcher.dispatched_workflows.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn recover_running_instance_with_pending_node_restarts_via_start() {
        let node = make_node(
            "n1",
            NodeExecutionStatus::Pending,
            TaskType_::Http,
            TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                url: "/x".into(),
                method: crate::task::entity::task_definition::HttpMethod::Get,
                headers: vec![],
                body: vec![],
                form: vec![],
                retry_count: 0,
                retry_delay: 0,
                timeout: 30,
                success_condition: None,
            }),
        );
        let instance = make_instance("wf1", WorkflowInstanceStatus::Running, "n1", vec![node]);
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone()]),
            zombies: Mutex::new(vec![instance]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        assert_eq!(workflows.len(), 1);
        assert_eq!(workflows[0].workflow_instance_id, "wf1");
        assert!(matches!(workflows[0].event, WorkflowEvent::Start));
    }

    #[tokio::test]
    async fn recover_running_instance_with_await_node_delegates_to_recover_await() {
        let node = make_node(
            "a1",
            NodeExecutionStatus::Await,
            TaskType_::Http,
            TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                url: "/cb".into(),
                method: crate::task::entity::task_definition::HttpMethod::Get,
                headers: vec![],
                body: vec![],
                form: vec![],
                retry_count: 0,
                retry_delay: 0,
                timeout: 30,
                success_condition: None,
            }),
        );
        let instance = make_instance("wf1", WorkflowInstanceStatus::Running, "a1", vec![node]);
        let completed_task = TaskInstanceEntity {
            task_instance_id: "wf1-a1".into(),
            task_status: TaskInstanceStatus::Completed,
            output: Some(serde_json::json!({"result": "ok"})),
            ..default_task_instance()
        };
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone()]),
            zombies: Mutex::new(vec![instance]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![completed_task]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        assert_eq!(workflows.len(), 1);
        match &workflows[0].event {
            WorkflowEvent::NodeCallback {
                node_id,
                child_task_id,
                status,
                ..
            } => {
                assert_eq!(node_id, "a1");
                assert_eq!(child_task_id, "wf1-a1");
                assert_eq!(*status, NodeExecutionStatus::Success);
            }
            _ => panic!("expected NodeCallback"),
        }
    }

    #[tokio::test]
    async fn recover_running_instance_with_suspended_node_delegates_to_recover_await() {
        let node = make_node(
            "p1",
            NodeExecutionStatus::Suspended,
            TaskType_::Pause,
            TTemplate::Pause(PauseTemplate {
                wait_seconds: 60,
                mode: PauseMode::Auto,
            }),
        );
        let instance = make_instance("wf1", WorkflowInstanceStatus::Running, "p1", vec![node]);
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone()]),
            zombies: Mutex::new(vec![instance]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        // Since task not found → restart_via_start
        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        assert_eq!(workflows.len(), 1);
        assert!(matches!(workflows[0].event, WorkflowEvent::Start));
    }

    #[tokio::test]
    async fn recover_await_single_completed_task_supplements_callback() {
        let node = make_node(
            "h1",
            NodeExecutionStatus::Await,
            TaskType_::Http,
            TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                url: "/cb".into(),
                method: crate::task::entity::task_definition::HttpMethod::Get,
                headers: vec![],
                body: vec![],
                form: vec![],
                retry_count: 0,
                retry_delay: 0,
                timeout: 30,
                success_condition: None,
            }),
        );
        let instance = make_instance("wf1", WorkflowInstanceStatus::Await, "h1", vec![node]);
        let completed = TaskInstanceEntity {
            task_instance_id: "wf1-h1".into(),
            task_status: TaskInstanceStatus::Completed,
            output: Some(serde_json::json!({"result": "done"})),
            ..default_task_instance()
        };
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone()]),
            zombies: Mutex::new(vec![instance]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![completed]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        assert_eq!(workflows.len(), 1);
        match &workflows[0].event {
            WorkflowEvent::NodeCallback {
                node_id,
                child_task_id,
                status,
                output,
                ..
            } => {
                assert_eq!(node_id, "h1");
                assert_eq!(child_task_id, "wf1-h1");
                assert_eq!(*status, NodeExecutionStatus::Success);
                assert_eq!(output.as_ref().unwrap()["result"], "done");
            }
            _ => panic!("expected NodeCallback"),
        }
    }

    #[tokio::test]
    async fn recover_await_single_failed_task_supplements_callback() {
        let node = make_node(
            "h2",
            NodeExecutionStatus::Await,
            TaskType_::Http,
            TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                url: "/fail".into(),
                method: crate::task::entity::task_definition::HttpMethod::Get,
                headers: vec![],
                body: vec![],
                form: vec![],
                retry_count: 0,
                retry_delay: 0,
                timeout: 30,
                success_condition: None,
            }),
        );
        let instance = make_instance("wf2", WorkflowInstanceStatus::Await, "h2", vec![node]);
        let failed = TaskInstanceEntity {
            task_instance_id: "wf2-h2".into(),
            task_status: TaskInstanceStatus::Failed,
            error_message: Some("timeout".into()),
            ..default_task_instance()
        };
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone()]),
            zombies: Mutex::new(vec![instance]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![failed]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        assert_eq!(workflows.len(), 1);
        match &workflows[0].event {
            WorkflowEvent::NodeCallback {
                status,
                error_message,
                ..
            } => {
                assert_eq!(*status, NodeExecutionStatus::Failed);
                assert_eq!(error_message.as_ref().unwrap(), "timeout");
            }
            _ => panic!("expected NodeCallback"),
        }
    }

    #[tokio::test]
    async fn recover_await_single_stale_task_redispatches() {
        let node = make_node(
            "h3",
            NodeExecutionStatus::Await,
            TaskType_::Http,
            TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                url: "/stale".into(),
                method: crate::task::entity::task_definition::HttpMethod::Get,
                headers: vec![],
                body: vec![],
                form: vec![],
                retry_count: 0,
                retry_delay: 0,
                timeout: 30,
                success_condition: None,
            }),
        );
        let instance = make_instance("wf3", WorkflowInstanceStatus::Await, "h3", vec![node]);
        let stale = TaskInstanceEntity {
            task_instance_id: "wf3-h3".into(),
            task_status: TaskInstanceStatus::Pending,
            ..default_task_instance()
        };
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone()]),
            zombies: Mutex::new(vec![instance]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![stale]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let tasks = dispatcher.dispatched_tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_instance_id, "wf3-h3");
    }

    #[tokio::test]
    async fn recover_await_single_no_task_restarts_via_start() {
        let node = make_node(
            "h4",
            NodeExecutionStatus::Await,
            TaskType_::Http,
            TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                url: "/new".into(),
                method: crate::task::entity::task_definition::HttpMethod::Get,
                headers: vec![],
                body: vec![],
                form: vec![],
                retry_count: 0,
                retry_delay: 0,
                timeout: 30,
                success_condition: None,
            }),
        );
        let instance = make_instance("wf4", WorkflowInstanceStatus::Await, "h4", vec![node]);
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone()]),
            zombies: Mutex::new(vec![instance]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        assert_eq!(workflows.len(), 1);
        assert!(matches!(workflows[0].event, WorkflowEvent::Start));
    }

    #[tokio::test]
    async fn recover_await_container_mixed_states() {
        use crate::task::entity::task_definition::ForkJoinTemplate;
        let template = TTemplate::ForkJoin(ForkJoinTemplate {
            tasks: vec![],
            concurrency: 2,
            mode: crate::task::entity::task_definition::ParallelMode::Batch,
            max_failures: None,
        });
        let mut node = make_node(
            "fj1",
            NodeExecutionStatus::Await,
            TaskType_::ForkJoin,
            template,
        );
        node.task_instance.output = Some(serde_json::json!({
            "total_items": 3,
            "dispatched_count": 3,
            "success_count": 1,
            "failed_count": 0,
        }));

        let instance = make_instance("wf5", WorkflowInstanceStatus::Await, "fj1", vec![node]);
        let completed = TaskInstanceEntity {
            task_instance_id: "wf5-fj1-0".into(),
            task_status: TaskInstanceStatus::Completed,
            output: Some(serde_json::json!({"idx": 0})),
            ..default_task_instance()
        };
        let failed = TaskInstanceEntity {
            task_instance_id: "wf5-fj1-1".into(),
            task_status: TaskInstanceStatus::Failed,
            error_message: Some("fail".into()),
            ..default_task_instance()
        };
        let stale = TaskInstanceEntity {
            task_instance_id: "wf5-fj1-2".into(),
            task_status: TaskInstanceStatus::Running,
            ..default_task_instance()
        };
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone()]),
            zombies: Mutex::new(vec![instance]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![completed, failed, stale]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        assert_eq!(workflows.len(), 2); // 2 callbacks supplemented (completed + failed)
        let tasks = dispatcher.dispatched_tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1); // 1 stale task redispatched
    }

    #[tokio::test]
    async fn sweep_expired_approvals_rejects_and_dispatches_callback() {
        let node = make_node(
            "a1",
            NodeExecutionStatus::Suspended,
            TaskType_::Approval,
            TTemplate::Approval(ApprovalTemplate {
                name: "approve".into(),
                title: "test".into(),
                description: None,
                approvers: vec![ApproverRule::User("u1".into())],
                approval_mode: ApprovalMode::Any,
                timeout: Some(1),
                self_approval: SelfApprovalPolicy::Skip,
            }),
        );
        let instance = make_instance("wf1", WorkflowInstanceStatus::Suspended, "a1", vec![node]);
        // Add a dummy zombie so run_cycle doesn't return early before sweep_expired_approvals
        let dummy_zombie = make_instance(
            "zombie",
            WorkflowInstanceStatus::Running,
            "dummy",
            vec![make_node(
                "dummy",
                NodeExecutionStatus::Success,
                TaskType_::Http,
                TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                    url: "/x".into(),
                    method: crate::task::entity::task_definition::HttpMethod::Get,
                    headers: vec![],
                    body: vec![],
                    form: vec![],
                    retry_count: 0,
                    retry_delay: 0,
                    timeout: 30,
                    success_condition: None,
                }),
            )],
        );
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone(), dummy_zombie.clone()]),
            zombies: Mutex::new(vec![dummy_zombie]),
            by_status: Mutex::new(vec![]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), true);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        let approval_callbacks: Vec<_> = workflows.iter().filter(|w| {
            matches!(&w.event, WorkflowEvent::NodeCallback { node_id, .. } if node_id == "a1")
        }).collect();
        assert_eq!(approval_callbacks.len(), 1);
        match &approval_callbacks[0].event {
            WorkflowEvent::NodeCallback {
                status,
                error_message,
                ..
            } => {
                assert_eq!(*status, NodeExecutionStatus::Failed);
                assert_eq!(error_message.as_ref().unwrap(), "approval expired");
            }
            _ => panic!("expected NodeCallback"),
        }
    }

    #[tokio::test]
    async fn sweep_expired_pause_auto_nodes_awaken() {
        let now = Utc::now();
        let past = now - Duration::seconds(10);
        let mut node = make_node(
            "p1",
            NodeExecutionStatus::Suspended,
            TaskType_::Pause,
            TTemplate::Pause(PauseTemplate {
                wait_seconds: 5,
                mode: PauseMode::Auto,
            }),
        );
        node.task_instance.output = Some(serde_json::json!({
            "resume_at": past.to_rfc3339(),
            "mode": "Auto",
            "wait_seconds": 5,
        }));

        let instance = make_instance("wf1", WorkflowInstanceStatus::Suspended, "p1", vec![node]);
        // Add a dummy zombie so run_cycle doesn't return early before sweep_expired_pause_nodes
        let dummy_zombie = make_instance(
            "zombie",
            WorkflowInstanceStatus::Running,
            "dummy",
            vec![make_node(
                "dummy",
                NodeExecutionStatus::Success,
                TaskType_::Http,
                TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                    url: "/x".into(),
                    method: crate::task::entity::task_definition::HttpMethod::Get,
                    headers: vec![],
                    body: vec![],
                    form: vec![],
                    retry_count: 0,
                    retry_delay: 0,
                    timeout: 30,
                    success_condition: None,
                }),
            )],
        );
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone(), dummy_zombie.clone()]),
            zombies: Mutex::new(vec![dummy_zombie]),
            by_status: Mutex::new(vec![instance]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        let pause_callbacks: Vec<_> = workflows.iter().filter(|w| {
            matches!(&w.event, WorkflowEvent::NodeCallback { node_id, .. } if node_id == "p1")
        }).collect();
        assert_eq!(pause_callbacks.len(), 1);
        match &pause_callbacks[0].event {
            WorkflowEvent::NodeCallback { status, .. } => {
                assert_eq!(*status, NodeExecutionStatus::Success);
            }
            _ => panic!("expected NodeCallback"),
        }
    }

    #[tokio::test]
    async fn sweep_expired_pause_manual_nodes_ignored() {
        let now = Utc::now();
        let past = now - Duration::seconds(10);
        let mut node = make_node(
            "p2",
            NodeExecutionStatus::Suspended,
            TaskType_::Pause,
            TTemplate::Pause(PauseTemplate {
                wait_seconds: 0,
                mode: PauseMode::Manual,
            }),
        );
        node.task_instance.output = Some(serde_json::json!({
            "resume_at": past.to_rfc3339(),
            "mode": "Manual",
        }));

        let instance = make_instance("wf2", WorkflowInstanceStatus::Suspended, "p2", vec![node]);
        let dummy_zombie = make_instance(
            "zombie",
            WorkflowInstanceStatus::Running,
            "dummy",
            vec![make_node(
                "dummy",
                NodeExecutionStatus::Success,
                TaskType_::Http,
                TTemplate::Http(crate::task::entity::task_definition::TaskHttpTemplate {
                    url: "/x".into(),
                    method: crate::task::entity::task_definition::HttpMethod::Get,
                    headers: vec![],
                    body: vec![],
                    form: vec![],
                    retry_count: 0,
                    retry_delay: 0,
                    timeout: 30,
                    success_condition: None,
                }),
            )],
        );
        let wf_repo = Arc::new(MockWfRepo {
            instances: Mutex::new(vec![instance.clone(), dummy_zombie.clone()]),
            zombies: Mutex::new(vec![dummy_zombie]),
            by_status: Mutex::new(vec![instance]),
            lock_cas_ok: Mutex::new(true),
        });
        let task_repo = Arc::new(MockTaskRepo {
            instances: Mutex::new(vec![]),
        });
        let dispatcher = Arc::new(MockDispatcher::new());

        let sweeper = make_sweeper(wf_repo, task_repo, dispatcher.clone(), false);
        sweeper.run_cycle().await;

        let workflows = dispatcher.dispatched_workflows.lock().unwrap();
        let pause_callbacks: Vec<_> = workflows.iter().filter(|w| {
            matches!(&w.event, WorkflowEvent::NodeCallback { node_id, .. } if node_id == "p2")
        }).collect();
        assert_eq!(
            pause_callbacks.len(),
            0,
            "manual pause nodes should not be auto-resumed"
        );
    }
}
