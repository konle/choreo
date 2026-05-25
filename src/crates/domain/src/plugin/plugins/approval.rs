use async_trait::async_trait;
use tracing::{error, info};

use crate::approval::entity::ApprovalStatus;
use crate::approval::service::ApprovalService;
use crate::plugin::interface::{ExecutionResult, PluginExecutor, PluginInterface};
use crate::shared::workflow::TaskType;
use crate::task::entity::task_definition::TaskTemplate;
use crate::workflow::entity::workflow_definition::{
    NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
};

pub struct ApprovalPlugin {
    approval_svc: ApprovalService,
}

impl ApprovalPlugin {
    pub fn new(approval_svc: ApprovalService) -> Self {
        Self { approval_svc }
    }
}

#[async_trait]
impl PluginInterface for ApprovalPlugin {
    async fn execute(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        workflow_instance: &mut WorkflowInstanceEntity,
    ) -> anyhow::Result<ExecutionResult> {
        let template = match &node_instance.task_instance.task_template {
            TaskTemplate::Approval(t) => t,
            other => {
                error!(node_id = %node_instance.node_id, template = ?other, "invalid template for ApprovalPlugin");
                return Err(anyhow::anyhow!("Invalid task template for ApprovalPlugin"));
            }
        };

        node_instance.task_instance.input = Some(serde_json::json!({
            "title": template.title.clone(),
            "name": template.name.clone(),
            "description": template.description.clone(),
        }));

        let approval = self
            .approval_svc
            .create_approval(
                &workflow_instance.tenant_id,
                &workflow_instance.workflow_instance_id,
                &node_instance.node_id,
                template,
                &node_instance.context,
                workflow_instance.created_by.clone(),
            )
            .await
            .map_err(|e| {
                error!(
                    workflow_instance_id = %workflow_instance.workflow_instance_id,
                    node_id = %node_instance.node_id,
                    error = %e,
                    "failed to create approval instance"
                );
                anyhow::anyhow!("Failed to create approval instance: {}", e)
            })?;

        info!(
            approval_id = %approval.id,
            workflow_instance_id = %workflow_instance.workflow_instance_id,
            node_id = %node_instance.node_id,
            "approval created, workflow suspended"
        );

        node_instance.task_instance.output = Some(serde_json::json!({
            "approval_id": approval.id,
            "approvers": approval.approvers,
            "approval_mode": format!("{:?}", approval.approval_mode),
        }));

        Ok(ExecutionResult::suspended())
    }

    async fn handle_callback(
        &self,
        _executor: &dyn PluginExecutor,
        node_instance: &mut WorkflowNodeInstanceEntity,
        _workflow_instance: &mut WorkflowInstanceEntity,
        _child_task_id: &str,
        status: &NodeExecutionStatus,
        output: &Option<serde_json::Value>,
        error_message: &Option<String>,
        _input: &Option<serde_json::Value>,
    ) -> anyhow::Result<ExecutionResult> {
        node_instance.error_message = error_message.clone();
        node_instance.task_instance.input = _input.clone();
        node_instance.task_instance.output = output.clone();
        node_instance.task_instance.error_message = error_message.clone();

        match status {
            NodeExecutionStatus::Success => Ok(ExecutionResult::success(None)),
            NodeExecutionStatus::Skipped => Ok(ExecutionResult::skipped(None)),
            NodeExecutionStatus::Failed => Ok(ExecutionResult::failed()),
            _ => Ok(ExecutionResult::pending()),
        }
    }

    fn plugin_type(&self) -> TaskType {
        TaskType::Approval
    }
}

pub fn approval_status_to_node_status(status: &ApprovalStatus) -> NodeExecutionStatus {
    match status {
        ApprovalStatus::Approved => NodeExecutionStatus::Success,
        ApprovalStatus::Rejected => NodeExecutionStatus::Failed,
        ApprovalStatus::Pending => NodeExecutionStatus::Suspended,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::entity::{ApprovalInstanceEntity, ApprovalStatus};
    use crate::approval::repository::{ApprovalRepository, RepositoryError};
    use crate::plugin::interface::{ChildStatus, PluginInterface};
    use crate::shared::workflow::{TaskInstanceStatus, WorkflowInstanceStatus};
    use crate::task::entity::task_definition::{
        ApprovalMode, ApprovalTemplate, ApproverRule, SelfApprovalPolicy, TaskInstanceEntity,
        TaskTemplate as TTemplate,
    };
    use crate::user::entity::{TenantRole, UserTenantRole};
    use crate::user::repository::UserTenantRoleRepository;
    use crate::workflow::entity::workflow_definition::{
        NodeExecutionStatus, WorkflowInstanceEntity, WorkflowNodeInstanceEntity,
    };
    use chrono::Utc;
    use std::sync::Arc;

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
        async fn resolve_child_status(&self, _: &str, _: &TTemplate) -> ChildStatus {
            unreachable!()
        }
    }

    struct MockApprovalRepo {
        created: std::sync::Mutex<Option<ApprovalInstanceEntity>>,
    }

    #[async_trait::async_trait]
    impl ApprovalRepository for MockApprovalRepo {
        async fn create(
            &self,
            entity: &ApprovalInstanceEntity,
        ) -> Result<ApprovalInstanceEntity, RepositoryError> {
            let mut created = self.created.lock().unwrap();
            *created = Some(entity.clone());
            Ok(entity.clone())
        }
        async fn get_by_id(
            &self,
            _tenant_id: &str,
            _id: &str,
        ) -> Result<ApprovalInstanceEntity, RepositoryError> {
            Ok(self.created.lock().unwrap().clone().unwrap())
        }
        async fn update(
            &self,
            entity: &ApprovalInstanceEntity,
        ) -> Result<ApprovalInstanceEntity, RepositoryError> {
            let mut created = self.created.lock().unwrap();
            *created = Some(entity.clone());
            Ok(entity.clone())
        }
        async fn find_by_workflow_and_node(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<Option<ApprovalInstanceEntity>, RepositoryError> {
            Ok(self.created.lock().unwrap().clone())
        }
        async fn list_pending_by_approver(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
            Ok(vec![])
        }
        async fn list_by_tenant(
            &self,
            _: &str,
        ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
            Ok(vec![])
        }
        async fn scan_expired_approvals(
            &self,
            _: u32,
        ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
            Ok(vec![])
        }
    }

    struct MockRoleRepo;

    #[async_trait::async_trait]
    impl UserTenantRoleRepository for MockRoleRepo {
        async fn assign_role(
            &self,
            _: &str,
            _: &str,
            _: &TenantRole,
        ) -> Result<UserTenantRole, RepositoryError> {
            unreachable!()
        }
        async fn get_role(&self, _: &str, _: &str) -> Result<UserTenantRole, RepositoryError> {
            unreachable!()
        }
        async fn list_by_tenant(&self, _: &str) -> Result<Vec<UserTenantRole>, RepositoryError> {
            Ok(vec![])
        }
        async fn list_by_user(&self, _: &str) -> Result<Vec<UserTenantRole>, RepositoryError> {
            Ok(vec![])
        }
        async fn remove_role(&self, _: &str, _: &str) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn list_users_by_role(
            &self,
            _tenant_id: &str,
            _role: &str,
        ) -> Result<Vec<UserTenantRole>, RepositoryError> {
            Ok(vec![])
        }
    }

    fn make_node(
        _plugin: &ApprovalPlugin,
        wf: &WorkflowInstanceEntity,
        node_id: &str,
    ) -> WorkflowNodeInstanceEntity {
        let now = Utc::now();
        WorkflowNodeInstanceEntity {
            node_id: node_id.to_string(),
            node_type: TaskType::Approval,
            task_instance: TaskInstanceEntity {
                id: format!("ti-{}", node_id),
                tenant_id: wf.tenant_id.clone(),
                task_id: "".into(),
                task_name: "approval".to_string(),
                task_type: TaskType::Approval,
                task_template: TTemplate::Approval(ApprovalTemplate {
                    name: "approve".into(),
                    title: "Approve this".into(),
                    description: Some("Please approve".into()),
                    approvers: vec![ApproverRule::User("approver1".into())],
                    approval_mode: ApprovalMode::Any,
                    timeout: Some(3600),
                    self_approval: SelfApprovalPolicy::Skip,
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
            context: serde_json::json!({"assignee": "user_from_context"}),
            next_node: None,
            status: NodeExecutionStatus::Pending,
            error_message: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_instance() -> WorkflowInstanceEntity {
        let now = Utc::now();
        WorkflowInstanceEntity {
            workflow_instance_id: "wf1".into(),
            tenant_id: "t1".into(),
            workflow_meta_id: "m1".into(),
            workflow_version: 1,
            status: WorkflowInstanceStatus::Running,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            context: serde_json::json!({}),
            entry_node: "a1".into(),
            current_node: "a1".into(),
            nodes: vec![],
            epoch: 0,
            locked_by: None,
            locked_duration: None,
            locked_at: None,
            parent_context: None,
            depth: 0,
            created_by: Some("applicant1".into()),
        }
    }

    #[tokio::test]
    async fn execute_creates_approval_and_returns_suspended() {
        let approval_repo = Arc::new(MockApprovalRepo {
            created: std::sync::Mutex::new(None),
        });
        let svc = ApprovalService::new(approval_repo.clone(), Arc::new(MockRoleRepo));
        let plugin = ApprovalPlugin::new(svc);
        let mut wf = make_instance();
        let mut node = make_node(&plugin, &wf, "a1");

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Suspended);
        let created = approval_repo.created.lock().unwrap().take().unwrap();
        assert_eq!(created.tenant_id, "t1");
        assert_eq!(created.workflow_instance_id, "wf1");
        assert_eq!(created.node_id, "a1");
        assert!(created.expires_at.is_some());
        let output = node.task_instance.output.as_ref().unwrap();
        assert_eq!(output["approval_id"], created.id);
    }

    #[tokio::test]
    async fn execute_with_context_variable_approver_uses_variable() {
        let approval_repo = Arc::new(MockApprovalRepo {
            created: std::sync::Mutex::new(None),
        });
        let svc = ApprovalService::new(approval_repo.clone(), Arc::new(MockRoleRepo));
        let plugin = ApprovalPlugin::new(svc);
        let mut wf = WorkflowInstanceEntity {
            created_by: Some("applicant1".into()),
            context: serde_json::json!({"assignee": "user_from_context"}),
            ..make_instance()
        };
        let mut node = WorkflowNodeInstanceEntity {
            task_instance: TaskInstanceEntity {
                task_template: TTemplate::Approval(ApprovalTemplate {
                    name: "ctx_approve".into(),
                    title: "Approval".into(),
                    description: None,
                    approvers: vec![ApproverRule::ContextVariable("assignee".into())],
                    approval_mode: ApprovalMode::Any,
                    timeout: None,
                    self_approval: SelfApprovalPolicy::Allow,
                }),
                ..make_node(&plugin, &wf, "a2").task_instance
            },
            ..make_node(&plugin, &wf, "a2")
        };

        let result = plugin
            .execute(&StubExecutor, &mut node, &mut wf)
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Suspended);
        let created = approval_repo.created.lock().unwrap().take().unwrap();
        assert_eq!(created.approvers, vec!["user_from_context"]);
    }

    #[tokio::test]
    async fn handle_callback_success_returns_success() {
        let approval_repo = Arc::new(MockApprovalRepo {
            created: std::sync::Mutex::new(None),
        });
        let svc = ApprovalService::new(approval_repo.clone(), Arc::new(MockRoleRepo));
        let plugin = ApprovalPlugin::new(svc);
        let mut node = make_node(&plugin, &make_instance(), "a3");
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &StubExecutor,
                &mut node,
                &mut wf,
                "tid",
                &NodeExecutionStatus::Success,
                &None,
                &None,
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Success);
    }

    #[tokio::test]
    async fn handle_callback_failed_returns_failed() {
        let approval_repo = Arc::new(MockApprovalRepo {
            created: std::sync::Mutex::new(None),
        });
        let svc = ApprovalService::new(approval_repo.clone(), Arc::new(MockRoleRepo));
        let plugin = ApprovalPlugin::new(svc);
        let mut node = make_node(&plugin, &make_instance(), "a4");
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &StubExecutor,
                &mut node,
                &mut wf,
                "tid",
                &NodeExecutionStatus::Failed,
                &None,
                &Some("rejected".into()),
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Failed);
        assert_eq!(node.error_message, Some("rejected".into()));
    }

    #[tokio::test]
    async fn handle_callback_skipped_returns_skipped() {
        let approval_repo = Arc::new(MockApprovalRepo {
            created: std::sync::Mutex::new(None),
        });
        let svc = ApprovalService::new(approval_repo.clone(), Arc::new(MockRoleRepo));
        let plugin = ApprovalPlugin::new(svc);
        let mut node = make_node(&plugin, &make_instance(), "a5");
        let mut wf = make_instance();

        let result = plugin
            .handle_callback(
                &StubExecutor,
                &mut node,
                &mut wf,
                "tid",
                &NodeExecutionStatus::Skipped,
                &None,
                &None,
                &None,
            )
            .await
            .unwrap();

        assert_eq!(result.status, NodeExecutionStatus::Skipped);
    }

    #[test]
    fn plugin_type_is_approval() {
        let svc = ApprovalService::new(
            Arc::new(MockApprovalRepo {
                created: std::sync::Mutex::new(None),
            }),
            Arc::new(MockRoleRepo),
        );
        let plugin = ApprovalPlugin::new(svc);
        assert_eq!(plugin.plugin_type(), TaskType::Approval);
    }

    #[test]
    fn approval_status_conversion() {
        assert_eq!(
            approval_status_to_node_status(&ApprovalStatus::Approved),
            NodeExecutionStatus::Success
        );
        assert_eq!(
            approval_status_to_node_status(&ApprovalStatus::Rejected),
            NodeExecutionStatus::Failed
        );
        assert_eq!(
            approval_status_to_node_status(&ApprovalStatus::Pending),
            NodeExecutionStatus::Suspended
        );
    }
}
