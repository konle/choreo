use chrono::{Duration, Utc};
use std::sync::Arc;
use uuid::Uuid;

use crate::approval::entity::{ApprovalDecision, ApprovalInstanceEntity, ApprovalStatus, Decision};
use crate::approval::repository::{ApprovalRepository, RepositoryError};
use crate::task::entity::task_definition::{
    ApprovalMode, ApprovalTemplate, ApproverRule, SelfApprovalPolicy,
};
use crate::user::repository::UserTenantRoleRepository;

#[derive(Clone)]
pub struct ApprovalService {
    pub repository: Arc<dyn ApprovalRepository>,
    pub role_repository: Arc<dyn UserTenantRoleRepository>,
}

pub fn evaluate_approval_mode(
    mode: &ApprovalMode,
    total_approvers: usize,
    decisions: &[ApprovalDecision],
) -> Option<ApprovalStatus> {
    let approves = decisions
        .iter()
        .filter(|d| d.decision == Decision::Approve)
        .count();
    let rejects = decisions
        .iter()
        .filter(|d| d.decision == Decision::Reject)
        .count();

    match mode {
        ApprovalMode::Any => {
            if approves >= 1 {
                Some(ApprovalStatus::Approved)
            } else if rejects >= 1 {
                Some(ApprovalStatus::Rejected)
            } else {
                None
            }
        }
        ApprovalMode::All => {
            if rejects >= 1 {
                Some(ApprovalStatus::Rejected)
            } else if approves == total_approvers {
                Some(ApprovalStatus::Approved)
            } else {
                None
            }
        }
        ApprovalMode::Majority => {
            let threshold = total_approvers / 2 + 1;
            if approves >= threshold {
                Some(ApprovalStatus::Approved)
            } else if rejects >= threshold {
                Some(ApprovalStatus::Rejected)
            } else {
                None
            }
        }
    }
}

impl ApprovalService {
    pub fn new(
        repository: Arc<dyn ApprovalRepository>,
        role_repository: Arc<dyn UserTenantRoleRepository>,
    ) -> Self {
        Self {
            repository,
            role_repository,
        }
    }

    pub async fn create_approval(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        node_id: &str,
        template: &ApprovalTemplate,
        context: &serde_json::Value,
        applicant_id: Option<String>,
    ) -> Result<ApprovalInstanceEntity, RepositoryError> {
        let mut approvers = self
            .resolve_approvers(tenant_id, &template.approvers, context)
            .await?;

        if template.self_approval == SelfApprovalPolicy::Skip {
            if let Some(ref uid) = applicant_id {
                approvers.retain(|id| id != uid);
            }
        }

        if approvers.is_empty() {
            return Err("no approvers resolved from rules (self-approval filtering may have removed all candidates)".into());
        }

        let expires_at = template
            .timeout
            .map(|secs| Utc::now() + Duration::seconds(secs as i64));

        let entity = ApprovalInstanceEntity {
            id: Uuid::new_v4().to_string(),
            tenant_id: tenant_id.to_string(),
            workflow_instance_id: workflow_instance_id.to_string(),
            node_id: node_id.to_string(),
            title: template.title.clone(),
            description: template.description.clone(),
            approval_mode: template.approval_mode.clone(),
            approvers,
            decisions: vec![],
            status: ApprovalStatus::Pending,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            expires_at,
            applicant_id,
        };

        self.repository.create(&entity).await
    }

    pub async fn decide(
        &self,
        tenant_id: &str,
        approval_id: &str,
        user_id: &str,
        decision: Decision,
        comment: Option<String>,
    ) -> Result<ApprovalInstanceEntity, RepositoryError> {
        let mut entity = self.repository.get_by_id(tenant_id, approval_id).await?;

        if entity.status != ApprovalStatus::Pending {
            return Err(format!("approval {} is already {:?}", approval_id, entity.status).into());
        }

        if !entity.approvers.contains(&user_id.to_string()) {
            return Err(format!("user {} is not an approver for {}", user_id, approval_id).into());
        }

        if entity.decisions.iter().any(|d| d.user_id == user_id) {
            return Err(format!("user {} has already decided on {}", user_id, approval_id).into());
        }

        entity.decisions.push(ApprovalDecision {
            user_id: user_id.to_string(),
            decision,
            comment,
            decided_at: Utc::now(),
        });

        let final_status = self.evaluate_mode(&entity);
        if let Some(status) = final_status {
            entity.status = status;
        }

        entity.updated_at = Utc::now();
        self.repository.update(&entity).await
    }

    pub async fn get_by_id(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<ApprovalInstanceEntity, RepositoryError> {
        self.repository.get_by_id(tenant_id, id).await
    }

    pub async fn find_by_workflow_and_node(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        node_id: &str,
    ) -> Result<Option<ApprovalInstanceEntity>, RepositoryError> {
        self.repository
            .find_by_workflow_and_node(tenant_id, workflow_instance_id, node_id)
            .await
    }

    pub async fn list_pending_by_approver(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
        self.repository
            .list_pending_by_approver(tenant_id, user_id)
            .await
    }

    pub async fn list_by_tenant(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
        self.repository.list_by_tenant(tenant_id).await
    }

    fn evaluate_mode(&self, entity: &ApprovalInstanceEntity) -> Option<ApprovalStatus> {
        evaluate_approval_mode(&entity.approval_mode, entity.approvers.len(), &entity.decisions)
    }

    async fn resolve_approvers(
        &self,
        tenant_id: &str,
        rules: &[ApproverRule],
        context: &serde_json::Value,
    ) -> Result<Vec<String>, RepositoryError> {
        let mut user_ids = Vec::new();

        for rule in rules {
            match rule {
                ApproverRule::User(uid) => {
                    if !user_ids.contains(uid) {
                        user_ids.push(uid.clone());
                    }
                }
                ApproverRule::Role(role_name) => {
                    let role_entities = self
                        .role_repository
                        .list_users_by_role(tenant_id, role_name)
                        .await?;
                    for r in role_entities {
                        if !user_ids.contains(&r.user_id) {
                            user_ids.push(r.user_id);
                        }
                    }
                }
                ApproverRule::ContextVariable(var_name) => {
                    if let Some(val) = context.get(var_name) {
                        match val {
                            serde_json::Value::String(s) => {
                                if !user_ids.contains(s) {
                                    user_ids.push(s.clone());
                                }
                            }
                            serde_json::Value::Array(arr) => {
                                for item in arr {
                                    if let Some(s) = item.as_str() {
                                        let owned = s.to_string();
                                        if !user_ids.contains(&owned) {
                                            user_ids.push(owned);
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(user_ids)
    }

    pub async fn scan_expired_approvals(
        &self,
        limit: u32,
    ) -> Result<Vec<ApprovalInstanceEntity>, RepositoryError> {
        self.repository.scan_expired_approvals(limit).await
    }

    pub async fn expire_approval(
        &self,
        approval: &ApprovalInstanceEntity,
    ) -> Result<ApprovalInstanceEntity, RepositoryError> {
        let mut expired = approval.clone();
        expired.status = ApprovalStatus::Rejected;
        expired.updated_at = Utc::now();
        self.repository.update(&expired).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::entity::ApprovalDecision;
    use crate::task::entity::task_definition::ApprovalMode;
    use chrono::Utc;

    fn make_decision(user: &str, decision: Decision) -> ApprovalDecision {
        ApprovalDecision {
            user_id: user.into(),
            decision,
            comment: None,
            decided_at: Utc::now(),
        }
    }

    #[test]
    fn any_mode_first_approve_wins() {
        let decisions = vec![make_decision("u1", Decision::Approve)];
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::Any, 3, &decisions),
            Some(ApprovalStatus::Approved)
        );
    }

    #[test]
    fn any_mode_first_reject_wins() {
        let decisions = vec![make_decision("u1", Decision::Reject)];
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::Any, 3, &decisions),
            Some(ApprovalStatus::Rejected)
        );
    }

    #[test]
    fn any_mode_no_decisions() {
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::Any, 3, &[]),
            None
        );
    }

    #[test]
    fn all_mode_reject_ends() {
        let decisions = vec![
            make_decision("u1", Decision::Approve),
            make_decision("u2", Decision::Reject),
        ];
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::All, 3, &decisions),
            Some(ApprovalStatus::Rejected)
        );
    }

    #[test]
    fn all_mode_all_approve() {
        let decisions = vec![
            make_decision("u1", Decision::Approve),
            make_decision("u2", Decision::Approve),
            make_decision("u3", Decision::Approve),
        ];
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::All, 3, &decisions),
            Some(ApprovalStatus::Approved)
        );
    }

    #[test]
    fn all_mode_pending() {
        let decisions = vec![
            make_decision("u1", Decision::Approve),
        ];
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::All, 3, &decisions),
            None
        );
    }

    #[test]
    fn majority_approve_meets_threshold() {
        let decisions = vec![
            make_decision("u1", Decision::Approve),
            make_decision("u2", Decision::Approve),
            make_decision("u3", Decision::Approve),
        ];
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::Majority, 5, &decisions),
            Some(ApprovalStatus::Approved)
        );
    }

    #[test]
    fn majority_reject_meets_threshold() {
        let decisions = vec![
            make_decision("u1", Decision::Reject),
            make_decision("u2", Decision::Reject),
            make_decision("u3", Decision::Reject),
        ];
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::Majority, 5, &decisions),
            Some(ApprovalStatus::Rejected)
        );
    }

    #[test]
    fn majority_pending() {
        let decisions = vec![
            make_decision("u1", Decision::Approve),
            make_decision("u2", Decision::Reject),
        ];
        assert_eq!(
            evaluate_approval_mode(&ApprovalMode::Majority, 5, &decisions),
            None
        );
    }
}
