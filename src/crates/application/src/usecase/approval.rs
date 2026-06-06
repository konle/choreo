use crate::auth::context::AuthContext;
use crate::auth::service::AuthService;
use domain::approval::entity::ApprovalInstanceEntity;
use domain::approval::entity::Decision;
use domain::approval::service::ApprovalService;
use domain::user::entity::Permission;

pub struct ApprovalUsecase {
    approval_service: ApprovalService,
    auth_service: AuthService,
}

impl ApprovalUsecase {
    pub fn new(approval_service: ApprovalService, auth_service: AuthService) -> Self {
        Self {
            approval_service,
            auth_service,
        }
    }

    pub async fn list_approvals(
        &self,
        auth: &AuthContext,
    ) -> Result<Vec<ApprovalInstanceEntity>, String> {
        self.auth_service.authorize(auth, &Permission::ReadOnly)?;
        self.approval_service
            .list_by_tenant(&auth.tenant_id)
            .await
            .map_err(|e| format!("failed to list approvals: {}", e))
    }

    pub async fn decide_approval(
        &self,
        auth: &AuthContext,
        approval_id: &str,
        decision: Decision,
        comment: Option<String>,
    ) -> Result<ApprovalInstanceEntity, String> {
        self.auth_service
            .authorize(auth, &Permission::ApprovalDecide)?;
        self.approval_service
            .decide(&auth.tenant_id, approval_id, &auth.user_id, decision, comment)
            .await
            .map_err(|e| format!("failed to decide approval: {}", e))
    }
}
