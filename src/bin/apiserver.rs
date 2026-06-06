use clap::Parser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

use infrastructure::mongodb::apikey::apikey_repository_impl::ApiKeyRepositoryImpl;
use infrastructure::mongodb::approval::approval_repository_impl::ApprovalRepositoryImpl;
use infrastructure::mongodb::notification::notification_repository_impl::{
    NotificationRecordRepositoryImpl, NotificationSubscriptionRepositoryImpl,
};
use infrastructure::mongodb::task::task_repository_impl::{
    TaskInstanceRepositoryImpl, TaskRepositoryImpl,
};
use infrastructure::mongodb::tenant::tenant_repository_impl::TenantRepositoryImpl;
use infrastructure::mongodb::user::user_repository_impl::{
    UserRepositoryImpl, UserTenantRoleRepositoryImpl,
};
use infrastructure::mongodb::variable::variable_repository_impl::VariableRepositoryImpl;
use infrastructure::mongodb::workflow::workflow_repository_impl::{
    WorkflowDefinitionRepositoryImpl, WorkflowInstanceRepositoryImpl,
};
use infrastructure::queue::consumer;
use infrastructure::queue::dispatcher::ApalisDispatcher;

use application::auth::service::AuthService;
use application::auth::token::TokenService;
use application::usecase::approval::ApprovalUsecase;
use application::usecase::task::TaskUsecase;
use application::usecase::workflow::WorkflowUsecase;

use domain::apikey::service::ApiKeyService;
use domain::approval::service::ApprovalService;
use domain::notification::service::NotificationService;
use domain::task::service::{TaskInstanceService, TaskService};
use domain::tenant::service::TenantService;
use domain::user::entity::TenantRole;
use domain::user::service::UserService;
use domain::variable::service::VariableService;
use domain::workflow::service::{WorkflowDefinitionService, WorkflowInstanceService};

use http_handler::handler::apikey::ApiKeyHandler;
use http_handler::handler::approval::ApprovalHandler;
use http_handler::handler::auth::AuthHandler;
use http_handler::handler::notification::NotificationHandler;
use http_handler::handler::subscription::SubscriptionHandler;
use http_handler::handler::task::{TaskHandler, TaskInstanceHandler};
use http_handler::handler::tenant::TenantHandler;
use http_handler::handler::user::UserHandler;
use http_handler::handler::variable::VariableHandler;
use http_handler::handler::workflow::{WorkflowHandler, WorkflowInstanceHandler};
use http_handler::router::create_router;

use mcp_handler::server::{McpServer, create_mcp_service};

use workflow::config::AppConfig;

#[derive(Parser)]
#[command(name = "apiserver", about = "Workflow API Server")]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: String,

    #[arg(long, help = "Initialize default tenant and super admin")]
    init: bool,
}

async fn bootstrap_admin(
    init: &workflow::config::InitConfig,
    user_service: &UserService,
) -> domain::user::entity::UserEntity {
    match user_service.get_user_by_username(&init.admin_username).await {
        Ok(existing) => {
            info!(username = %init.admin_username, "super admin already exists, skipping");
            existing
        }
        Err(_) => {
            let password_hash = bcrypt::hash(&init.admin_password, bcrypt::DEFAULT_COST)
                .expect("failed to hash admin password");
            let user = user_service
                .create_user(
                    init.admin_username.clone(),
                    init.admin_email.clone(),
                    password_hash,
                    true,
                )
                .await
                .expect("failed to create super admin user");
            info!(username = %init.admin_username, user_id = %user.user_id, "created super admin");
            user
        }
    }
}

async fn bootstrap_tenant(
    init: &workflow::config::InitConfig,
    tenant_service: &TenantService,
) -> domain::tenant::entity::TenantEntity {
    match tenant_service.get_tenant(&init.default_tenant_name).await {
        Ok(existing) => {
            info!(tenant = %init.default_tenant_name, "tenant already exists, skipping");
            existing
        }
        Err(_) => {
            let t = tenant_service
                .create_tenant(
                    init.default_tenant_name.clone(),
                    init.default_tenant_description.clone(),
                )
                .await
                .expect("failed to create default tenant");
            info!(tenant = %init.default_tenant_name, tenant_id = %t.tenant_id, "created tenant");
            t
        }
    }
}

async fn bootstrap_role(
    admin: &domain::user::entity::UserEntity,
    tenant: &domain::tenant::entity::TenantEntity,
    init: &workflow::config::InitConfig,
    user_service: &UserService,
) {
    match user_service
        .get_role(&admin.user_id, &tenant.tenant_id)
        .await
    {
        Ok(_) => {
            info!(tenant_id = %tenant.tenant_id, "admin already has role, skipping");
        }
        Err(_) => {
            user_service
                .assign_role(&admin.user_id, &tenant.tenant_id, &TenantRole::TenantAdmin)
                .await
                .expect("failed to assign admin role");
            info!(username = %init.admin_username, tenant_id = %tenant.tenant_id, "assigned TenantAdmin role");
        }
    }
}

async fn bootstrap(config: &AppConfig, user_service: &UserService, tenant_service: &TenantService) {
    let init = &config.init;
    let admin = bootstrap_admin(init, user_service).await;
    let tenant = bootstrap_tenant(init, tenant_service).await;
    bootstrap_role(&admin, &tenant, init, user_service).await;
    info!(
        "bootstrap complete ─ username: {}, tenant: {} (id: {})",
        init.admin_username, init.default_tenant_name, tenant.tenant_id
    );
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = AppConfig::load(&cli.config).expect("failed to load config");

    workflow::init_tracing(&config.log);

    info!(config = %cli.config, "apiserver starting");

    let mongo_client = mongodb::Client::with_uri_str(&config.database.mongo_url)
        .await
        .unwrap_or_else(|e| {
            error!(url = %config.database.mongo_url, error = %e, "failed to connect to MongoDB");
            std::process::exit(1);
        });
    info!("connected to MongoDB");

    let task_storage = consumer::create_task_storage(&config.database.redis_url).await;
    let workflow_storage = consumer::create_workflow_storage(&config.database.redis_url).await;
    info!("connected to Redis");

    let dispatcher: Arc<dyn domain::shared::job::TaskDispatcher> =
        Arc::new(ApalisDispatcher::new(task_storage, workflow_storage));

    let db = mongo_client.database("workflow");
    infrastructure::mongodb::indexes::ensure_all_indexes(&db)
        .await
        .unwrap_or_else(|e| {
            error!(error = %e, "failed to ensure indexes");
        });
    info!("ensured MongoDB indexes");

    let task_repo = Arc::new(TaskRepositoryImpl::new(mongo_client.clone()));
    let task_instance_repo = Arc::new(TaskInstanceRepositoryImpl::new(mongo_client.clone()));
    let tenant_repo = Arc::new(TenantRepositoryImpl::new(mongo_client.clone()));
    let user_repo = Arc::new(UserRepositoryImpl::new(mongo_client.clone()));
    let role_repo = Arc::new(UserTenantRoleRepositoryImpl::new(mongo_client.clone()));
    let approval_repo = Arc::new(ApprovalRepositoryImpl::new(mongo_client.clone()));
    let apikey_repo = Arc::new(ApiKeyRepositoryImpl::new(mongo_client.clone()));
    let variable_repo = Arc::new(VariableRepositoryImpl::new(mongo_client.clone()));
    let workflow_def_repo = Arc::new(WorkflowDefinitionRepositoryImpl::new(mongo_client.clone()));
    let workflow_inst_repo = Arc::new(WorkflowInstanceRepositoryImpl::new(mongo_client.clone()));

    let task_service = TaskService::new(task_repo);
    let task_instance_service = Arc::new(TaskInstanceService::new(task_instance_repo));
    let tenant_service = TenantService::new(tenant_repo);
    let user_service = UserService::new(user_repo, role_repo.clone());
    let approval_service = ApprovalService::new(approval_repo, role_repo);
    let apikey_service = ApiKeyService::new(apikey_repo);
    let variable_service =
        VariableService::new(variable_repo, config.security.variable_encrypt_key.clone());
    let workflow_def_service = WorkflowDefinitionService::new(workflow_def_repo);
    let workflow_inst_service =
        WorkflowInstanceService::new(workflow_inst_repo, task_instance_service.clone());

    let notification_sub_repo =
        Arc::new(NotificationSubscriptionRepositoryImpl::new(mongo_client.clone()));
    notification_sub_repo.ensure_indexes().await.unwrap_or_else(|e| {
        error!(error = %e, "failed to ensure notification subscription indexes");
    });
    let notification_record_repo =
        Arc::new(NotificationRecordRepositoryImpl::new(mongo_client.clone()));
    notification_record_repo.ensure_indexes().await.unwrap_or_else(|e| {
        error!(error = %e, "failed to ensure notification record indexes");
    });
    let notification_service = NotificationService::new(
        notification_sub_repo,
        notification_record_repo,
        config.notification.frontend_base_url.clone(),
    );

    if cli.init {
        bootstrap(&config, &user_service, &tenant_service).await;
    }

    let auth_handler = Arc::new(AuthHandler::new(
        user_service.clone(),
        tenant_service.clone(),
    ));
    let tenant_handler = Arc::new(TenantHandler::new(tenant_service));
    let user_handler = Arc::new(UserHandler::new(user_service));
    let approval_handler = Arc::new(ApprovalHandler::new(approval_service.clone(), dispatcher.clone()));
    let apikey_handler = Arc::new(ApiKeyHandler::new(apikey_service));
    let variable_handler = Arc::new(VariableHandler::new(variable_service.clone()));
    let task_handler = Arc::new(TaskHandler::new(task_service.clone()));
    let task_instance_handler = Arc::new(TaskInstanceHandler::new(
        (*task_instance_service).clone(),
        task_service,
        variable_service,
        dispatcher.clone(),
    ));
    let workflow_handler = Arc::new(WorkflowHandler::new(workflow_def_service.clone()));
    let workflow_instance_handler = Arc::new(WorkflowInstanceHandler::new(
        workflow_def_service.clone(),
        workflow_inst_service.clone(),
        dispatcher.clone(),
    ));

    let notification_handler = Arc::new(NotificationHandler::new(notification_service.clone()));
    let subscription_handler =
        Arc::new(SubscriptionHandler::new(notification_service));

    let app = create_router(
        auth_handler,
        tenant_handler,
        user_handler,
        variable_handler,
        approval_handler,
        apikey_handler,
        task_handler,
        task_instance_handler,
        workflow_handler,
        workflow_instance_handler,
        notification_handler,
        subscription_handler,
    );

    // ── Application layer (shared by HTTP and MCP) ──
    let token_service = TokenService::new(TokenService::jwt_secret());
    let auth_service = Arc::new(AuthService::new(token_service));

    let workflow_usecase = Arc::new(WorkflowUsecase::new(
        workflow_def_service,
        workflow_inst_service,
        dispatcher.clone(),
        (*auth_service).clone(),
    ));
    let task_usecase = Arc::new(TaskUsecase::new(
        (*task_instance_service).clone(),
        (*auth_service).clone(),
    ));
    let approval_usecase = Arc::new(ApprovalUsecase::new(
        approval_service,
        (*auth_service).clone(),
    ));

    // ── MCP server (port = http_port + 1) ──
    let mcp_server = McpServer::new(
        auth_service,
        workflow_usecase,
        task_usecase,
        approval_usecase,
    );
    let mcp_service = create_mcp_service(mcp_server);
    let mcp_port = config.server.port + 1;
    let mcp_addr = format!("0.0.0.0:{}", mcp_port);
    let mcp_fallback = axum::Router::new().fallback_service(mcp_service);

    let http_addr = format!("0.0.0.0:{}", config.server.port);
    let http_listener = TcpListener::bind(&http_addr).await.unwrap_or_else(|e| {
        error!(addr = %http_addr, error = %e, "failed to bind HTTP");
        std::process::exit(1);
    });
    let mcp_listener = TcpListener::bind(&mcp_addr).await.unwrap_or_else(|e| {
        error!(addr = %mcp_addr, error = %e, "failed to bind MCP");
        std::process::exit(1);
    });

    info!(http_addr = %http_addr, mcp_addr = %mcp_addr, "apiserver ready");
    let http = axum::serve(http_listener, app);
    let mcp = axum::serve(mcp_listener, mcp_fallback);

    tokio::select! {
        result = http => {
            if let Err(e) = result {
                error!(error = %e, "HTTP server error");
            }
        }
        result = mcp => {
            if let Err(e) = result {
                error!(error = %e, "MCP server error");
            }
        }
    }
}
