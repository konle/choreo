use apalis::prelude::*;
use apalis_redis::RedisStorage;
use clap::Parser;
use domain::approval::service::ApprovalService;
use domain::notification::dispatcher::NotificationDispatcher;
use domain::notification::service::NotificationService;
use domain::plugin::manager::PluginManager;
use domain::shared::job::{ExecuteTaskJob, ExecuteWorkflowJob, WorkflowEvent};
use domain::sweeper::{Sweeper, SweeperConfig};
use domain::task::service::TaskInstanceService;
use domain::variable::service::VariableService;
use domain::workflow::entity::workflow_definition::NodeExecutionStatus;
use domain::workflow::service::{WorkflowDefinitionService, WorkflowInstanceService};
use infrastructure::queue::consumer;
use infrastructure::queue::dispatcher::{ApalisDispatcher, ApalisNotificationDispatcher};
use std::sync::Arc;
use tokio::time::Instant;
use tracing::{error, info, warn};
use workflow::config::AppConfig;

async fn handle_workflow_job(
    job: ExecuteWorkflowJob,
    manager: Data<Arc<PluginManager>>,
) -> Result<(), std::io::Error> {
    info!(
        workflow_instance_id = %job.workflow_instance_id,
        event = ?job.event,
        "processing workflow job"
    );

    let worker_id = "workflow-worker-1";
    if let Err(e) = manager.process_workflow_job(job.clone(), worker_id).await {
        error!(
            workflow_instance_id = %job.workflow_instance_id,
            error = %e,
            "workflow job failed"
        );
        return Err(std::io::Error::other(e));
    }

    Ok(())
}

use domain::task::executors::http::HttpTaskExecutor;
use domain::task::executors::llm::LlmTaskExecutor;
use domain::task::manager::TaskManager;

/// Build a workflow callback job from a task execution result.
/// Uses the unified `should_notify_parent_task` logic to determine events.
fn build_outbound_for_task(
    job: &ExecuteTaskJob,
    old_status: &domain::shared::workflow::TaskInstanceStatus,
    new_status: &domain::shared::workflow::TaskInstanceStatus,
    output: Option<serde_json::Value>,
    error_message: Option<String>,
    input: Option<serde_json::Value>,
) -> Option<ExecuteWorkflowJob> {
    use domain::task::entity::transition::{
        build_workflow_event_for_task, should_notify_parent_task,
    };

    let event_kind = should_notify_parent_task(old_status, new_status)?;
    let caller = job.caller_context.as_ref()?;

    let event = build_workflow_event_for_task(
        &event_kind,
        caller,
        &job.task_instance_id,
        output,
        error_message,
        input,
    );

    Some(ExecuteWorkflowJob {
        workflow_instance_id: caller.workflow_instance_id.clone(),
        tenant_id: job.tenant_id.clone(),
        event,
    })
}

fn exec_status_to_task_instance_status(
    status: &NodeExecutionStatus,
) -> domain::shared::workflow::TaskInstanceStatus {
    match status {
        NodeExecutionStatus::Success => domain::shared::workflow::TaskInstanceStatus::Completed,
        _ => domain::shared::workflow::TaskInstanceStatus::Failed,
    }
}

async fn handle_task_job(
    job: ExecuteTaskJob,
    ctx: Data<(Arc<PluginManager>, Arc<TaskManager>)>,
) -> Result<(), std::io::Error> {
    let manager = &ctx.0;
    let task_manager = &ctx.1;
    let task_svc = task_manager.task_instance_svc();
    info!(task_instance_id = %job.task_instance_id, "processing task job");
    let task_instance_entity = match task_svc.submit_instance(&job.task_instance_id).await {
        Ok(inst) => inst,
        Err(e) => {
            warn!(task_instance_id = %job.task_instance_id, error = %e,
                "task instance not claimable (already running or terminal), skipping");
            return Ok(());
        }
    };
    let start = Instant::now(); // 记录开始时间

    let old_task_status = domain::shared::workflow::TaskInstanceStatus::Running;

    let exec_result = match task_manager.execute_task(&task_instance_entity).await {
        Ok(r) => r,
        Err(e) => {
            error!(
                task_instance_id = %job.task_instance_id,
                task_type = ?task_instance_entity.task_type,
                error = %e,
                "task execution failed"
            );
            // CAS: running -> failed
            if let Err(cas_err) = task_svc
                .fail_with_error(
                    &job.task_instance_id,
                    e.to_string(),
                    Some(start.elapsed().as_millis() as u64),
                )
                .await
            {
                warn!(task_instance_id = %job.task_instance_id, error = %cas_err, "CAS fail_with_error failed (may already be terminal)");
            }

            let new_status = domain::shared::workflow::TaskInstanceStatus::Failed;
            if let Some(outbound) = build_outbound_for_task(
                &job,
                &old_task_status,
                &new_status,
                None,
                Some(e.to_string()),
                None,
            ) {
                if let Err(dispatch_err) = manager.dispatcher().dispatch_workflow(outbound).await {
                    error!(
                        task_instance_id = %job.task_instance_id,
                        error = %dispatch_err,
                        "failed to dispatch outbound event to parent"
                    );
                    return Err(std::io::Error::other(dispatch_err));
                }
            }

            return Ok(());
        }
    };
    let execution_duration = start.elapsed().as_millis() as u64;

    // Determine new task status
    let new_task_status = {
        let status = exec_status_to_task_instance_status(&exec_result.status);
        match status {
            domain::shared::workflow::TaskInstanceStatus::Completed => {
                if let Err(e) = task_svc
                    .complete_with_output(
                        &job.task_instance_id,
                        exec_result.output.clone(),
                        exec_result.input.clone(),
                        Some(execution_duration),
                    )
                    .await
                {
                    warn!(task_instance_id = %job.task_instance_id, error = %e, "CAS complete_with_output failed");
                }
                domain::shared::workflow::TaskInstanceStatus::Completed
            }
            _ => {
                let error_msg = exec_result.error_message.clone().unwrap_or_default();
                if let Err(e) = task_svc
                    .fail_with_error(&job.task_instance_id, error_msg, Some(execution_duration))
                    .await
                {
                    warn!(task_instance_id = %job.task_instance_id, error = %e, "CAS fail_with_error failed");
                }
                domain::shared::workflow::TaskInstanceStatus::Failed
            }
        }
    };

    info!(
        task_instance_id = %job.task_instance_id,
        status = ?exec_result.status,
        "task completed"
    );

    // Dispatch outbound event to parent workflow using unified transition logic
    if let Some(outbound) = build_outbound_for_task(
        &job,
        &old_task_status,
        &new_task_status,
        exec_result.output,
        exec_result.error_message,
        exec_result.input,
    ) {
        if let Err(e) = manager.dispatcher().dispatch_workflow(outbound).await {
            error!(
                task_instance_id = %job.task_instance_id,
                error = %e,
                "failed to dispatch outbound event to parent"
            );
            return Err(std::io::Error::other(e));
        }
    }

    Ok(())
}

async fn handle_notification_event(
    event: domain::notification::entity::NotificationEvent,
    service: Data<Arc<NotificationService>>,
) -> Result<(), std::io::Error> {
    use domain::notification::entity::NotificationChannel;

    if let Some(ref user_ids) = event.target_user_ids {
        for uid in user_ids {
            if let Err(e) = service
                .create_in_app_record(
                    &event.tenant_id,
                    uid,
                    &event.event_type,
                    &event.payload,
                    "force_push",
                    "",
                    event.workflow_meta_id.as_deref(),
                )
                .await
            {
                warn!(notification_error = %e, "failed to create forced notification record");
            }
        }
    } else {
        let recipients = match service
            .find_recipients_for_event(
                &event.tenant_id,
                &event.event_type,
                event.workflow_meta_id.as_deref(),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(notification_error = %e, "failed to find notification recipients");
                return Ok(());
            }
        };

        for (user_id, channels) in &recipients {
            for chan in channels {
                match chan {
                    NotificationChannel::InApp => {
                        if let Err(e) = service
                            .create_in_app_record(
                                &event.tenant_id,
                                user_id,
                                &event.event_type,
                                &event.payload,
                                "subscription",
                                "",
                                event.workflow_meta_id.as_deref(),
                            )
                            .await
                        {
                            warn!(notification_error = %e, "failed to create in-app notification record");
                        }
                    }
                    NotificationChannel::Webhook { .. } => {
                        // V1: Webhook delivery not yet implemented
                    }
                }
            }
        }
    }

    Ok(())
}

fn create_plugin_manager(
    workflow_definition_svc: WorkflowDefinitionService,
    workflow_instance_svc: Arc<WorkflowInstanceService>,
    task_instance_svc: Arc<TaskInstanceService>,
    variable_svc: VariableService,
    approval_svc: ApprovalService,
    task_storage: RedisStorage<ExecuteTaskJob>,
    workflow_storage: RedisStorage<ExecuteWorkflowJob>,
    notification_dispatcher: Arc<dyn NotificationDispatcher>,
) -> Arc<PluginManager> {
    let dispatcher = Arc::new(ApalisDispatcher::new(task_storage, workflow_storage));
    let mut manager = PluginManager::new(workflow_instance_svc.clone(), dispatcher)
        .with_task_instance_service(task_instance_svc)
        .with_variable_service(variable_svc)
        .with_notification_dispatcher(notification_dispatcher);
    manager.register(Box::new(domain::plugin::plugins::http::HttpPlugin::new()));
    manager.register(Box::new(
        domain::plugin::plugins::parallel::ParallelPlugin::new(),
    ));
    manager.register(Box::new(
        domain::plugin::plugins::ifcondition::IfConditionPlugin::new(),
    ));
    manager.register(Box::new(
        domain::plugin::plugins::contextrewrite::ContextRewritePlugin::new(),
    ));
    manager.register(Box::new(
        domain::plugin::plugins::forkjoin::ForkJoinPlugin::new(),
    ));
    manager.register(Box::new(
        domain::plugin::plugins::approval::ApprovalPlugin::new(approval_svc),
    ));
    manager.register(Box::new(
        domain::plugin::plugins::subworkflow::SubWorkflowPlugin::new(
            workflow_definition_svc,
            (*workflow_instance_svc).clone(),
        ),
    ));
    manager.register(Box::new(domain::plugin::plugins::pause::PausePlugin::new()));
    manager.register(Box::new(domain::plugin::plugins::llm::LlmPlugin::new()));
    Arc::new(manager)
}

fn create_task_manager(task_instance_svc: Arc<TaskInstanceService>) -> Arc<TaskManager> {
    let mut manager = TaskManager::new(task_instance_svc);
    manager.register(Box::new(HttpTaskExecutor::new()));
    manager.register(Box::new(LlmTaskExecutor::new()));
    Arc::new(manager)
}

#[derive(Parser)]
#[command(name = "engine", about = "Workflow Engine")]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = AppConfig::load(&cli.config).expect("failed to load config");

    workflow::init_tracing(&config.log);

    info!(config = %cli.config, "engine starting");

    let mongo_client = mongodb::Client::with_uri_str(&config.database.mongo_url)
        .await
        .unwrap_or_else(|e| {
            error!(url = %config.database.mongo_url, error = %e, "failed to connect to MongoDB");
            std::process::exit(1);
        });
    info!("connected to MongoDB");

    let workflow_def_repo = Arc::new(
        infrastructure::mongodb::workflow::workflow_repository_impl::WorkflowDefinitionRepositoryImpl::new(mongo_client.clone())
    );
    let workflow_definition_svc = WorkflowDefinitionService::new(workflow_def_repo);

    let task_repo = Arc::new(
        infrastructure::mongodb::task::task_repository_impl::TaskInstanceRepositoryImpl::new(
            mongo_client.clone(),
        ),
    );
    let task_svc = Arc::new(TaskInstanceService::new(task_repo));
    let task_manager = create_task_manager(task_svc.clone());

    let workflow_repo = Arc::new(
        infrastructure::mongodb::workflow::workflow_repository_impl::WorkflowInstanceRepositoryImpl::new(mongo_client.clone())
    );
    let workflow_instance_svc = Arc::new(WorkflowInstanceService::new(
        workflow_repo,
        task_svc.clone(),
    ));

    let variable_repo = Arc::new(
        infrastructure::mongodb::variable::variable_repository_impl::VariableRepositoryImpl::new(
            mongo_client.clone(),
        ),
    );
    let variable_svc =
        VariableService::new(variable_repo, config.security.variable_encrypt_key.clone());

    let role_repo = Arc::new(
        infrastructure::mongodb::user::user_repository_impl::UserTenantRoleRepositoryImpl::new(
            mongo_client.clone(),
        ),
    );
    let approval_repo = Arc::new(
        infrastructure::mongodb::approval::approval_repository_impl::ApprovalRepositoryImpl::new(
            mongo_client.clone(),
        ),
    );
    let approval_svc = ApprovalService::new(approval_repo, role_repo);

    let wf_storage = consumer::create_workflow_storage(&config.database.redis_url).await;
    let task_storage = consumer::create_task_storage(&config.database.redis_url).await;
    let notification_storage =
        consumer::create_notification_storage(&config.database.redis_url).await;
    info!("connected to Redis");

    let notification_dispatcher: Arc<dyn NotificationDispatcher> =
        Arc::new(ApalisNotificationDispatcher::new(notification_storage.clone()));

    let notification_sub_repo = Arc::new(
        infrastructure::mongodb::notification::notification_repository_impl::NotificationSubscriptionRepositoryImpl::new(mongo_client.clone()),
    );
    notification_sub_repo.ensure_indexes().await.unwrap_or_else(|e| {
        error!(error = %e, "failed to ensure notification subscription indexes");
    });
    let notification_record_repo = Arc::new(
        infrastructure::mongodb::notification::notification_repository_impl::NotificationRecordRepositoryImpl::new(mongo_client.clone()),
    );
    notification_record_repo.ensure_indexes().await.unwrap_or_else(|e| {
        error!(error = %e, "failed to ensure notification record indexes");
    });
    let notification_service = Arc::new(NotificationService::new(
        notification_sub_repo,
        notification_record_repo,
        config.notification.frontend_base_url.clone(),
    ));

    let plugin_manager = create_plugin_manager(
        workflow_definition_svc,
        workflow_instance_svc.clone(),
        task_svc.clone(),
        variable_svc,
        approval_svc.clone(),
        task_storage.clone(),
        wf_storage.clone(),
        notification_dispatcher.clone(),
    );

    let wf_worker = WorkerBuilder::new("workflow-worker")
        .data(plugin_manager.clone())
        .backend(wf_storage)
        .build_fn(handle_workflow_job);

    let task_worker = WorkerBuilder::new("task-worker")
        .data((plugin_manager.clone(), task_manager))
        .backend(task_storage)
        .build_fn(handle_task_job);

    if config.sweeper.enabled {
        let sweeper = Arc::new(
            Sweeper::new(
                workflow_instance_svc.clone(),
                task_svc.clone(),
                plugin_manager.dispatcher(),
                SweeperConfig {
                    interval_secs: config.sweeper.interval_secs,
                    max_recover_per_cycle: config.sweeper.max_recover_per_cycle,
                },
            )
            .with_approval_service(approval_svc.clone())
            .with_notification_dispatcher(notification_dispatcher.clone()),
        );
        let interval_secs = config.sweeper.interval_secs;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
            interval.tick().await; // skip first immediate tick
            loop {
                interval.tick().await;
                sweeper.run_cycle().await;
            }
        });
        info!(
            interval_secs = config.sweeper.interval_secs,
            max_recover = config.sweeper.max_recover_per_cycle,
            "sweeper started"
        );
    }

    info!("engine ready, waiting for jobs");

    let notification_worker = WorkerBuilder::new("notification-worker")
        .data(notification_service)
        .backend(notification_storage)
        .build_fn(handle_notification_event);

    Monitor::new()
        .register(wf_worker)
        .register(task_worker)
        .register(notification_worker)
        .run()
        .await
        .unwrap_or_else(|e| {
            error!(error = %e, "monitor failed");
        });
}
