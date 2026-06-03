use api::response::response::Response;
use axum::{
    Json, Router,
    routing::{get, post},
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info};

use workflow::config::AppConfig;

#[derive(Parser)]
#[command(name = "testserver", about = "Test Server")]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: String,
}
#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = AppConfig::load(&cli.config).expect("failed to load config");

    workflow::init_tracing(&config.log);

    info!(config = %cli.config, "testserver starting");

    let addr = "0.0.0.0:8081".to_string();
    let listener = TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        error!(addr = %addr, error = %e, "failed to bind");
        std::process::exit(1);
    });

    info!(addr = %addr, "apiserver ready");
    let app = create_app().await;
    axum::serve(listener, app).await.unwrap_or_else(|e| {
        error!(error = %e, "server error");
    });
}

async fn create_app() -> Router {
    Router::new()
        .route("/", get(root))
        .route("/user", post(create_user))
}

// 返回一个json: {"code": 0, "message": "success", "data": "Hello, World!"}
async fn root() -> Json<Response<String>> {
    Json(Response::success("Hello, World!".to_string()))
}

#[derive(Deserialize, Debug)]
struct CreateUserRequest {
    name: String,
    email: String,
    #[allow(dead_code)]
    password: String,
}

#[derive(Serialize)]
struct CreateUserResponse {
    id: String,
    name: String,
    email: String,
}

async fn create_user(Json(req): Json<CreateUserRequest>) -> Json<Response<CreateUserResponse>> {
    info!("create_user: {:?}", req);
    // 随机80% 失败
    // if rand::rng().random_bool(0.8) {
    //     error!("failed to create user");
    //     return Json(Response::error(400, "failed to create user".to_string()));
    // }
    error!("created user");
    Json(Response::success(CreateUserResponse {
        id: "123".to_string(),
        name: req.name.clone(),
        email: req.email.clone(),
    }))
}
