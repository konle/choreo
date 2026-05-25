pub mod config;

use config::LogConfig;
use tracing_subscriber::{EnvFilter, fmt};

pub fn init_tracing(log: &LogConfig) {
    let filter = EnvFilter::try_new(&log.level).unwrap_or_else(|_| EnvFilter::new("info"));

    match log.format.as_str() {
        "json" => {
            fmt()
                .json()
                .with_env_filter(filter)
                .with_target(true)
                .with_thread_ids(false)
                .with_line_number(true)
                .init();
        }
        _ => {
            fmt()
                .with_env_filter(filter)
                .with_target(true)
                .with_thread_ids(false)
                .with_line_number(true)
                .init();
        }
    }
}
