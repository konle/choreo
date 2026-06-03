use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub security: SecurityConfig,
    pub log: LogConfig,
    pub init: InitConfig,
    #[serde(default)]
    pub sweeper: SweeperConfig,
    #[serde(default)]
    pub notification: NotificationConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NotificationConfig {
    #[serde(default = "default_frontend_base_url")]
    pub frontend_base_url: String,
}

fn default_frontend_base_url() -> String {
    "http://localhost:5173".to_string()
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            frontend_base_url: default_frontend_base_url(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub mongo_url: String,
    pub redis_url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SecurityConfig {
    pub variable_encrypt_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LogConfig {
    pub level: String,
    pub format: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SweeperConfig {
    #[serde(default = "default_sweeper_enabled")]
    pub enabled: bool,
    #[serde(default = "default_sweeper_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_sweeper_max_recover")]
    pub max_recover_per_cycle: u32,
}

fn default_sweeper_enabled() -> bool {
    true
}
fn default_sweeper_interval() -> u64 {
    60
}
fn default_sweeper_max_recover() -> u32 {
    10
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            enabled: default_sweeper_enabled(),
            interval_secs: default_sweeper_interval(),
            max_recover_per_cycle: default_sweeper_max_recover(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct InitConfig {
    pub admin_username: String,
    pub admin_password: String,
    pub admin_email: String,
    pub default_tenant_name: String,
    pub default_tenant_description: String,
}

pub fn apply_env_overrides(
    config: &mut AppConfig,
    lookup: impl Fn(&str) -> Option<String>,
) {
    if let Some(v) = lookup("MONGO_URL") {
        config.database.mongo_url = v;
    }
    if let Some(v) = lookup("REDIS_URL") {
        config.database.redis_url = v;
    }
    if let Some(v) = lookup("API_PORT")
        && let Ok(port) = v.parse()
    {
        config.server.port = port;
    }
    if let Some(v) = lookup("VARIABLE_ENCRYPT_KEY") {
        config.security.variable_encrypt_key = v;
    }
    if let Some(v) = lookup("LOG_LEVEL") {
        config.log.level = v;
    }
}

impl AppConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(Path::new(path))
            .map_err(|e| anyhow::anyhow!("failed to read config file '{}': {}", path, e))?;
        let mut config: AppConfig = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse config file '{}': {}", path, e))?;

        apply_env_overrides(&mut config, |k| std::env::var(k).ok());

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> AppConfig {
        AppConfig {
            server: ServerConfig { port: 3000 },
            database: DatabaseConfig { mongo_url: "mongodb://localhost".into(), redis_url: "redis://localhost".into() },
            security: SecurityConfig { variable_encrypt_key: "default".into() },
            log: LogConfig { level: "info".into(), format: "json".into() },
            init: InitConfig {
                admin_username: "admin".into(), admin_password: "admin".into(),
                admin_email: "a@b.com".into(), default_tenant_name: "default".into(),
                default_tenant_description: "".into(),
            },
            sweeper: Default::default(),
            notification: Default::default(),
        }
    }

    #[test]
    fn apply_env_overrides_modifies_config() {
        let mut config = default_config();
        let overrides = [
            ("MONGO_URL", "mongodb://prod"),
            ("REDIS_URL", "redis://prod"),
            ("API_PORT", "8080"),
            ("VARIABLE_ENCRYPT_KEY", "prod-key"),
            ("LOG_LEVEL", "debug"),
        ];
        apply_env_overrides(&mut config, |k| {
            overrides.iter().find(|(ok, _)| *ok == k).map(|(_, v)| v.to_string())
        });
        assert_eq!(config.database.mongo_url, "mongodb://prod");
        assert_eq!(config.database.redis_url, "redis://prod");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.security.variable_encrypt_key, "prod-key");
        assert_eq!(config.log.level, "debug");
    }

    #[test]
    fn apply_env_overrides_no_vars_no_change() {
        let mut config = default_config();
        let port_before = config.server.port;
        apply_env_overrides(&mut config, |_| None);
        assert_eq!(config.server.port, port_before);
    }

    #[test]
    fn apply_env_overrides_invalid_port_ignored() {
        let mut config = default_config();
        apply_env_overrides(&mut config, |k| if k == "API_PORT" { Some("abc".into()) } else { None });
        assert_eq!(config.server.port, 3000);
    }
}
