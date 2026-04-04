use crate::api::ApiSurface;
use crate::application::ApplicationContext;
use crate::config::{AppConfig, ConfigError};
use crate::infrastructure::Infrastructure;
use crate::web::WebSurface;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Runtime {
    pub config: AppConfig,
    pub application: ApplicationContext,
    pub infrastructure: Infrastructure,
    pub api: ApiSurface,
    pub web: WebSurface,
}

impl Runtime {
    pub fn startup_summary(&self) -> String {
        format!(
            "discern runtime ready: db={}, api={}, web={} ({})",
            self.infrastructure.sqlite.database_path,
            self.api.base_path,
            self.web.mount_path,
            self.web.asset_dir
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeBootstrapError {
    InvalidConfig(ConfigError),
}

pub fn bootstrap(config: AppConfig) -> Result<Runtime, RuntimeBootstrapError> {
    config
        .validate()
        .map_err(RuntimeBootstrapError::InvalidConfig)?;

    let infrastructure = Infrastructure::from_config(&config.storage);

    Ok(Runtime {
        application: ApplicationContext::new(),
        api: ApiSurface::from_config(&config.api),
        web: WebSurface::from_config(&config.web),
        infrastructure,
        config,
    })
}

#[cfg(test)]
mod tests {
    use crate::config::{AppConfig, ConfigError};

    use super::{RuntimeBootstrapError, bootstrap};

    #[test]
    fn bootstrap_assembles_runtime_layers() {
        let runtime = bootstrap(AppConfig::default()).expect("runtime should bootstrap");

        assert_eq!(runtime.api.base_path, "/api");
        assert_eq!(runtime.web.mount_path, "/");
        assert_eq!(runtime.infrastructure.sqlite.database_path, "discern.db");
    }

    #[test]
    fn bootstrap_rejects_invalid_config() {
        let mut config = AppConfig::default();
        config.api.base_path = "api".to_string();

        assert_eq!(
            bootstrap(config),
            Err(RuntimeBootstrapError::InvalidConfig(ConfigError::new(
                "api.base_path",
                "path must start with '/'",
            )))
        );
    }
}
