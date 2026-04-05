use crate::api::ApiSurface;
use crate::application::ApplicationContext;
use crate::config::{AppConfig, ConfigValidationReport};
use crate::infrastructure::Infrastructure;
use crate::web::WebSurface;

#[derive(Debug, Clone)]
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
            self.infrastructure.sqlite.database_path.display(),
            self.api.base_path,
            self.web.mount_path,
            self.web.asset_dir.display()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeBootstrapError {
    InvalidConfig(ConfigValidationReport),
}

pub fn bootstrap(config: AppConfig) -> Result<Runtime, RuntimeBootstrapError> {
    config
        .validate_startup()
        .map_err(RuntimeBootstrapError::InvalidConfig)?;

    let infrastructure = Infrastructure::from_config(&config.storage);

    Ok(Runtime {
        application: ApplicationContext::new(&config),
        api: ApiSurface::from_config(&config.api),
        web: WebSurface::from_config(&config.web),
        infrastructure,
        config,
    })
}

#[cfg(test)]
mod tests {
    use crate::config::{AppConfig, ConfigValidationIssue, ConfigValidationReport};

    use super::{RuntimeBootstrapError, bootstrap};

    #[test]
    fn bootstrap_assembles_runtime_layers() {
        let runtime = bootstrap(AppConfig::default()).expect("runtime should bootstrap");

        assert_eq!(runtime.api.base_path, "/api");
        assert_eq!(runtime.web.mount_path, "/");
        assert!(runtime.application.config.diagnostics.is_empty());
        assert_eq!(
            runtime.application.config.import.default_mode,
            runtime.config.import.default_mode
        );
        assert_eq!(
            runtime.application.config.export.default_profile,
            runtime.config.export.default_profile
        );
        assert_eq!(runtime.application.workers.file_io.limit(), 2);
        assert_eq!(runtime.application.workers.provider_requests.limit(), 2);
        assert_eq!(runtime.application.workers.db_writes.limit(), 1);
        assert_eq!(
            runtime.infrastructure.sqlite.database_path,
            std::path::PathBuf::from("discern.db")
        );
    }

    #[test]
    fn bootstrap_rejects_invalid_config() {
        let mut config = AppConfig::default();
        config.api.base_path = "api".to_string();

        assert!(matches!(
            bootstrap(config),
            Err(RuntimeBootstrapError::InvalidConfig(ConfigValidationReport {
                errors
            })) if errors
                == vec![ConfigValidationIssue {
                    field: "api.base_path".to_string(),
                    message: "path must start with '/'".to_string(),
                }]
        ));
    }
}
