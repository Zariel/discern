use std::fs::{self, OpenOptions};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::api::envelope::ApiEnvelope;
use crate::application::config::ValidatedRuntimeConfig;
use crate::application::observability::ObservabilityContext;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDiagnosticResource {
    pub field: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValidationResource {
    pub valid: bool,
    pub diagnostics: Vec<ConfigDiagnosticResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivenessResource {
    pub alive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessCheckResource {
    pub name: String,
    pub healthy: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessResource {
    pub ready: bool,
    pub checks: Vec<ReadinessCheckResource>,
}

pub struct DiagnosticsApi {
    config: ValidatedRuntimeConfig,
    observability: ObservabilityContext,
}

impl DiagnosticsApi {
    pub fn new(config: ValidatedRuntimeConfig, observability: ObservabilityContext) -> Self {
        Self {
            config,
            observability,
        }
    }

    pub fn get_config_validation(
        &self,
        request_id: impl Into<String>,
    ) -> ApiEnvelope<ConfigValidationResource> {
        let request_id = request_id.into();
        ApiEnvelope::success(
            ConfigValidationResource {
                valid: self.config.diagnostics.is_empty(),
                diagnostics: self
                    .config
                    .diagnostics
                    .issues
                    .iter()
                    .map(|diagnostic| ConfigDiagnosticResource {
                        field: diagnostic.field.clone(),
                        message: diagnostic.message.clone(),
                    })
                    .collect(),
            },
            request_id,
        )
    }

    pub fn get_liveness(&self, request_id: impl Into<String>) -> ApiEnvelope<LivenessResource> {
        ApiEnvelope::success(LivenessResource { alive: true }, request_id.into())
    }

    pub fn get_readiness(&self, request_id: impl Into<String>) -> ApiEnvelope<ReadinessResource> {
        let checks = collect_readiness_checks(&self.config);
        ApiEnvelope::success(
            ReadinessResource {
                ready: checks.iter().all(|check| check.healthy),
                checks,
            },
            request_id.into(),
        )
    }

    pub fn get_metrics(&self) -> String {
        self.observability.metrics.render_prometheus()
    }
}

fn collect_readiness_checks(config: &ValidatedRuntimeConfig) -> Vec<ReadinessCheckResource> {
    let mut checks = Vec::new();
    checks.push(check_sqlite_writable(&config.storage.sqlite_path));
    checks.push(check_sqlite_schema(&config.storage.sqlite_path));
    checks.push(check_directory_writable(
        "managed_library_root",
        &config.storage.managed_library_root,
    ));
    for watcher in &config.storage.watch_directories {
        checks.push(check_directory_readable(
            &format!("watch_directory:{}", watcher.name),
            &watcher.path,
        ));
    }
    checks
}

fn check_sqlite_writable(path: &Path) -> ReadinessCheckResource {
    match Connection::open(path) {
        Ok(connection) => match connection.execute_batch("BEGIN IMMEDIATE; ROLLBACK;") {
            Ok(()) => ReadinessCheckResource {
                name: "sqlite_writable".to_string(),
                healthy: true,
                detail: format!("opened writable database {}", path.display()),
            },
            Err(error) => ReadinessCheckResource {
                name: "sqlite_writable".to_string(),
                healthy: false,
                detail: format!(
                    "failed to acquire write lock for {}: {error}",
                    path.display()
                ),
            },
        },
        Err(error) => ReadinessCheckResource {
            name: "sqlite_writable".to_string(),
            healthy: false,
            detail: format!("failed to open {}: {error}", path.display()),
        },
    }
}

fn check_sqlite_schema(path: &Path) -> ReadinessCheckResource {
    match Connection::open(path) {
        Ok(connection) => match connection.query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM sqlite_master
                 WHERE type = 'table' AND name = 'jobs'
             )",
            [],
            |row| row.get::<_, i64>(0),
        ) {
            Ok(1) => ReadinessCheckResource {
                name: "sqlite_schema".to_string(),
                healthy: true,
                detail: "initial schema is available".to_string(),
            },
            Ok(_) => ReadinessCheckResource {
                name: "sqlite_schema".to_string(),
                healthy: false,
                detail: format!("jobs table is missing in {}", path.display()),
            },
            Err(error) => ReadinessCheckResource {
                name: "sqlite_schema".to_string(),
                healthy: false,
                detail: format!("failed to inspect sqlite schema: {error}"),
            },
        },
        Err(error) => ReadinessCheckResource {
            name: "sqlite_schema".to_string(),
            healthy: false,
            detail: format!("failed to open {}: {error}", path.display()),
        },
    }
}

fn check_directory_readable(name: &str, path: &Path) -> ReadinessCheckResource {
    if !path.is_dir() {
        return ReadinessCheckResource {
            name: name.to_string(),
            healthy: false,
            detail: format!("{} is missing or not a directory", path.display()),
        };
    }
    match fs::read_dir(path) {
        Ok(_) => ReadinessCheckResource {
            name: name.to_string(),
            healthy: true,
            detail: format!("{} is readable", path.display()),
        },
        Err(error) => ReadinessCheckResource {
            name: name.to_string(),
            healthy: false,
            detail: format!("failed to read {}: {error}", path.display()),
        },
    }
}

fn check_directory_writable(name: &str, path: &Path) -> ReadinessCheckResource {
    if !path.is_dir() {
        return ReadinessCheckResource {
            name: name.to_string(),
            healthy: false,
            detail: format!("{} is missing or not a directory", path.display()),
        };
    }

    let probe_path = path.join(format!(
        ".discern-readiness-{}-{}",
        std::process::id(),
        unix_timestamp_nanos()
    ));
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe_path)
    {
        Ok(file) => {
            drop(file);
            let _ = fs::remove_file(&probe_path);
            ReadinessCheckResource {
                name: name.to_string(),
                healthy: true,
                detail: format!("{} is writable", path.display()),
            }
        }
        Err(error) => ReadinessCheckResource {
            name: name.to_string(),
            healthy: false,
            detail: format!(
                "failed to write readiness probe in {}: {error}",
                path.display()
            ),
        },
    }
}

fn unix_timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use crate::application::config::{ConfigDiagnostic, ConfigDiagnostics, ValidatedRuntimeConfig};
    use crate::application::observability::ObservabilityContext;
    use crate::config::AppConfig;
    use crate::infrastructure::sqlite::SqliteRepositoryContext;

    use super::*;

    #[test]
    fn config_validation_returns_diagnostics() {
        let mut config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        config.diagnostics = ConfigDiagnostics {
            issues: vec![ConfigDiagnostic {
                field: "export.default_profile".to_string(),
                message: "profile was missing".to_string(),
            }],
        };

        let envelope = DiagnosticsApi::new(config, ObservabilityContext::default())
            .get_config_validation("req_config");

        let data = envelope.data.expect("data should exist");
        assert!(!data.valid);
        assert_eq!(data.diagnostics.len(), 1);
        assert_eq!(data.diagnostics[0].field, "export.default_profile");
    }

    #[test]
    fn liveness_reports_process_alive() {
        let api = DiagnosticsApi::new(
            ValidatedRuntimeConfig::from_validated_app_config(&AppConfig::default()),
            ObservabilityContext::default(),
        );

        let envelope = api.get_liveness("req_live");

        assert_eq!(
            envelope.data.expect("data should exist"),
            LivenessResource { alive: true }
        );
    }

    #[test]
    fn readiness_reports_storage_and_schema_health() {
        let root = temp_root("diagnostics-ready");
        let mut config = AppConfig::default();
        config.storage.sqlite_path = root.join("discern.db");
        config.storage.managed_library_root = root.join("library");
        config.storage.watch_directories[0].name = "incoming".to_string();
        config.storage.watch_directories[0].path = root.join("incoming");

        fs::create_dir_all(&config.storage.managed_library_root).expect("library should exist");
        fs::create_dir_all(&config.storage.watch_directories[0].path)
            .expect("watch directory should exist");
        let context = SqliteRepositoryContext::open(config.storage.sqlite_path.clone())
            .expect("sqlite should open");
        context.ensure_schema().expect("schema should initialize");

        let api = DiagnosticsApi::new(
            ValidatedRuntimeConfig::from_validated_app_config(&config),
            ObservabilityContext::default(),
        );
        let readiness = api
            .get_readiness("req_ready")
            .data
            .expect("data should exist");

        assert!(readiness.ready);
        assert!(readiness.checks.iter().all(|check| check.healthy));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_fails_when_watch_directory_is_missing() {
        let root = temp_root("diagnostics-not-ready");
        let mut config = AppConfig::default();
        config.storage.sqlite_path = root.join("discern.db");
        config.storage.managed_library_root = root.join("library");
        config.storage.watch_directories[0].name = "incoming".to_string();
        config.storage.watch_directories[0].path = root.join("missing-incoming");

        fs::create_dir_all(&config.storage.managed_library_root).expect("library should exist");
        let context = SqliteRepositoryContext::open(config.storage.sqlite_path.clone())
            .expect("sqlite should open");
        context.ensure_schema().expect("schema should initialize");

        let api = DiagnosticsApi::new(
            ValidatedRuntimeConfig::from_validated_app_config(&config),
            ObservabilityContext::default(),
        );
        let readiness = api
            .get_readiness("req_ready")
            .data
            .expect("data should exist");

        assert!(!readiness.ready);
        assert!(
            readiness
                .checks
                .iter()
                .any(|check| { check.name == "watch_directory:incoming" && !check.healthy })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn metrics_render_prometheus_text() {
        let observability = ObservabilityContext::default();
        observability.metrics.increment_counter(
            "jobs_total",
            crate::application::observability::labels([
                ("type", "verify_import"),
                ("status", "succeeded"),
            ]),
        );
        let api = DiagnosticsApi::new(
            ValidatedRuntimeConfig::from_validated_app_config(&AppConfig::default()),
            observability,
        );

        let metrics = api.get_metrics();

        assert!(metrics.contains("jobs_total"));
        assert!(metrics.contains("job_type=\"verify_import\""));
    }

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "discern-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("temp root should be created");
        root
    }
}
