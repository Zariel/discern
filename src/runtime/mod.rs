use crate::api::ApiSurface;
use crate::application::jobs::JobService;
use crate::application::{ApplicationContext, LogLevel};
use crate::config::{AppConfig, ConfigValidationReport};
use crate::domain::job::Job;
use crate::infrastructure::Infrastructure;
use crate::infrastructure::sqlite::{SqliteRepositories, SqliteRepositoryContext};
use crate::web::WebSurface;

#[derive(Debug, Clone)]
pub struct Runtime {
    pub config: AppConfig,
    pub application: ApplicationContext,
    pub infrastructure: Infrastructure,
    pub api: ApiSurface,
    pub web: WebSurface,
    pub startup_recovery: StartupRecoveryReport,
}

#[derive(Debug, Clone, Default)]
pub struct StartupRecoveryReport {
    pub recovered_jobs: Vec<Job>,
}

impl Runtime {
    pub fn startup_summary(&self) -> String {
        format!(
            "discern runtime ready: db={}, api={}, web={} ({}), recovered_jobs={}",
            self.infrastructure.sqlite.database_path.display(),
            self.api.base_path,
            self.web.mount_path,
            self.web.asset_dir.display(),
            self.startup_recovery.recovered_jobs.len(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeBootstrapError {
    InvalidConfig(ConfigValidationReport),
    Storage(String),
}

pub fn bootstrap(config: AppConfig) -> Result<Runtime, RuntimeBootstrapError> {
    let recovered_at_unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_secs() as i64;
    bootstrap_at(config, recovered_at_unix_seconds)
}

fn bootstrap_at(
    config: AppConfig,
    recovered_at_unix_seconds: i64,
) -> Result<Runtime, RuntimeBootstrapError> {
    config
        .validate_startup()
        .map_err(RuntimeBootstrapError::InvalidConfig)?;

    let application = ApplicationContext::new(&config);
    let infrastructure = Infrastructure::from_config_with_observability(
        &config.storage,
        &config.providers.musicbrainz,
        &config.providers.discogs,
        Some(application.observability.clone()),
    );
    let startup_recovery = recover_startup_jobs(
        &infrastructure.sqlite.database_path,
        recovered_at_unix_seconds,
        &application,
    )?;
    application.observability.emit(
        LogLevel::Info,
        "runtime_bootstrap_completed",
        [
            (
                "db_path",
                infrastructure.sqlite.database_path.display().to_string(),
            ),
            ("api_base_path", config.api.base_path.clone()),
            ("web_mount_path", config.web.mount_path.clone()),
            (
                "recovered_jobs",
                startup_recovery.recovered_jobs.len().to_string(),
            ),
        ],
    );

    Ok(Runtime {
        application,
        api: ApiSurface::from_config(&config.api),
        web: WebSurface::from_config_with_api(&config.web, &config.api),
        infrastructure,
        startup_recovery,
        config,
    })
}

fn recover_startup_jobs(
    database_path: &std::path::Path,
    recovered_at_unix_seconds: i64,
    application: &ApplicationContext,
) -> Result<StartupRecoveryReport, RuntimeBootstrapError> {
    let context = SqliteRepositoryContext::open(database_path.to_path_buf())
        .map_err(|error| RuntimeBootstrapError::Storage(error.message))?;
    context
        .ensure_schema()
        .map_err(|error| RuntimeBootstrapError::Storage(error.message))?;
    let repositories = SqliteRepositories::new(context);
    let recovered_jobs = JobService::new(repositories)
        .recover_unfinished_jobs(recovered_at_unix_seconds)
        .map_err(|error| RuntimeBootstrapError::Storage(error.message))?;
    application.observability.metrics.set_gauge(
        "startup_recovered_jobs",
        crate::application::observability::labels([]),
        recovered_jobs.len() as f64,
    );
    Ok(StartupRecoveryReport { recovered_jobs })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use crate::application::repository::{
        ImportBatchCommandRepository, JobCommandRepository, JobRepository,
        ReleaseInstanceCommandRepository, SourceCommandRepository,
    };
    use crate::config::{AppConfig, ConfigValidationIssue, ConfigValidationReport};
    use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
    use crate::domain::job::{Job, JobStatus, JobSubject, JobTrigger, JobType};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        ReleaseInstanceState, TechnicalVariant,
    };
    use crate::domain::source::{Source, SourceKind, SourceLocator};
    use crate::infrastructure::sqlite::{SqliteRepositories, SqliteRepositoryContext};
    use crate::support::ids::{ImportBatchId, ReleaseInstanceId, SourceId};

    use super::{RuntimeBootstrapError, bootstrap, bootstrap_at};

    #[test]
    fn bootstrap_assembles_runtime_layers() {
        let root = temp_root("runtime-bootstrap");
        let mut config = AppConfig::default();
        config.storage.sqlite_path = root.join("discern.db");

        let runtime = bootstrap(config.clone()).expect("runtime should bootstrap");

        assert_eq!(runtime.api.base_path, "/api");
        assert_eq!(runtime.web.mount_path, "/");
        assert_eq!(runtime.web.api_client.paths.jobs, "/api/jobs");
        assert_eq!(
            runtime.web.shell.default_route,
            crate::web::ShellRoute::IssueQueue
        );
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
            config.storage.sqlite_path
        );
        assert!(runtime.startup_recovery.recovered_jobs.is_empty());

        let _ = fs::remove_dir_all(root);
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

    #[test]
    fn bootstrap_recovers_running_jobs_in_sqlite() {
        let root = temp_root("runtime-recovery");
        let database_path = root.join("discern.db");
        let context =
            SqliteRepositoryContext::open(database_path.clone()).expect("context should open");
        context.ensure_schema().expect("schema should initialize");
        let repositories = SqliteRepositories::new(context);

        let source_id = SourceId::new();
        repositories
            .create_source(&Source {
                id: source_id.clone(),
                kind: SourceKind::ManualAdd,
                display_name: "Incoming".to_string(),
                locator: SourceLocator::ManualEntry {
                    submitted_path: root.join("incoming"),
                },
                external_reference: None,
            })
            .expect("source should persist");

        let batch = ImportBatch {
            id: ImportBatchId::new(),
            source_id: source_id.clone(),
            mode: ImportMode::Copy,
            status: ImportBatchStatus::Grouped,
            requested_by: BatchRequester::Operator {
                name: "operator".to_string(),
            },
            created_at_unix_seconds: 10,
            received_paths: vec![root.join("incoming")],
        };
        repositories
            .create_import_batch(&batch)
            .expect("batch should persist");

        let release_instance = ReleaseInstance {
            id: ReleaseInstanceId::new(),
            import_batch_id: batch.id.clone(),
            source_id,
            release_id: None,
            state: ReleaseInstanceState::Matched,
            technical_variant: TechnicalVariant {
                format_family: FormatFamily::Mp3,
                bitrate_mode: BitrateMode::Variable,
                bitrate_kbps: Some(320),
                sample_rate_hz: Some(44_100),
                bit_depth: None,
                track_count: 1,
                total_duration_seconds: 240,
            },
            provenance: ProvenanceSnapshot {
                ingest_origin: IngestOrigin::ManualAdd,
                original_source_path: root.join("incoming").display().to_string(),
                imported_at_unix_seconds: 10,
                gazelle_reference: None,
            },
        };
        repositories
            .create_release_instance(&release_instance)
            .expect("release instance should persist");

        let running_job = Job {
            id: crate::support::ids::JobId::new(),
            job_type: JobType::RenderExportMetadata,
            subject: JobSubject::ReleaseInstance(release_instance.id.clone()),
            status: JobStatus::Running,
            progress_phase: "rendering_export".to_string(),
            retry_count: 0,
            triggered_by: JobTrigger::System,
            created_at_unix_seconds: 10,
            started_at_unix_seconds: Some(11),
            finished_at_unix_seconds: None,
            error_payload: None,
        };
        repositories
            .create_job(&running_job)
            .expect("job should persist");

        let mut config = AppConfig::default();
        config.storage.sqlite_path = database_path.clone();

        let runtime = bootstrap_at(config, 500).expect("runtime should bootstrap with recovery");
        assert_eq!(runtime.startup_recovery.recovered_jobs.len(), 1);
        assert_eq!(
            runtime.startup_recovery.recovered_jobs[0].status,
            JobStatus::Resumable
        );

        let repositories = SqliteRepositories::new(
            SqliteRepositoryContext::open(database_path).expect("context should reopen"),
        );
        let stored_job = repositories
            .get_job(&running_job.id)
            .expect("job query should succeed")
            .expect("job should exist");
        assert_eq!(stored_job.status, JobStatus::Resumable);
        assert_eq!(
            stored_job.error_payload,
            Some("recovered during startup".to_string())
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(label: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("discern-runtime-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("temp root should exist");
        root
    }
}
