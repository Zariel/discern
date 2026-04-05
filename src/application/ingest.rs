use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::application::config::{ValidatedRuntimeConfig, WatchDirectoryPolicy};
use crate::application::repository::{
    ImportBatchCommandRepository, JobCommandRepository, RepositoryError, RepositoryErrorKind,
    SourceCommandRepository, SourceRepository,
};
use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus};
use crate::domain::job::{Job, JobSubject, JobTrigger, JobType};
use crate::domain::source::{Source, SourceKind, SourceLocator};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchDiscoveryError {
    pub kind: WatchDiscoveryErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchDiscoveryErrorKind {
    NotFound,
    Conflict,
    Storage,
    Io,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchDiscoveryReport {
    pub created_batches: Vec<ImportBatch>,
    pub queued_jobs: Vec<Job>,
    pub skipped_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestSubmissionReport {
    pub source: Source,
    pub batch: ImportBatch,
    pub job: Job,
}

pub struct WatchDiscoveryService<R> {
    repository: R,
    config: ValidatedRuntimeConfig,
}

impl<R> WatchDiscoveryService<R> {
    pub fn new(repository: R, config: ValidatedRuntimeConfig) -> Self {
        Self { repository, config }
    }
}

impl<R> WatchDiscoveryService<R>
where
    R: SourceRepository
        + SourceCommandRepository
        + ImportBatchCommandRepository
        + JobCommandRepository,
{
    pub fn submit_api_paths(
        &self,
        client_name: impl Into<String>,
        submitted_paths: Vec<PathBuf>,
        submitted_at_unix_seconds: i64,
    ) -> Result<IngestSubmissionReport, WatchDiscoveryError> {
        let client_name = client_name.into();
        let locator = SourceLocator::ApiClient {
            client_name: client_name.clone(),
        };
        let source = self.find_or_create_source(
            SourceKind::ApiClient,
            client_name.clone(),
            locator,
            Some(client_name.clone()),
        )?;

        let batch = self.create_submission_batch(
            source.clone(),
            self.config.import.default_mode.clone(),
            BatchRequester::ExternalClient { name: client_name },
            submitted_paths,
            submitted_at_unix_seconds,
        )?;
        let job =
            self.queue_discover_batch_job(&batch, JobTrigger::System, submitted_at_unix_seconds)?;

        Ok(IngestSubmissionReport { source, batch, job })
    }

    pub fn submit_manual_path(
        &self,
        operator_name: impl Into<String>,
        submitted_path: PathBuf,
        submitted_at_unix_seconds: i64,
    ) -> Result<IngestSubmissionReport, WatchDiscoveryError> {
        let operator_name = operator_name.into();
        let locator = SourceLocator::ManualEntry {
            submitted_path: submitted_path.clone(),
        };
        let source = self.find_or_create_source(
            SourceKind::ManualAdd,
            format!("manual:{operator_name}"),
            locator,
            None,
        )?;

        let batch = self.create_submission_batch(
            source.clone(),
            self.config.import.default_mode.clone(),
            BatchRequester::Operator {
                name: operator_name,
            },
            vec![submitted_path],
            submitted_at_unix_seconds,
        )?;
        let job =
            self.queue_discover_batch_job(&batch, JobTrigger::Operator, submitted_at_unix_seconds)?;

        Ok(IngestSubmissionReport { source, batch, job })
    }

    pub fn discover_watch_batches(
        &self,
        discovered_at_unix_seconds: i64,
    ) -> Result<WatchDiscoveryReport, WatchDiscoveryError> {
        let mut report = WatchDiscoveryReport::default();

        for watcher in &self.config.storage.watch_directories {
            let source = self.find_or_create_watch_source(watcher)?;
            let active_paths = self
                .repository
                .list_active_import_batches_for_source(&source.id)
                .map_err(map_repository_error)?
                .into_iter()
                .flat_map(|batch| {
                    batch
                        .received_paths
                        .into_iter()
                        .map(|path| normalize_path(&path))
                })
                .collect::<HashSet<_>>();

            for candidate_path in
                scan_watch_directory(watcher, &self.config.import.supported_formats)?
            {
                let normalized = normalize_path(&candidate_path);
                if active_paths.contains(&normalized) {
                    report.skipped_paths.push(candidate_path);
                    continue;
                }

                let batch = self.create_submission_batch(
                    source.clone(),
                    watcher
                        .import_mode_override
                        .clone()
                        .unwrap_or_else(|| self.config.import.default_mode.clone()),
                    BatchRequester::System,
                    vec![candidate_path.clone()],
                    discovered_at_unix_seconds,
                )?;
                let job = self.queue_discover_batch_job(
                    &batch,
                    JobTrigger::System,
                    discovered_at_unix_seconds,
                )?;

                report.created_batches.push(batch);
                report.queued_jobs.push(job);
            }
        }

        Ok(report)
    }

    fn find_or_create_watch_source(
        &self,
        watcher: &WatchDirectoryPolicy,
    ) -> Result<Source, WatchDiscoveryError> {
        self.find_or_create_source(
            SourceKind::WatchDirectory,
            watcher.name.clone(),
            SourceLocator::FilesystemPath(watcher.path.clone()),
            None,
        )
    }

    fn find_or_create_source(
        &self,
        kind: SourceKind,
        display_name: impl Into<String>,
        locator: SourceLocator,
        external_reference: Option<String>,
    ) -> Result<Source, WatchDiscoveryError> {
        if let Some(source) = self
            .repository
            .find_source_by_locator(&locator)
            .map_err(map_repository_error)?
        {
            return Ok(source);
        }

        let source = Source {
            id: crate::support::ids::SourceId::new(),
            kind,
            display_name: display_name.into(),
            locator,
            external_reference,
        };
        self.repository
            .create_source(&source)
            .map_err(map_repository_error)?;
        Ok(source)
    }

    fn create_submission_batch(
        &self,
        source: Source,
        mode: crate::domain::import_batch::ImportMode,
        requested_by: BatchRequester,
        submitted_paths: Vec<PathBuf>,
        created_at_unix_seconds: i64,
    ) -> Result<ImportBatch, WatchDiscoveryError> {
        let batch = ImportBatch {
            id: crate::support::ids::ImportBatchId::new(),
            source_id: source.id,
            mode,
            status: ImportBatchStatus::Created,
            requested_by,
            created_at_unix_seconds,
            received_paths: submitted_paths
                .into_iter()
                .map(|path| normalize_path(&path))
                .collect(),
        };
        self.repository
            .create_import_batch(&batch)
            .map_err(map_repository_error)?;
        Ok(batch)
    }

    fn queue_discover_batch_job(
        &self,
        batch: &ImportBatch,
        triggered_by: JobTrigger,
        created_at_unix_seconds: i64,
    ) -> Result<Job, WatchDiscoveryError> {
        let job = Job::queued(
            JobType::DiscoverBatch,
            JobSubject::ImportBatch(batch.id.clone()),
            triggered_by,
            created_at_unix_seconds,
        );
        self.repository
            .create_job(&job)
            .map_err(map_repository_error)?;
        Ok(job)
    }
}

fn scan_watch_directory(
    watcher: &WatchDirectoryPolicy,
    supported_formats: &[crate::domain::release_instance::FormatFamily],
) -> Result<Vec<PathBuf>, WatchDiscoveryError> {
    let mut candidates = Vec::new();
    let entries = fs::read_dir(&watcher.path).map_err(|error| WatchDiscoveryError {
        kind: if error.kind() == std::io::ErrorKind::NotFound {
            WatchDiscoveryErrorKind::NotFound
        } else {
            WatchDiscoveryErrorKind::Io
        },
        message: format!(
            "failed to read watch directory {}: {error}",
            watcher.path.display()
        ),
    })?;

    for entry in entries {
        let entry = entry.map_err(|error| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Io,
            message: format!(
                "failed to read watch entry under {}: {error}",
                watcher.path.display()
            ),
        })?;
        let path = entry.path();
        if is_hidden(&path) {
            continue;
        }

        let file_type = entry.file_type().map_err(|error| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Io,
            message: format!("failed to inspect {}: {error}", path.display()),
        })?;

        if file_type.is_dir() {
            if directory_contains_supported_audio(&path, supported_formats)? {
                candidates.push(path);
            }
            continue;
        }

        if file_type.is_file() && is_supported_audio_file(&path, supported_formats) {
            candidates.push(path);
        }
    }

    candidates.sort();
    Ok(candidates)
}

fn directory_contains_supported_audio(
    directory: &Path,
    supported_formats: &[crate::domain::release_instance::FormatFamily],
) -> Result<bool, WatchDiscoveryError> {
    let mut stack = vec![directory.to_path_buf()];
    while let Some(path) = stack.pop() {
        let entries = fs::read_dir(&path).map_err(|error| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Io,
            message: format!("failed to scan {}: {error}", path.display()),
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| WatchDiscoveryError {
                kind: WatchDiscoveryErrorKind::Io,
                message: format!("failed to scan {}: {error}", path.display()),
            })?;
            let candidate = entry.path();
            if is_hidden(&candidate) {
                continue;
            }
            let file_type = entry.file_type().map_err(|error| WatchDiscoveryError {
                kind: WatchDiscoveryErrorKind::Io,
                message: format!("failed to inspect {}: {error}", candidate.display()),
            })?;
            if file_type.is_dir() {
                stack.push(candidate);
            } else if file_type.is_file() && is_supported_audio_file(&candidate, supported_formats)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn is_supported_audio_file(
    path: &Path,
    supported_formats: &[crate::domain::release_instance::FormatFamily],
) -> bool {
    let extension = match path.extension().and_then(|value| value.to_str()) {
        Some(extension) => extension.to_ascii_lowercase(),
        None => return false,
    };
    supported_formats.iter().any(|format| match format {
        crate::domain::release_instance::FormatFamily::Flac => extension == "flac",
        crate::domain::release_instance::FormatFamily::Mp3 => extension == "mp3",
    })
}

fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| name.starts_with('.'))
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        normalized.push(component.as_os_str());
    }
    normalized
}

fn map_repository_error(error: RepositoryError) -> WatchDiscoveryError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => WatchDiscoveryErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            WatchDiscoveryErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => WatchDiscoveryErrorKind::Storage,
    };
    WatchDiscoveryError {
        kind,
        message: error.message,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::application::config::ValidatedRuntimeConfig;
    use crate::application::repository::{ImportBatchRepository, JobRepository, SourceRepository};
    use crate::domain::import_batch::ImportMode;
    use crate::domain::job::JobStatus;
    use crate::support::pagination::{Page, PageRequest};

    use super::*;

    #[test]
    fn service_discovers_supported_watch_inputs_and_enqueues_jobs() {
        let temp_root = test_root("discovers");
        fs::create_dir_all(temp_root.join("album")).expect("album directory should be created");
        fs::write(temp_root.join("album").join("01.flac"), b"flac")
            .expect("audio file should be created");
        fs::write(temp_root.join("single.mp3"), b"mp3").expect("single should be created");
        fs::create_dir_all(temp_root.join("artwork-only"))
            .expect("aux directory should be created");
        fs::write(temp_root.join("artwork-only").join("cover.jpg"), b"jpg")
            .expect("artwork file should be created");

        let mut config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        config.storage.watch_directories = vec![WatchDirectoryPolicy {
            name: "incoming".to_string(),
            path: temp_root.clone(),
            scan_mode: crate::config::WatchScanMode::EventDriven,
            import_mode_override: Some(ImportMode::Hardlink),
        }];

        let repository = InMemoryIngestRepository::default();
        let service = WatchDiscoveryService::new(repository.clone(), config);

        let report = service
            .discover_watch_batches(100)
            .expect("discovery should succeed");

        assert_eq!(report.created_batches.len(), 2);
        assert_eq!(report.queued_jobs.len(), 2);
        assert_eq!(report.created_batches[0].mode, ImportMode::Hardlink);
        assert!(
            report
                .created_batches
                .iter()
                .all(|batch| batch.status == ImportBatchStatus::Created)
        );
        assert!(
            report.queued_jobs.iter().all(
                |job| job.job_type == JobType::DiscoverBatch && job.status == JobStatus::Queued
            )
        );
        assert!(report.skipped_paths.is_empty());

        cleanup_root(temp_root);
    }

    #[test]
    fn service_skips_paths_with_active_batches_on_rescan() {
        let temp_root = test_root("rescans");
        fs::create_dir_all(temp_root.join("album")).expect("album directory should be created");
        fs::write(temp_root.join("album").join("01.flac"), b"flac")
            .expect("audio file should be created");

        let mut config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        config.storage.watch_directories = vec![WatchDirectoryPolicy {
            name: "incoming".to_string(),
            path: temp_root.clone(),
            scan_mode: crate::config::WatchScanMode::EventDriven,
            import_mode_override: None,
        }];

        let repository = InMemoryIngestRepository::default();
        let service = WatchDiscoveryService::new(repository.clone(), config);

        let first = service
            .discover_watch_batches(100)
            .expect("first discovery should succeed");
        let second = service
            .discover_watch_batches(101)
            .expect("rescan should succeed");

        assert_eq!(first.created_batches.len(), 1);
        assert!(second.created_batches.is_empty());
        assert_eq!(second.skipped_paths, vec![temp_root.join("album")]);

        cleanup_root(temp_root);
    }

    #[test]
    fn service_submits_api_paths_without_guessing_identity() {
        let repository = InMemoryIngestRepository::default();
        let config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        let service = WatchDiscoveryService::new(repository.clone(), config);

        let report = service
            .submit_api_paths(
                "lidarr",
                vec![
                    PathBuf::from("/imports/api/release-a"),
                    PathBuf::from("/imports/api/release-b"),
                ],
                200,
            )
            .expect("api intake should succeed");

        assert_eq!(report.source.kind, SourceKind::ApiClient);
        assert_eq!(
            report.source.locator,
            SourceLocator::ApiClient {
                client_name: "lidarr".to_string()
            }
        );
        assert_eq!(
            report.batch.requested_by,
            BatchRequester::ExternalClient {
                name: "lidarr".to_string()
            }
        );
        assert_eq!(report.batch.received_paths.len(), 2);
        assert_eq!(report.job.job_type, JobType::DiscoverBatch);
        assert_eq!(report.job.triggered_by, JobTrigger::System);
    }

    #[test]
    fn service_submits_manual_path_with_operator_source() {
        let repository = InMemoryIngestRepository::default();
        let config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        let service = WatchDiscoveryService::new(repository.clone(), config);

        let report = service
            .submit_manual_path("chris", PathBuf::from("/imports/manual/drop"), 300)
            .expect("manual intake should succeed");

        assert_eq!(report.source.kind, SourceKind::ManualAdd);
        assert_eq!(
            report.source.locator,
            SourceLocator::ManualEntry {
                submitted_path: PathBuf::from("/imports/manual/drop")
            }
        );
        assert_eq!(
            report.batch.requested_by,
            BatchRequester::Operator {
                name: "chris".to_string()
            }
        );
        assert_eq!(
            report.batch.received_paths,
            vec![PathBuf::from("/imports/manual/drop")]
        );
        assert_eq!(report.job.triggered_by, JobTrigger::Operator);
    }

    #[derive(Clone, Default)]
    struct InMemoryIngestRepository {
        sources: Arc<Mutex<HashMap<String, Source>>>,
        batches: Arc<Mutex<HashMap<String, ImportBatch>>>,
        jobs: Arc<Mutex<HashMap<String, Job>>>,
    }

    impl SourceRepository for InMemoryIngestRepository {
        fn find_source_by_locator(
            &self,
            locator: &SourceLocator,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(self
                .sources
                .lock()
                .expect("repository should lock")
                .values()
                .find(|source| &source.locator == locator)
                .cloned())
        }
    }

    impl SourceCommandRepository for InMemoryIngestRepository {
        fn create_source(&self, source: &Source) -> Result<(), RepositoryError> {
            self.sources
                .lock()
                .expect("repository should lock")
                .insert(source.id.as_uuid().to_string(), source.clone());
            Ok(())
        }
    }

    impl ImportBatchCommandRepository for InMemoryIngestRepository {
        fn create_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError> {
            self.batches
                .lock()
                .expect("repository should lock")
                .insert(batch.id.as_uuid().to_string(), batch.clone());
            Ok(())
        }

        fn list_active_import_batches_for_source(
            &self,
            source_id: &crate::support::ids::SourceId,
        ) -> Result<Vec<ImportBatch>, RepositoryError> {
            Ok(self
                .batches
                .lock()
                .expect("repository should lock")
                .values()
                .filter(|batch| {
                    batch.source_id == *source_id
                        && matches!(
                            batch.status,
                            ImportBatchStatus::Created
                                | ImportBatchStatus::Discovering
                                | ImportBatchStatus::Grouped
                                | ImportBatchStatus::Submitted
                        )
                })
                .cloned()
                .collect())
        }
    }

    impl ImportBatchRepository for InMemoryIngestRepository {
        fn get_import_batch(
            &self,
            id: &crate::support::ids::ImportBatchId,
        ) -> Result<Option<ImportBatch>, RepositoryError> {
            Ok(self
                .batches
                .lock()
                .expect("repository should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_import_batches(
            &self,
            _query: &crate::application::repository::ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }
    }

    impl JobRepository for InMemoryIngestRepository {
        fn get_job(&self, id: &crate::support::ids::JobId) -> Result<Option<Job>, RepositoryError> {
            Ok(self
                .jobs
                .lock()
                .expect("repository should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_jobs(
            &self,
            _query: &crate::application::repository::JobListQuery,
        ) -> Result<Page<Job>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }
    }

    impl JobCommandRepository for InMemoryIngestRepository {
        fn create_job(&self, job: &Job) -> Result<(), RepositoryError> {
            self.jobs
                .lock()
                .expect("repository should lock")
                .insert(job.id.as_uuid().to_string(), job.clone());
            Ok(())
        }

        fn update_job(&self, _job: &Job) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn list_recoverable_jobs(&self) -> Result<Vec<Job>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    fn test_root(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "discern-watch-discovery-{suffix}-{}",
            uuid::Uuid::new_v4()
        ))
    }

    fn cleanup_root(path: PathBuf) {
        let _ = fs::remove_dir_all(path);
    }
}
