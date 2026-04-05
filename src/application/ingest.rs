use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use id3::TagLike;
use serde_json::json;
use serde_yaml::Value as YamlValue;

use crate::application::config::{ValidatedRuntimeConfig, WatchDirectoryPolicy};
use crate::application::repository::{
    ImportBatchCommandRepository, ImportBatchRepository, IngestEvidenceCommandRepository,
    JobCommandRepository, MetadataSnapshotCommandRepository, RepositoryError, RepositoryErrorKind,
    SourceCommandRepository, SourceRepository, StagingManifestCommandRepository,
};
use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus};
use crate::domain::ingest_evidence::{
    IngestEvidenceRecord, IngestEvidenceSource, IngestEvidenceSubject, ObservedValue,
    ObservedValueKind,
};
use crate::domain::job::{Job, JobSubject, JobTrigger, JobType};
use crate::domain::metadata_snapshot::{
    MetadataSnapshot, MetadataSnapshotSource, MetadataSubject, SnapshotFormat,
};
use crate::domain::source::{Source, SourceKind, SourceLocator};
use crate::domain::staging_manifest::{
    AuxiliaryFile, AuxiliaryFileRole, FileFingerprint, GroupingDecision, GroupingStrategy,
    ObservedTag, StagedFile, StagedReleaseGroup, StagingManifest, StagingManifestSource,
};
use crate::support::ids::ImportBatchId;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchAnalysisReport {
    pub batch: ImportBatch,
    pub manifest: StagingManifest,
    pub evidence_records: Vec<IngestEvidenceRecord>,
    pub metadata_snapshots: Vec<MetadataSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupedReleaseInputs {
    pub decision: GroupingDecision,
    pub auxiliary_files: Vec<AuxiliaryFile>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtractedIngestEvidence {
    pub records: Vec<IngestEvidenceRecord>,
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

fn build_staging_manifest(
    batch: &ImportBatch,
    source: &Source,
    grouped: &GroupedReleaseInputs,
    evidence_records: &[IngestEvidenceRecord],
    captured_at_unix_seconds: i64,
) -> Result<StagingManifest, WatchDiscoveryError> {
    let mut discovered_files = Vec::new();

    for group in &grouped.decision.groups {
        for file_path in &group.file_paths {
            discovered_files.push(build_staged_file(file_path, evidence_records)?);
        }
    }

    discovered_files.sort_by(|left, right| left.path.cmp(&right.path));

    Ok(StagingManifest {
        id: crate::support::ids::StagingManifestId::new(),
        batch_id: batch.id.clone(),
        source: StagingManifestSource {
            kind: source.kind.clone(),
            source_path: manifest_source_path(source, batch),
        },
        discovered_files,
        auxiliary_files: grouped.auxiliary_files.clone(),
        grouping: grouped.decision.clone(),
        captured_at_unix_seconds,
    })
}

fn build_staged_file(
    file_path: &Path,
    evidence_records: &[IngestEvidenceRecord],
) -> Result<StagedFile, WatchDiscoveryError> {
    let normalized_path = normalize_path(file_path);
    let evidence = evidence_records
        .iter()
        .find(|record| {
            matches!(
                &record.subject,
                IngestEvidenceSubject::DiscoveredPath(path) if path == &normalized_path
            )
        })
        .ok_or_else(|| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Conflict,
            message: format!(
                "missing embedded-tag evidence for {}",
                normalized_path.display()
            ),
        })?;
    let metadata = fs::metadata(file_path).map_err(|error| WatchDiscoveryError {
        kind: if error.kind() == std::io::ErrorKind::NotFound {
            WatchDiscoveryErrorKind::NotFound
        } else {
            WatchDiscoveryErrorKind::Io
        },
        message: format!("failed to inspect {}: {error}", file_path.display()),
    })?;

    Ok(StagedFile {
        path: normalized_path,
        fingerprint: FileFingerprint::LightweightFingerprint(format!(
            "{}:{}",
            metadata.len(),
            metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_secs())
                .unwrap_or_default()
        )),
        observed_tags: evidence
            .observations
            .iter()
            .map(|observation| ObservedTag {
                key: observed_value_kind_key(&observation.kind).to_string(),
                value: observation.value.clone(),
            })
            .collect(),
        duration_ms: evidence
            .observations
            .iter()
            .find(|observation| observation.kind == ObservedValueKind::DurationMs)
            .and_then(|observation| observation.value.parse().ok()),
        format_family: infer_format_family(file_path).ok_or_else(|| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Conflict,
            message: format!("unsupported audio format for {}", file_path.display()),
        })?,
    })
}

fn build_metadata_snapshots(
    batch_id: &ImportBatchId,
    evidence_records: &[IngestEvidenceRecord],
    captured_at_unix_seconds: i64,
) -> Result<Vec<MetadataSnapshot>, WatchDiscoveryError> {
    let mut snapshots = Vec::new();

    for record in evidence_records {
        let source = match ingest_evidence_source_to_metadata_source(&record.source) {
            Some(source) => source,
            None => continue,
        };
        let (format, payload) = match &record.structured_payload {
            Some(payload) => (
                snapshot_format_for_evidence_source(&record.source),
                payload.clone(),
            ),
            None => (
                SnapshotFormat::Json,
                serde_json::to_string(&json!({
                    "subject": format_ingest_evidence_subject(&record.subject),
                    "observations": record
                        .observations
                        .iter()
                        .map(|observation| {
                            json!({
                                "kind": observed_value_kind_key(&observation.kind),
                                "value": observation.value,
                            })
                        })
                        .collect::<Vec<_>>(),
                }))
                .map_err(|error| WatchDiscoveryError {
                    kind: WatchDiscoveryErrorKind::Conflict,
                    message: format!("failed to serialize metadata snapshot: {error}"),
                })?,
            ),
        };
        snapshots.push(MetadataSnapshot {
            id: crate::support::ids::MetadataSnapshotId::new(),
            subject: MetadataSubject::ImportBatch(batch_id.clone()),
            source,
            format,
            payload,
            captured_at_unix_seconds,
        });
    }

    Ok(snapshots)
}

fn manifest_source_path(source: &Source, batch: &ImportBatch) -> PathBuf {
    match &source.locator {
        SourceLocator::FilesystemPath(path) => normalize_path(path),
        SourceLocator::ManualEntry { submitted_path } => normalize_path(submitted_path),
        SourceLocator::ApiClient { .. } | SourceLocator::TrackerRef { .. } => batch
            .received_paths
            .first()
            .cloned()
            .unwrap_or_else(|| PathBuf::from(".")),
    }
}

fn ingest_evidence_source_to_metadata_source(
    source: &IngestEvidenceSource,
) -> Option<MetadataSnapshotSource> {
    match source {
        IngestEvidenceSource::EmbeddedTags => Some(MetadataSnapshotSource::EmbeddedTags),
        IngestEvidenceSource::FileName => Some(MetadataSnapshotSource::FileNameHeuristics),
        IngestEvidenceSource::DirectoryStructure => {
            Some(MetadataSnapshotSource::DirectoryStructure)
        }
        IngestEvidenceSource::GazelleYaml => Some(MetadataSnapshotSource::GazelleYaml),
        IngestEvidenceSource::AuxiliaryFile => None,
    }
}

fn snapshot_format_for_evidence_source(source: &IngestEvidenceSource) -> SnapshotFormat {
    match source {
        IngestEvidenceSource::GazelleYaml => SnapshotFormat::Yaml,
        _ => SnapshotFormat::Json,
    }
}

fn format_ingest_evidence_subject(subject: &IngestEvidenceSubject) -> serde_json::Value {
    match subject {
        IngestEvidenceSubject::DiscoveredPath(path) => json!({
            "kind": "discovered_path",
            "value": path.to_string_lossy(),
        }),
        IngestEvidenceSubject::GroupedReleaseInput { group_key } => json!({
            "kind": "grouped_release_input",
            "value": group_key,
        }),
    }
}

fn observed_value_kind_key(kind: &ObservedValueKind) -> &'static str {
    match kind {
        ObservedValueKind::Artist => "artist",
        ObservedValueKind::ReleaseTitle => "release_title",
        ObservedValueKind::ReleaseYear => "release_year",
        ObservedValueKind::TrackTitle => "track_title",
        ObservedValueKind::TrackNumber => "track_number",
        ObservedValueKind::DiscNumber => "disc_number",
        ObservedValueKind::DurationMs => "duration_ms",
        ObservedValueKind::FormatFamily => "format_family",
        ObservedValueKind::Label => "label",
        ObservedValueKind::CatalogNumber => "catalog_number",
        ObservedValueKind::MediaDescriptor => "media_descriptor",
        ObservedValueKind::SourceDescriptor => "source_descriptor",
        ObservedValueKind::TrackerIdentifier => "tracker_identifier",
    }
}

impl<R> WatchDiscoveryService<R>
where
    R: SourceRepository
        + ImportBatchRepository
        + ImportBatchCommandRepository
        + StagingManifestCommandRepository
        + IngestEvidenceCommandRepository
        + MetadataSnapshotCommandRepository,
{
    pub fn analyze_import_batch(
        &self,
        batch_id: &ImportBatchId,
        captured_at_unix_seconds: i64,
    ) -> Result<BatchAnalysisReport, WatchDiscoveryError> {
        let batch = self
            .repository
            .get_import_batch(batch_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| WatchDiscoveryError {
                kind: WatchDiscoveryErrorKind::NotFound,
                message: format!("import batch {} was not found", batch_id.as_uuid()),
            })?;
        let source = self
            .repository
            .get_source(&batch.source_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| WatchDiscoveryError {
                kind: WatchDiscoveryErrorKind::NotFound,
                message: format!("source {} was not found", batch.source_id.as_uuid()),
            })?;
        let grouped =
            group_release_inputs(&batch.received_paths, &self.config.import.supported_formats)?;
        let evidence = extract_group_evidence(batch_id, &grouped, captured_at_unix_seconds)?;
        let manifest = build_staging_manifest(
            &batch,
            &source,
            &grouped,
            &evidence.records,
            captured_at_unix_seconds,
        )?;
        let metadata_snapshots =
            build_metadata_snapshots(batch_id, &evidence.records, captured_at_unix_seconds)?;

        self.repository
            .create_staging_manifest(&manifest)
            .map_err(map_repository_error)?;
        self.repository
            .create_ingest_evidence_records(&evidence.records)
            .map_err(map_repository_error)?;
        self.repository
            .create_metadata_snapshots(&metadata_snapshots)
            .map_err(map_repository_error)?;

        let mut updated_batch = batch.clone();
        updated_batch.status = ImportBatchStatus::Grouped;
        self.repository
            .update_import_batch(&updated_batch)
            .map_err(map_repository_error)?;

        Ok(BatchAnalysisReport {
            batch: updated_batch,
            manifest,
            evidence_records: evidence.records,
            metadata_snapshots,
        })
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

pub fn group_release_inputs(
    submitted_paths: &[PathBuf],
    supported_formats: &[crate::domain::release_instance::FormatFamily],
) -> Result<GroupedReleaseInputs, WatchDiscoveryError> {
    let mut groups = Vec::new();
    let mut auxiliary_files = Vec::new();
    let mut loose_files_by_parent = std::collections::BTreeMap::<PathBuf, Vec<PathBuf>>::new();
    let mut notes = Vec::new();

    for submitted_path in submitted_paths {
        let metadata = fs::metadata(submitted_path).map_err(|error| WatchDiscoveryError {
            kind: if error.kind() == std::io::ErrorKind::NotFound {
                WatchDiscoveryErrorKind::NotFound
            } else {
                WatchDiscoveryErrorKind::Io
            },
            message: format!("failed to inspect {}: {error}", submitted_path.display()),
        })?;

        if metadata.is_dir() {
            let (audio_files, aux_files) =
                collect_directory_contents(submitted_path, supported_formats)?;
            auxiliary_files.extend(aux_files.clone());
            if !audio_files.is_empty() {
                groups.push(StagedReleaseGroup {
                    key: group_key_for_path(submitted_path),
                    file_paths: audio_files,
                    auxiliary_paths: aux_files.into_iter().map(|file| file.path).collect(),
                });
            }
        } else if metadata.is_file() && is_supported_audio_file(submitted_path, supported_formats) {
            let parent = submitted_path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."));
            loose_files_by_parent
                .entry(parent)
                .or_default()
                .push(normalize_path(submitted_path));
        }
    }

    let mut loose_groups = Vec::new();
    for (parent, mut files) in loose_files_by_parent {
        files.sort();
        if files.len() == 1 || track_numbers_are_contiguous(&files) {
            let aux_files = collect_nearby_auxiliary_files(&parent)?;
            auxiliary_files.extend(aux_files.clone());
            loose_groups.push(StagedReleaseGroup {
                key: group_key_for_path(&parent),
                file_paths: files,
                auxiliary_paths: aux_files.into_iter().map(|file| file.path).collect(),
            });
        } else {
            notes.push(format!(
                "split ambiguous loose files under {} into one-file groups",
                parent.display()
            ));
            for file in files {
                loose_groups.push(StagedReleaseGroup {
                    key: group_key_for_path(&file),
                    file_paths: vec![file],
                    auxiliary_paths: Vec::new(),
                });
            }
        }
    }

    let strategy = if !groups.is_empty() {
        GroupingStrategy::CommonParentDirectory
    } else {
        GroupingStrategy::TrackNumberContinuity
    };
    groups.extend(loose_groups);

    Ok(GroupedReleaseInputs {
        decision: GroupingDecision {
            strategy,
            groups,
            notes,
        },
        auxiliary_files,
    })
}

pub fn extract_group_evidence(
    batch_id: &ImportBatchId,
    grouped: &GroupedReleaseInputs,
    captured_at_unix_seconds: i64,
) -> Result<ExtractedIngestEvidence, WatchDiscoveryError> {
    let mut records = Vec::new();

    for group in &grouped.decision.groups {
        for file_path in &group.file_paths {
            records.push(extract_audio_file_evidence(
                batch_id,
                file_path,
                captured_at_unix_seconds,
            )?);
        }

        for auxiliary_path in &group.auxiliary_paths {
            if classify_auxiliary_file(auxiliary_path) == Some(AuxiliaryFileRole::GazelleYaml) {
                records.push(parse_gazelle_yaml_evidence(
                    batch_id,
                    &group.key,
                    auxiliary_path,
                    captured_at_unix_seconds,
                )?);
            }
        }
    }

    Ok(ExtractedIngestEvidence { records })
}

fn extract_audio_file_evidence(
    batch_id: &ImportBatchId,
    file_path: &Path,
    captured_at_unix_seconds: i64,
) -> Result<IngestEvidenceRecord, WatchDiscoveryError> {
    let format = infer_format_family(file_path).ok_or_else(|| WatchDiscoveryError {
        kind: WatchDiscoveryErrorKind::Conflict,
        message: format!("unsupported audio format for {}", file_path.display()),
    })?;

    let mut observations = vec![ObservedValue::format_family(format.clone())];
    match format {
        crate::domain::release_instance::FormatFamily::Mp3 => {
            let tag = id3::Tag::read_from_path(file_path).map_err(|error| WatchDiscoveryError {
                kind: WatchDiscoveryErrorKind::Io,
                message: format!(
                    "failed to read MP3 tags from {}: {error}",
                    file_path.display()
                ),
            })?;
            observations.extend(observations_from_id3_tag(&tag));
        }
        crate::domain::release_instance::FormatFamily::Flac => {
            let tag =
                metaflac::Tag::read_from_path(file_path).map_err(|error| WatchDiscoveryError {
                    kind: WatchDiscoveryErrorKind::Io,
                    message: format!(
                        "failed to read FLAC tags from {}: {error}",
                        file_path.display()
                    ),
                })?;
            if let Some(vorbis) = tag.vorbis_comments() {
                observations.extend(observations_from_vorbis_comments(
                    vorbis.comments.iter().flat_map(|(key, values)| {
                        values
                            .iter()
                            .cloned()
                            .map(|value| (key.clone(), value))
                            .collect::<Vec<_>>()
                    }),
                ));
            }
        }
    }

    Ok(IngestEvidenceRecord {
        id: crate::support::ids::IngestEvidenceId::new(),
        batch_id: batch_id.clone(),
        subject: IngestEvidenceSubject::DiscoveredPath(normalize_path(file_path)),
        source: IngestEvidenceSource::EmbeddedTags,
        observations,
        structured_payload: None,
        captured_at_unix_seconds,
    })
}

fn parse_gazelle_yaml_evidence(
    batch_id: &ImportBatchId,
    group_key: &str,
    yaml_path: &Path,
    captured_at_unix_seconds: i64,
) -> Result<IngestEvidenceRecord, WatchDiscoveryError> {
    let payload = fs::read_to_string(yaml_path).map_err(|error| WatchDiscoveryError {
        kind: WatchDiscoveryErrorKind::Io,
        message: format!("failed to read YAML from {}: {error}", yaml_path.display()),
    })?;
    let document: YamlValue =
        serde_yaml::from_str(&payload).map_err(|error| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Conflict,
            message: format!("failed to parse YAML from {}: {error}", yaml_path.display()),
        })?;

    let mut observations = Vec::new();
    for (field, kind) in [
        ("release_name", ObservedValueKind::ReleaseTitle),
        ("artist", ObservedValueKind::Artist),
        ("year", ObservedValueKind::ReleaseYear),
        ("label", ObservedValueKind::Label),
        ("catalog_number", ObservedValueKind::CatalogNumber),
        ("catalog", ObservedValueKind::CatalogNumber),
        ("media", ObservedValueKind::MediaDescriptor),
        ("source", ObservedValueKind::SourceDescriptor),
        ("format", ObservedValueKind::FormatFamily),
        ("encoding", ObservedValueKind::FormatFamily),
        ("scene", ObservedValueKind::TrackerIdentifier),
        ("tracker_id", ObservedValueKind::TrackerIdentifier),
        ("torrent_id", ObservedValueKind::TrackerIdentifier),
    ] {
        if let Some(value) = yaml_string_field(&document, field) {
            observations.push(ObservedValue {
                kind: kind.clone(),
                value,
            });
        }
    }

    Ok(IngestEvidenceRecord {
        id: crate::support::ids::IngestEvidenceId::new(),
        batch_id: batch_id.clone(),
        subject: IngestEvidenceSubject::GroupedReleaseInput {
            group_key: group_key.to_string(),
        },
        source: IngestEvidenceSource::GazelleYaml,
        observations,
        structured_payload: Some(payload),
        captured_at_unix_seconds,
    })
}

fn observations_from_id3_tag(tag: &id3::Tag) -> Vec<ObservedValue> {
    let mut observations = Vec::new();
    push_optional_observation(
        &mut observations,
        ObservedValueKind::Artist,
        tag.artist().map(str::to_string),
    );
    push_optional_observation(
        &mut observations,
        ObservedValueKind::ReleaseTitle,
        tag.album().map(str::to_string),
    );
    push_optional_observation(
        &mut observations,
        ObservedValueKind::TrackTitle,
        tag.title().map(str::to_string),
    );
    push_optional_observation(
        &mut observations,
        ObservedValueKind::TrackNumber,
        tag.track().map(|value| value.to_string()),
    );
    push_optional_observation(
        &mut observations,
        ObservedValueKind::DiscNumber,
        tag.disc().map(|value| value.to_string()),
    );
    push_optional_observation(
        &mut observations,
        ObservedValueKind::ReleaseYear,
        tag.year().map(|value| value.to_string()),
    );
    observations
}

fn observations_from_vorbis_comments(
    comments: impl IntoIterator<Item = (String, String)>,
) -> Vec<ObservedValue> {
    let mut observations = Vec::new();
    for (key, value) in comments {
        let normalized_key = key.to_ascii_lowercase();
        let kind = match normalized_key.as_str() {
            "artist" | "albumartist" => Some(ObservedValueKind::Artist),
            "album" => Some(ObservedValueKind::ReleaseTitle),
            "title" => Some(ObservedValueKind::TrackTitle),
            "tracknumber" => Some(ObservedValueKind::TrackNumber),
            "discnumber" => Some(ObservedValueKind::DiscNumber),
            "date" | "year" => Some(ObservedValueKind::ReleaseYear),
            "label" | "organization" | "publisher" => Some(ObservedValueKind::Label),
            "catalog_number" | "catalognumber" => Some(ObservedValueKind::CatalogNumber),
            _ => None,
        };
        if let Some(kind) = kind {
            observations.push(ObservedValue { kind, value });
        }
    }
    observations
}

fn infer_format_family(path: &Path) -> Option<crate::domain::release_instance::FormatFamily> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    match extension.as_str() {
        "flac" => Some(crate::domain::release_instance::FormatFamily::Flac),
        "mp3" => Some(crate::domain::release_instance::FormatFamily::Mp3),
        _ => None,
    }
}

fn yaml_string_field(document: &YamlValue, field: &str) -> Option<String> {
    match document {
        YamlValue::Mapping(map) => {
            map.get(YamlValue::String(field.to_string()))
                .and_then(|value| match value {
                    YamlValue::String(text) => Some(text.clone()),
                    YamlValue::Number(number) => Some(number.to_string()),
                    YamlValue::Bool(boolean) => Some(boolean.to_string()),
                    _ => None,
                })
        }
        _ => None,
    }
}

fn push_optional_observation(
    observations: &mut Vec<ObservedValue>,
    kind: ObservedValueKind,
    value: impl Into<Option<String>>,
) {
    if let Some(value) = value.into() {
        observations.push(ObservedValue { kind, value });
    }
}

fn collect_directory_contents(
    directory: &Path,
    supported_formats: &[crate::domain::release_instance::FormatFamily],
) -> Result<(Vec<PathBuf>, Vec<AuxiliaryFile>), WatchDiscoveryError> {
    let mut audio_files = Vec::new();
    let mut auxiliary_files = Vec::new();
    let entries = fs::read_dir(directory).map_err(|error| WatchDiscoveryError {
        kind: WatchDiscoveryErrorKind::Io,
        message: format!("failed to scan {}: {error}", directory.display()),
    })?;

    for entry in entries {
        let entry = entry.map_err(|error| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Io,
            message: format!("failed to scan {}: {error}", directory.display()),
        })?;
        let path = entry.path();
        if is_hidden(&path) {
            continue;
        }

        let file_type = entry.file_type().map_err(|error| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Io,
            message: format!("failed to inspect {}: {error}", path.display()),
        })?;
        if file_type.is_file() {
            if is_supported_audio_file(&path, supported_formats) {
                audio_files.push(normalize_path(&path));
            } else if let Some(role) = classify_auxiliary_file(&path) {
                auxiliary_files.push(AuxiliaryFile {
                    path: normalize_path(&path),
                    role,
                });
            }
        }
    }

    audio_files.sort();
    auxiliary_files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((audio_files, auxiliary_files))
}

fn collect_nearby_auxiliary_files(
    directory: &Path,
) -> Result<Vec<AuxiliaryFile>, WatchDiscoveryError> {
    let entries = fs::read_dir(directory).map_err(|error| WatchDiscoveryError {
        kind: WatchDiscoveryErrorKind::Io,
        message: format!("failed to scan {}: {error}", directory.display()),
    })?;
    let mut auxiliary_files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Io,
            message: format!("failed to scan {}: {error}", directory.display()),
        })?;
        let path = entry.path();
        if is_hidden(&path) {
            continue;
        }
        let file_type = entry.file_type().map_err(|error| WatchDiscoveryError {
            kind: WatchDiscoveryErrorKind::Io,
            message: format!("failed to inspect {}: {error}", path.display()),
        })?;
        if file_type.is_file()
            && let Some(role) = classify_auxiliary_file(&path)
        {
            auxiliary_files.push(AuxiliaryFile {
                path: normalize_path(&path),
                role,
            });
        }
    }
    auxiliary_files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(auxiliary_files)
}

fn classify_auxiliary_file(path: &Path) -> Option<AuxiliaryFileRole> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())?
        .to_ascii_lowercase();
    match extension.as_str() {
        "yaml" | "yml" => Some(AuxiliaryFileRole::GazelleYaml),
        "jpg" | "jpeg" | "png" => Some(AuxiliaryFileRole::Artwork),
        "cue" => Some(AuxiliaryFileRole::CueSheet),
        "log" => Some(AuxiliaryFileRole::Log),
        _ => None,
    }
}

fn track_numbers_are_contiguous(files: &[PathBuf]) -> bool {
    let mut numbers = files
        .iter()
        .filter_map(|path| {
            path.file_stem()
                .and_then(|value| value.to_str())
                .and_then(parse_leading_track_number)
        })
        .collect::<Vec<_>>();
    if numbers.len() != files.len() {
        return false;
    }
    numbers.sort_unstable();
    numbers
        .windows(2)
        .all(|window| matches!(window, [left, right] if *right == *left + 1))
}

fn parse_leading_track_number(stem: &str) -> Option<u32> {
    let digits = stem
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn group_key_for_path(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("group")
        .to_ascii_lowercase()
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

    #[test]
    fn grouping_uses_directory_boundaries_when_album_folders_are_clear() {
        let temp_root = test_root("group-dirs");
        fs::create_dir_all(temp_root.join("album-a")).expect("album a should be created");
        fs::create_dir_all(temp_root.join("album-b")).expect("album b should be created");
        fs::write(temp_root.join("album-a").join("01.flac"), b"flac").expect("track should exist");
        fs::write(temp_root.join("album-a").join("cover.jpg"), b"jpg").expect("cover should exist");
        fs::write(temp_root.join("album-b").join("01.mp3"), b"mp3").expect("track should exist");
        fs::write(temp_root.join("album-b").join("release.yaml"), b"yaml")
            .expect("yaml should exist");

        let grouped = group_release_inputs(
            &[temp_root.join("album-a"), temp_root.join("album-b")],
            &crate::config::AppConfig::default().import.supported_formats,
        )
        .expect("grouping should succeed");

        assert_eq!(
            grouped.decision.strategy,
            GroupingStrategy::CommonParentDirectory
        );
        assert_eq!(grouped.decision.groups.len(), 2);
        assert_eq!(grouped.auxiliary_files.len(), 2);

        cleanup_root(temp_root);
    }

    #[test]
    fn grouping_keeps_contiguous_loose_tracks_together() {
        let temp_root = test_root("group-loose");
        fs::create_dir_all(&temp_root).expect("root should be created");
        fs::write(temp_root.join("01 Intro.flac"), b"flac").expect("track should exist");
        fs::write(temp_root.join("02 Song.flac"), b"flac").expect("track should exist");
        fs::write(temp_root.join("cover.jpg"), b"jpg").expect("cover should exist");

        let grouped = group_release_inputs(
            &[
                temp_root.join("01 Intro.flac"),
                temp_root.join("02 Song.flac"),
            ],
            &crate::config::AppConfig::default().import.supported_formats,
        )
        .expect("grouping should succeed");

        assert_eq!(grouped.decision.groups.len(), 1);
        assert_eq!(grouped.decision.groups[0].file_paths.len(), 2);
        assert_eq!(grouped.auxiliary_files.len(), 1);

        cleanup_root(temp_root);
    }

    #[test]
    fn grouping_splits_ambiguous_loose_tracks_instead_of_merging() {
        let temp_root = test_root("group-ambiguous");
        fs::create_dir_all(&temp_root).expect("root should be created");
        fs::write(temp_root.join("01 Intro.flac"), b"flac").expect("track should exist");
        fs::write(temp_root.join("07 Other.flac"), b"flac").expect("track should exist");

        let grouped = group_release_inputs(
            &[
                temp_root.join("01 Intro.flac"),
                temp_root.join("07 Other.flac"),
            ],
            &crate::config::AppConfig::default().import.supported_formats,
        )
        .expect("grouping should succeed");

        assert_eq!(grouped.decision.groups.len(), 2);
        assert_eq!(grouped.decision.notes.len(), 1);

        cleanup_root(temp_root);
    }

    #[test]
    fn id3_mapping_extracts_day_one_fields() {
        let mut tag = id3::Tag::new();
        tag.set_artist("Radiohead");
        tag.set_album("Kid A");
        tag.set_title("Everything in Its Right Place");
        tag.set_track(1);
        tag.set_disc(1);
        tag.set_year(2000);

        let observations = observations_from_id3_tag(&tag);

        assert!(observations.iter().any(|value| {
            value.kind == ObservedValueKind::Artist && value.value == "Radiohead"
        }));
        assert!(observations.iter().any(|value| {
            value.kind == ObservedValueKind::ReleaseTitle && value.value == "Kid A"
        }));
        assert!(
            observations.iter().any(|value| {
                value.kind == ObservedValueKind::TrackNumber && value.value == "1"
            })
        );
    }

    #[test]
    fn vorbis_mapping_extracts_supported_fields() {
        let observations = observations_from_vorbis_comments(vec![
            ("ARTIST".to_string(), "Radiohead".to_string()),
            ("ALBUM".to_string(), "Kid A".to_string()),
            ("TRACKNUMBER".to_string(), "1".to_string()),
            ("DATE".to_string(), "2000".to_string()),
        ]);

        assert_eq!(observations.len(), 4);
        assert!(observations.iter().any(|value| {
            value.kind == ObservedValueKind::ReleaseYear && value.value == "2000"
        }));
    }

    #[test]
    fn gazelle_yaml_parser_keeps_structured_supporting_evidence() {
        let temp_root = test_root("yaml");
        fs::create_dir_all(&temp_root).expect("root should be created");
        let yaml_path = temp_root.join("release.yaml");
        fs::write(
            &yaml_path,
            "release_name: Kid A\nartist: Radiohead\nyear: 2000\nmedia: CD\ntracker_id: 1234\n",
        )
        .expect("yaml should be written");

        let record =
            parse_gazelle_yaml_evidence(&ImportBatchId::new(), "radiohead-kid-a", &yaml_path, 400)
                .expect("yaml parsing should succeed");

        assert_eq!(record.source, IngestEvidenceSource::GazelleYaml);
        assert!(record.structured_payload.is_some());
        assert!(record.observations.iter().any(|value| {
            value.kind == ObservedValueKind::ReleaseTitle && value.value == "Kid A"
        }));

        cleanup_root(temp_root);
    }

    #[test]
    fn mp3_extraction_reads_embedded_id3_tags() {
        let temp_root = test_root("mp3-tags");
        fs::create_dir_all(&temp_root).expect("root should be created");
        let mp3_path = temp_root.join("01-test.mp3");
        fs::write(&mp3_path, b"").expect("mp3 placeholder should exist");

        let mut tag = id3::Tag::new();
        tag.set_artist("Radiohead");
        tag.set_album("Kid A");
        tag.set_title("Everything in Its Right Place");
        tag.write_to_path(&mp3_path, id3::Version::Id3v24)
            .expect("id3 tag should be written");

        let record = extract_audio_file_evidence(&ImportBatchId::new(), &mp3_path, 401)
            .expect("mp3 evidence extraction should succeed");

        assert_eq!(record.source, IngestEvidenceSource::EmbeddedTags);
        assert!(record.observations.iter().any(|value| {
            value.kind == ObservedValueKind::Artist && value.value == "Radiohead"
        }));

        cleanup_root(temp_root);
    }

    #[test]
    fn service_persists_batch_analysis_output() {
        let temp_root = test_root("analyze-batch");
        let album_path = temp_root.join("Kid A");
        fs::create_dir_all(&album_path).expect("album directory should be created");
        let mp3_path = album_path.join("01 Everything.mp3");
        fs::write(&mp3_path, b"").expect("mp3 placeholder should exist");

        let mut tag = id3::Tag::new();
        tag.set_artist("Radiohead");
        tag.set_album("Kid A");
        tag.set_title("Everything in Its Right Place");
        tag.set_track(1);
        tag.write_to_path(&mp3_path, id3::Version::Id3v24)
            .expect("id3 tag should be written");

        let yaml_path = album_path.join("release.yaml");
        fs::write(
            &yaml_path,
            "release_name: Kid A\nartist: Radiohead\nyear: 2000\n",
        )
        .expect("yaml should be written");

        let repository = InMemoryIngestRepository::default();
        let config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        let service = WatchDiscoveryService::new(repository.clone(), config);

        let submission = service
            .submit_manual_path("chris", album_path.clone(), 500)
            .expect("manual intake should succeed");
        let report = service
            .analyze_import_batch(&submission.batch.id, 501)
            .expect("batch analysis should succeed");

        assert_eq!(report.batch.status, ImportBatchStatus::Grouped);
        assert_eq!(report.manifest.discovered_files.len(), 1);
        assert_eq!(report.manifest.auxiliary_files.len(), 1);
        assert_eq!(report.evidence_records.len(), 2);
        assert_eq!(report.metadata_snapshots.len(), 2);
        assert!(report.metadata_snapshots.iter().any(|snapshot| {
            snapshot.source == MetadataSnapshotSource::GazelleYaml
                && snapshot.format == SnapshotFormat::Yaml
        }));

        cleanup_root(temp_root);
    }

    #[derive(Clone, Default)]
    struct InMemoryIngestRepository {
        sources: Arc<Mutex<HashMap<String, Source>>>,
        batches: Arc<Mutex<HashMap<String, ImportBatch>>>,
        jobs: Arc<Mutex<HashMap<String, Job>>>,
        manifests: Arc<Mutex<Vec<StagingManifest>>>,
        evidence_records: Arc<Mutex<Vec<IngestEvidenceRecord>>>,
        metadata_snapshots: Arc<Mutex<Vec<MetadataSnapshot>>>,
    }

    impl SourceRepository for InMemoryIngestRepository {
        fn get_source(
            &self,
            id: &crate::support::ids::SourceId,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(self
                .sources
                .lock()
                .expect("repository should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

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

        fn update_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError> {
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

    impl StagingManifestCommandRepository for InMemoryIngestRepository {
        fn create_staging_manifest(
            &self,
            manifest: &StagingManifest,
        ) -> Result<(), RepositoryError> {
            self.manifests
                .lock()
                .expect("repository should lock")
                .push(manifest.clone());
            Ok(())
        }
    }

    impl IngestEvidenceCommandRepository for InMemoryIngestRepository {
        fn create_ingest_evidence_records(
            &self,
            records: &[IngestEvidenceRecord],
        ) -> Result<(), RepositoryError> {
            self.evidence_records
                .lock()
                .expect("repository should lock")
                .extend(records.iter().cloned());
            Ok(())
        }
    }

    impl MetadataSnapshotCommandRepository for InMemoryIngestRepository {
        fn create_metadata_snapshots(
            &self,
            snapshots: &[MetadataSnapshot],
        ) -> Result<(), RepositoryError> {
            self.metadata_snapshots
                .lock()
                .expect("repository should lock")
                .extend(snapshots.iter().cloned());
            Ok(())
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
