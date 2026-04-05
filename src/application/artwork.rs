use std::fs;
use std::path::{Path, PathBuf};

use crate::application::config::{ExportPolicy, StoragePolicy};
use crate::application::repository::{
    ExportRepository, IssueCommandRepository, IssueListQuery, IssueRepository,
    ManualOverrideListQuery, ManualOverrideRepository, ReleaseArtworkCommandRepository,
    ReleaseArtworkRepository, ReleaseInstanceRepository, ReleaseRepository, RepositoryError,
    RepositoryErrorKind, StagingManifestRepository,
};
use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
use crate::domain::manual_override::{OverrideField, OverrideSubject};
use crate::domain::release_artwork::{ArtworkSource, ReleaseArtwork};
use crate::domain::staging_manifest::{AuxiliaryFileRole, StagingManifest};
use crate::support::ids::{ReleaseArtworkId, ReleaseInstanceId};
use crate::support::pagination::PageRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtworkExportReport {
    pub selected_artwork: Option<ReleaseArtwork>,
    pub managed_artwork_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtworkExportError {
    pub kind: ArtworkExportErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtworkExportErrorKind {
    NotFound,
    Conflict,
    Storage,
}

pub struct ArtworkService<R> {
    repository: R,
}

impl<R> ArtworkService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> ArtworkService<R>
where
    R: ExportRepository
        + IssueCommandRepository
        + IssueRepository
        + ManualOverrideRepository
        + ReleaseArtworkCommandRepository
        + ReleaseArtworkRepository
        + ReleaseInstanceRepository
        + ReleaseRepository
        + StagingManifestRepository,
{
    pub fn export_primary_artwork(
        &self,
        storage: &StoragePolicy,
        export: &ExportPolicy,
        release_instance_id: &ReleaseInstanceId,
        changed_at_unix_seconds: i64,
    ) -> Result<ArtworkExportReport, ArtworkExportError> {
        let release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| ArtworkExportError {
                kind: ArtworkExportErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        let release_id = release_instance
            .release_id
            .clone()
            .ok_or_else(|| ArtworkExportError {
                kind: ArtworkExportErrorKind::Conflict,
                message: format!(
                    "release instance {} has no canonical release",
                    release_instance.id.as_uuid()
                ),
            })?;
        let export_snapshot = self
            .repository
            .get_latest_exported_metadata(&release_instance.id)
            .map_err(map_repository_error)?
            .ok_or_else(|| ArtworkExportError {
                kind: ArtworkExportErrorKind::NotFound,
                message: format!(
                    "no exported metadata snapshot found for {}",
                    release_instance.id.as_uuid()
                ),
            })?;
        let managed_files = self
            .repository
            .list_files_for_release_instance(
                &release_instance.id,
                Some(crate::domain::file::FileRole::Managed),
            )
            .map_err(map_repository_error)?;
        let managed_directory = managed_files
            .first()
            .and_then(|file| file.path.parent())
            .map(Path::to_path_buf)
            .ok_or_else(|| ArtworkExportError {
                kind: ArtworkExportErrorKind::Conflict,
                message: format!(
                    "release instance {} has no managed output directory",
                    release_instance.id.as_uuid()
                ),
            })?;
        let manifests = self
            .repository
            .list_staging_manifests_for_batch(&release_instance.import_batch_id)
            .map_err(map_repository_error)?;
        let existing_artwork = self
            .repository
            .list_release_artwork_for_release_instance(&release_instance.id)
            .map_err(map_repository_error)?;
        let override_selection = self
            .repository
            .list_manual_overrides(&ManualOverrideListQuery {
                subject: Some(OverrideSubject::ReleaseInstance(
                    release_instance.id.clone(),
                )),
                field: Some(OverrideField::ArtworkSelection),
                page: PageRequest::new(1, 0),
            })
            .map_err(map_repository_error)?
            .items
            .into_iter()
            .next()
            .map(|item| item.value);
        let candidates = collect_artwork_candidates(&manifests, &release_instance);

        let selected = choose_artwork(
            &existing_artwork,
            override_selection.as_deref(),
            &candidates,
            &release_id,
            &release_instance.id,
            export_snapshot.primary_artwork_filename.as_deref(),
        );

        let managed_artwork_path = if let Some(artwork) = &selected {
            let file_name = artwork
                .managed_filename
                .clone()
                .or_else(|| export_snapshot.primary_artwork_filename.clone())
                .or_else(|| default_sidecar_name(export))
                .ok_or_else(|| ArtworkExportError {
                    kind: ArtworkExportErrorKind::Conflict,
                    message: "export profile did not provide a sidecar artwork name".to_string(),
                })?;
            let original_path =
                artwork
                    .original_path
                    .as_ref()
                    .ok_or_else(|| ArtworkExportError {
                        kind: ArtworkExportErrorKind::Conflict,
                        message: format!("artwork {} had no source path", artwork.id.as_uuid()),
                    })?;
            let target_path = managed_directory.join(file_name);
            fs::create_dir_all(&storage.managed_library_root).map_err(storage_error)?;
            fs::copy(original_path, &target_path).map_err(storage_error)?;
            Some(target_path)
        } else {
            None
        };

        let mut persisted = Vec::new();
        if let Some(mut artwork) = selected {
            artwork.managed_filename = managed_artwork_path
                .as_ref()
                .and_then(|path| path.file_name())
                .and_then(|value| value.to_str())
                .map(|value| value.to_string());
            persisted.push(artwork);
        }
        self.repository
            .replace_release_artwork_for_release_instance(&release_instance.id, &persisted)
            .map_err(map_repository_error)?;
        synchronize_missing_artwork_issue(
            &self.repository,
            &release_instance.id,
            managed_artwork_path.is_some(),
            changed_at_unix_seconds,
        )?;

        Ok(ArtworkExportReport {
            selected_artwork: persisted.into_iter().next(),
            managed_artwork_path,
        })
    }
}

fn collect_artwork_candidates(
    manifests: &[StagingManifest],
    release_instance: &crate::domain::release_instance::ReleaseInstance,
) -> Vec<PathBuf> {
    let mut candidates = manifests
        .iter()
        .flat_map(|manifest| {
            manifest
                .auxiliary_files
                .iter()
                .filter(|file| matches!(file.role, AuxiliaryFileRole::Artwork))
                .filter(|file| {
                    manifest.grouping.groups.iter().any(|group| {
                        group.auxiliary_paths.contains(&file.path)
                            && representative_group_path(group)
                                == release_instance.provenance.original_source_path
                    })
                })
                .map(|file| file.path.clone())
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|path| artwork_sort_key(path));
    candidates
}

fn representative_group_path(
    group: &crate::domain::staging_manifest::StagedReleaseGroup,
) -> String {
    group
        .file_paths
        .iter()
        .map(|path| {
            path.parent()
                .unwrap_or(path.as_path())
                .display()
                .to_string()
        })
        .min()
        .unwrap_or_else(|| group.key.clone())
}

fn artwork_sort_key(path: &Path) -> (u8, String) {
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let rank = match stem.as_str() {
        "cover" => 0,
        "folder" => 1,
        "front" => 2,
        _ => 3,
    };
    (
        rank,
        path.file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string(),
    )
}

fn choose_artwork(
    existing: &[ReleaseArtwork],
    override_selection: Option<&str>,
    candidates: &[PathBuf],
    release_id: &crate::support::ids::ReleaseId,
    release_instance_id: &ReleaseInstanceId,
    managed_filename: Option<&str>,
) -> Option<ReleaseArtwork> {
    if let Some(existing) = existing.iter().find(|artwork| {
        artwork.is_primary
            && artwork
                .original_path
                .as_ref()
                .is_some_and(|path| path.is_file())
    }) {
        return Some(existing.clone());
    }

    let source_path = override_selection
        .and_then(|selection| resolve_override_path(selection, candidates))
        .or_else(|| candidates.first().cloned())?;

    Some(ReleaseArtwork {
        id: ReleaseArtworkId::new(),
        release_id: release_id.clone(),
        release_instance_id: Some(release_instance_id.clone()),
        source: if override_selection.is_some() {
            ArtworkSource::OperatorSelected
        } else {
            ArtworkSource::SourceLocal
        },
        is_primary: true,
        original_path: Some(source_path.clone()),
        managed_filename: managed_filename.map(|value| value.to_string()),
        mime_type: infer_mime_type(&source_path),
    })
}

fn resolve_override_path(selection: &str, candidates: &[PathBuf]) -> Option<PathBuf> {
    let direct = PathBuf::from(selection);
    if direct.is_file() {
        return Some(direct);
    }

    candidates
        .iter()
        .find(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case(selection))
        })
        .cloned()
}

fn infer_mime_type(path: &Path) -> String {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => "image/png".to_string(),
        _ => "image/jpeg".to_string(),
    }
}

fn default_sidecar_name(export: &ExportPolicy) -> Option<String> {
    export
        .profiles
        .iter()
        .find(|profile| profile.name == export.default_profile)
        .map(|profile| match &profile.artwork {
            crate::domain::export_profile::ArtworkPolicy::SidecarFile { file_name, .. } => {
                file_name.clone()
            }
        })
}

fn synchronize_missing_artwork_issue<R>(
    repository: &R,
    release_instance_id: &ReleaseInstanceId,
    artwork_present: bool,
    changed_at_unix_seconds: i64,
) -> Result<(), ArtworkExportError>
where
    R: IssueCommandRepository + IssueRepository,
{
    let subject = IssueSubject::ReleaseInstance(release_instance_id.clone());
    let existing = repository
        .list_issues(&IssueListQuery {
            state: Some(IssueState::Open),
            issue_type: Some(IssueType::MissingArtwork),
            subject: Some(subject.clone()),
            page: PageRequest::new(50, 0),
        })
        .map_err(map_repository_error)?;

    if artwork_present {
        for mut issue in existing.items {
            issue
                .resolve(changed_at_unix_seconds)
                .map_err(|error| ArtworkExportError {
                    kind: ArtworkExportErrorKind::Conflict,
                    message: format!("failed to resolve missing artwork issue: {error:?}"),
                })?;
            repository
                .update_issue(&issue)
                .map_err(map_repository_error)?;
        }
        return Ok(());
    }

    if existing.items.is_empty() {
        repository
            .create_issue(&Issue::open(
                IssueType::MissingArtwork,
                subject,
                "Artwork missing",
                Some("No operator-selected or source-local artwork was available.".to_string()),
                changed_at_unix_seconds,
            ))
            .map_err(map_repository_error)?;
    }
    Ok(())
}

fn map_repository_error(error: RepositoryError) -> ArtworkExportError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => ArtworkExportErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            ArtworkExportErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => ArtworkExportErrorKind::Storage,
    };
    ArtworkExportError {
        kind,
        message: error.message,
    }
}

fn storage_error(error: std::io::Error) -> ArtworkExportError {
    ArtworkExportError {
        kind: ArtworkExportErrorKind::Storage,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::application::repository::{
        ExportCommandRepository, ReleaseInstanceCommandRepository,
    };
    use crate::domain::exported_metadata_snapshot::{
        CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
    };
    use crate::domain::file::{FileRecord, FileRole};
    use crate::domain::manual_override::ManualOverride;
    use crate::domain::release::Release;
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        ReleaseInstanceState, TechnicalVariant,
    };
    use crate::domain::source::SourceKind;
    use crate::domain::staging_manifest::{
        AuxiliaryFile, GroupingDecision, GroupingStrategy, StagedReleaseGroup, StagingManifest,
        StagingManifestSource,
    };
    use crate::support::ids::{FileId, ReleaseGroupId, TrackInstanceId};

    use super::*;

    #[test]
    fn exports_operator_selected_artwork_as_sidecar() {
        let repository = InMemoryArtworkRepository::seeded(true, true);
        let export = crate::application::config::ValidatedRuntimeConfig::from_validated_app_config(
            &crate::config::AppConfig::default(),
        )
        .export;
        let storage =
            crate::application::config::ValidatedRuntimeConfig::from_validated_app_config(
                &crate::config::AppConfig::default(),
            )
            .storage;

        let report = ArtworkService::new(repository.clone())
            .export_primary_artwork(&storage, &export, &repository.release_instance.id, 200)
            .expect("artwork export should succeed");

        let selected = report.selected_artwork.expect("artwork should be selected");
        assert_eq!(selected.source, ArtworkSource::OperatorSelected);
        assert!(
            report
                .managed_artwork_path
                .expect("sidecar path should exist")
                .ends_with("cover.jpg")
        );
        assert_eq!(
            repository
                .stored_release_artwork()
                .first()
                .expect("artwork should persist")
                .managed_filename
                .as_deref(),
            Some("cover.jpg")
        );
    }

    #[test]
    fn opens_missing_artwork_issue_when_no_candidate_exists() {
        let repository = InMemoryArtworkRepository::seeded(false, false);
        let export = crate::application::config::ValidatedRuntimeConfig::from_validated_app_config(
            &crate::config::AppConfig::default(),
        )
        .export;
        let storage =
            crate::application::config::ValidatedRuntimeConfig::from_validated_app_config(
                &crate::config::AppConfig::default(),
            )
            .storage;

        let report = ArtworkService::new(repository.clone())
            .export_primary_artwork(&storage, &export, &repository.release_instance.id, 200)
            .expect("artwork export should succeed");

        assert!(report.selected_artwork.is_none());
        assert!(repository.has_open_missing_artwork_issue());
    }

    #[derive(Clone)]
    struct InMemoryArtworkRepository {
        release_instance: ReleaseInstance,
        release: Release,
        release_group: ReleaseGroup,
        manifests: Arc<Mutex<Vec<StagingManifest>>>,
        overrides: Arc<Mutex<Vec<ManualOverride>>>,
        issues: Arc<Mutex<Vec<Issue>>>,
        artworks: Arc<Mutex<Vec<ReleaseArtwork>>>,
        exports: Arc<Mutex<Vec<ExportedMetadataSnapshot>>>,
        files: Arc<Mutex<Vec<FileRecord>>>,
    }

    impl InMemoryArtworkRepository {
        fn seeded(with_artwork: bool, with_override: bool) -> Self {
            let root =
                std::env::temp_dir().join(format!("discern-artwork-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(root.join("imports/album")).expect("temp directories should create");
            fs::create_dir_all(root.join("managed/Radiohead/Kid A [FLAC]"))
                .expect("managed directory should create");
            let artwork_path = root.join("imports/album/folder.jpg");
            if with_artwork {
                fs::write(&artwork_path, b"jpeg-data").expect("artwork should write");
            }

            let release_group = ReleaseGroup {
                id: ReleaseGroupId::new(),
                primary_artist_id: crate::support::ids::ArtistId::new(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
                musicbrainz_release_group_id: None,
            };
            let release = Release {
                id: crate::support::ids::ReleaseId::new(),
                release_group_id: release_group.id.clone(),
                primary_artist_id: release_group.primary_artist_id.clone(),
                title: "Kid A".to_string(),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: Default::default(),
            };
            let release_instance = ReleaseInstance {
                id: ReleaseInstanceId::new(),
                import_batch_id: crate::support::ids::ImportBatchId::new(),
                source_id: crate::support::ids::SourceId::new(),
                release_id: Some(release.id.clone()),
                state: ReleaseInstanceState::Imported,
                technical_variant: TechnicalVariant {
                    format_family: FormatFamily::Flac,
                    bitrate_mode: BitrateMode::Lossless,
                    bitrate_kbps: None,
                    sample_rate_hz: Some(44_100),
                    bit_depth: Some(16),
                    track_count: 1,
                    total_duration_seconds: 200,
                },
                provenance: ProvenanceSnapshot {
                    ingest_origin: IngestOrigin::ManualAdd,
                    original_source_path: root.join("imports/album").display().to_string(),
                    imported_at_unix_seconds: 100,
                    gazelle_reference: None,
                },
            };
            let manifests = vec![StagingManifest {
                id: crate::support::ids::StagingManifestId::new(),
                batch_id: release_instance.import_batch_id.clone(),
                source: StagingManifestSource {
                    kind: SourceKind::ManualAdd,
                    source_path: root.join("imports/album"),
                },
                discovered_files: Vec::new(),
                auxiliary_files: if with_artwork {
                    vec![AuxiliaryFile {
                        path: artwork_path.clone(),
                        role: AuxiliaryFileRole::Artwork,
                    }]
                } else {
                    Vec::new()
                },
                grouping: GroupingDecision {
                    strategy: GroupingStrategy::CommonParentDirectory,
                    groups: vec![StagedReleaseGroup {
                        key: "album".to_string(),
                        file_paths: vec![root.join("imports/album/01-track.flac")],
                        auxiliary_paths: if with_artwork {
                            vec![artwork_path.clone()]
                        } else {
                            Vec::new()
                        },
                    }],
                    notes: Vec::new(),
                },
                captured_at_unix_seconds: 100,
            }];
            let overrides = if with_override {
                vec![ManualOverride {
                    id: crate::support::ids::ManualOverrideId::new(),
                    subject: OverrideSubject::ReleaseInstance(release_instance.id.clone()),
                    field: OverrideField::ArtworkSelection,
                    value: "folder.jpg".to_string(),
                    note: None,
                    created_by: "operator".to_string(),
                    created_at_unix_seconds: 150,
                }]
            } else {
                Vec::new()
            };

            Self {
                release_instance: release_instance.clone(),
                release,
                release_group,
                manifests: Arc::new(Mutex::new(manifests)),
                overrides: Arc::new(Mutex::new(overrides)),
                issues: Arc::new(Mutex::new(Vec::new())),
                artworks: Arc::new(Mutex::new(Vec::new())),
                exports: Arc::new(Mutex::new(vec![ExportedMetadataSnapshot {
                    id: crate::support::ids::ExportedMetadataSnapshotId::new(),
                    release_instance_id: release_instance.id.clone(),
                    export_profile: "generic_player".to_string(),
                    album_title: "Kid A".to_string(),
                    album_artist: "Radiohead".to_string(),
                    artist_credits: vec!["Radiohead".to_string()],
                    edition_visibility: QualifierVisibility::TagsAndPath,
                    technical_visibility: QualifierVisibility::PathOnly,
                    path_components: vec!["Radiohead".to_string(), "Kid A [FLAC]".to_string()],
                    primary_artwork_filename: Some("cover.jpg".to_string()),
                    compatibility: CompatibilityReport {
                        verified: true,
                        warnings: Vec::new(),
                    },
                    rendered_at_unix_seconds: 100,
                }])),
                files: Arc::new(Mutex::new(vec![FileRecord {
                    id: FileId::new(),
                    track_instance_id: TrackInstanceId::new(),
                    role: FileRole::Managed,
                    format_family: FormatFamily::Flac,
                    path: root.join("managed/Radiohead/Kid A [FLAC]/01 - Track.flac"),
                    checksum: None,
                    size_bytes: 10,
                }])),
            }
        }

        fn stored_release_artwork(&self) -> Vec<ReleaseArtwork> {
            self.artworks.lock().expect("artworks should lock").clone()
        }

        fn has_open_missing_artwork_issue(&self) -> bool {
            self.issues
                .lock()
                .expect("issues should lock")
                .iter()
                .any(|issue| {
                    issue.issue_type == IssueType::MissingArtwork && issue.state == IssueState::Open
                })
        }
    }

    impl ReleaseInstanceRepository for InMemoryArtworkRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<crate::domain::release_instance::ReleaseInstance>, RepositoryError>
        {
            Ok((self.release_instance.id == *id).then_some(self.release_instance.clone()))
        }

        fn list_release_instances(
            &self,
            _query: &crate::application::repository::ReleaseInstanceListQuery,
        ) -> Result<
            crate::support::pagination::Page<crate::domain::release_instance::ReleaseInstance>,
            RepositoryError,
        > {
            unimplemented!()
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<crate::domain::release_instance::ReleaseInstance>, RepositoryError>
        {
            unimplemented!()
        }

        fn list_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _page: &PageRequest,
        ) -> Result<
            crate::support::pagination::Page<crate::domain::candidate_match::CandidateMatch>,
            RepositoryError,
        > {
            unimplemented!()
        }

        fn get_candidate_match(
            &self,
            _id: &crate::support::ids::CandidateMatchId,
        ) -> Result<Option<crate::domain::candidate_match::CandidateMatch>, RepositoryError>
        {
            unimplemented!()
        }

        fn list_track_instances_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<crate::domain::track_instance::TrackInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            role: Option<crate::domain::file::FileRole>,
        ) -> Result<Vec<FileRecord>, RepositoryError> {
            Ok(self
                .files
                .lock()
                .expect("files should lock")
                .iter()
                .filter(|file| role.as_ref().is_none_or(|value| &file.role == value))
                .cloned()
                .collect())
        }
    }

    impl ReleaseRepository for InMemoryArtworkRepository {
        fn find_artist_by_musicbrainz_id(
            &self,
            _musicbrainz_artist_id: &str,
        ) -> Result<Option<crate::domain::artist::Artist>, RepositoryError> {
            Ok(None)
        }

        fn get_release_group(
            &self,
            id: &crate::support::ids::ReleaseGroupId,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok((self.release_group.id == *id).then_some(self.release_group.clone()))
        }

        fn find_release_group_by_musicbrainz_id(
            &self,
            _musicbrainz_release_group_id: &str,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(None)
        }

        fn get_release(
            &self,
            id: &crate::support::ids::ReleaseId,
        ) -> Result<Option<Release>, RepositoryError> {
            Ok((self.release.id == *id).then_some(self.release.clone()))
        }

        fn find_release_by_musicbrainz_id(
            &self,
            _musicbrainz_release_id: &str,
        ) -> Result<Option<Release>, RepositoryError> {
            Ok(None)
        }

        fn search_release_groups(
            &self,
            _query: &crate::application::repository::ReleaseGroupSearchQuery,
        ) -> Result<crate::support::pagination::Page<ReleaseGroup>, RepositoryError> {
            unimplemented!()
        }

        fn list_releases(
            &self,
            _query: &crate::application::repository::ReleaseListQuery,
        ) -> Result<crate::support::pagination::Page<Release>, RepositoryError> {
            unimplemented!()
        }

        fn list_tracks_for_release(
            &self,
            _release_id: &crate::support::ids::ReleaseId,
        ) -> Result<Vec<crate::domain::track::Track>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl StagingManifestRepository for InMemoryArtworkRepository {
        fn list_staging_manifests_for_batch(
            &self,
            _batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(self
                .manifests
                .lock()
                .expect("manifests should lock")
                .clone())
        }
    }

    impl ManualOverrideRepository for InMemoryArtworkRepository {
        fn get_manual_override(
            &self,
            _id: &crate::support::ids::ManualOverrideId,
        ) -> Result<Option<ManualOverride>, RepositoryError> {
            Ok(None)
        }

        fn list_manual_overrides(
            &self,
            query: &ManualOverrideListQuery,
        ) -> Result<crate::support::pagination::Page<ManualOverride>, RepositoryError> {
            let items = self
                .overrides
                .lock()
                .expect("overrides should lock")
                .iter()
                .filter(|item| {
                    query
                        .subject
                        .as_ref()
                        .is_none_or(|subject| &item.subject == subject)
                })
                .filter(|item| {
                    query
                        .field
                        .as_ref()
                        .is_none_or(|field| &item.field == field)
                })
                .cloned()
                .collect::<Vec<_>>();
            Ok(crate::support::pagination::Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl IssueRepository for InMemoryArtworkRepository {
        fn get_issue(
            &self,
            _id: &crate::support::ids::IssueId,
        ) -> Result<Option<Issue>, RepositoryError> {
            Ok(None)
        }

        fn list_issues(
            &self,
            query: &IssueListQuery,
        ) -> Result<crate::support::pagination::Page<Issue>, RepositoryError> {
            let items = self
                .issues
                .lock()
                .expect("issues should lock")
                .iter()
                .filter(|issue| {
                    query
                        .state
                        .as_ref()
                        .is_none_or(|state| &issue.state == state)
                })
                .filter(|issue| {
                    query
                        .issue_type
                        .as_ref()
                        .is_none_or(|issue_type| &issue.issue_type == issue_type)
                })
                .filter(|issue| {
                    query
                        .subject
                        .as_ref()
                        .is_none_or(|subject| &issue.subject == subject)
                })
                .cloned()
                .collect::<Vec<_>>();
            Ok(crate::support::pagination::Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl IssueCommandRepository for InMemoryArtworkRepository {
        fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("issues should lock")
                .push(issue.clone());
            Ok(())
        }

        fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            let mut issues = self.issues.lock().expect("issues should lock");
            if let Some(stored) = issues.iter_mut().find(|stored| stored.id == issue.id) {
                *stored = issue.clone();
            }
            Ok(())
        }
    }

    impl ReleaseArtworkRepository for InMemoryArtworkRepository {
        fn get_release_artwork(
            &self,
            _id: &ReleaseArtworkId,
        ) -> Result<Option<ReleaseArtwork>, RepositoryError> {
            Ok(None)
        }

        fn list_release_artwork_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<ReleaseArtwork>, RepositoryError> {
            Ok(self.artworks.lock().expect("artworks should lock").clone())
        }
    }

    impl ReleaseArtworkCommandRepository for InMemoryArtworkRepository {
        fn replace_release_artwork_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            artwork: &[ReleaseArtwork],
        ) -> Result<(), RepositoryError> {
            let mut stored = self.artworks.lock().expect("artworks should lock");
            *stored = artwork.to_vec();
            Ok(())
        }
    }

    impl ExportRepository for InMemoryArtworkRepository {
        fn get_latest_exported_metadata(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(self
                .exports
                .lock()
                .expect("exports should lock")
                .first()
                .cloned())
        }

        fn list_exported_metadata(
            &self,
            _query: &crate::application::repository::ExportedMetadataListQuery,
        ) -> Result<crate::support::pagination::Page<ExportedMetadataSnapshot>, RepositoryError>
        {
            unimplemented!()
        }

        fn get_exported_metadata(
            &self,
            _id: &crate::support::ids::ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            unimplemented!()
        }
    }

    impl ExportCommandRepository for InMemoryArtworkRepository {
        fn create_exported_metadata_snapshot(
            &self,
            _snapshot: &ExportedMetadataSnapshot,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn update_exported_metadata_snapshot(
            &self,
            _snapshot: &ExportedMetadataSnapshot,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl ReleaseInstanceCommandRepository for InMemoryArtworkRepository {
        fn create_release_instance(
            &self,
            _release_instance: &crate::domain::release_instance::ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn update_release_instance(
            &self,
            _release_instance: &crate::domain::release_instance::ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _matches: &[crate::domain::candidate_match::CandidateMatch],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_candidate_matches_for_provider(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _provider: &crate::domain::candidate_match::CandidateProvider,
            _matches: &[crate::domain::candidate_match::CandidateMatch],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_track_instances_and_files(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _track_instances: &[crate::domain::track_instance::TrackInstance],
            _files: &[FileRecord],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }
}
