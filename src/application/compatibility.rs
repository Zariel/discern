use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use tokio::task;

use crate::application::repository::{
    ExportCommandRepository, ExportRepository, IssueCommandRepository, IssueListQuery,
    IssueRepository, ReleaseInstanceListQuery, ReleaseInstanceRepository, ReleaseRepository,
    RepositoryError, RepositoryErrorKind,
};
use crate::domain::exported_metadata_snapshot::ExportedMetadataSnapshot;
use crate::domain::file::FileRole;
use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
use crate::domain::release::Release;
use crate::domain::release_instance::ReleaseInstance;
use crate::support::ids::ReleaseInstanceId;
use crate::support::pagination::PageRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityVerificationReport {
    pub verified: bool,
    pub warnings: Vec<String>,
    pub issue_types: Vec<IssueType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityVerificationError {
    pub kind: CompatibilityVerificationErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityVerificationErrorKind {
    NotFound,
    Conflict,
    Storage,
}

pub struct CompatibilityVerificationService<R> {
    repository: R,
}

impl<R> CompatibilityVerificationService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> CompatibilityVerificationService<R>
where
    R: ExportCommandRepository
        + ExportRepository
        + IssueCommandRepository
        + IssueRepository
        + ReleaseInstanceRepository
        + ReleaseRepository,
{
    pub async fn verify_release_instance(
        &self,
        release_instance_id: &ReleaseInstanceId,
        verified_at_unix_seconds: i64,
    ) -> Result<CompatibilityVerificationReport, CompatibilityVerificationError> {
        let release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| CompatibilityVerificationError {
                kind: CompatibilityVerificationErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        let release = load_release(&self.repository, &release_instance)?;
        let release_group_releases = self
            .repository
            .list_releases(&crate::application::repository::ReleaseListQuery {
                release_group_id: Some(release.release_group_id.clone()),
                text: None,
                page: PageRequest::new(100, 0),
            })
            .map_err(map_repository_error)?
            .items;
        let current_snapshot = self
            .repository
            .get_latest_exported_metadata(&release_instance.id)
            .map_err(map_repository_error)?
            .ok_or_else(|| CompatibilityVerificationError {
                kind: CompatibilityVerificationErrorKind::NotFound,
                message: format!(
                    "no exported metadata snapshot found for {}",
                    release_instance.id.as_uuid()
                ),
            })?;
        let canonical_tracks = self
            .repository
            .list_tracks_for_release(&release.id)
            .map_err(map_repository_error)?;
        let track_instances = self
            .repository
            .list_track_instances_for_release_instance(&release_instance.id)
            .map_err(map_repository_error)?;
        let managed_files = self
            .repository
            .list_files_for_release_instance(&release_instance.id, Some(FileRole::Managed))
            .map_err(map_repository_error)?;

        let sibling_instances = load_sibling_instances(
            &self.repository,
            &release_group_releases,
            &release_instance.id,
        )?;
        let sibling_exports = load_sibling_exports(&self.repository, &sibling_instances)?;

        let filesystem_findings = task::spawn_blocking({
            let managed_paths = managed_files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>();
            let artwork = current_snapshot.primary_artwork_filename.clone();
            move || inspect_filesystem(&managed_paths, artwork.as_deref())
        })
        .await
        .map_err(|error| CompatibilityVerificationError {
            kind: CompatibilityVerificationErrorKind::Storage,
            message: format!("compatibility verification task failed to join: {error}"),
        })?;

        let path_conflicts = find_path_conflicts(
            &release_instance,
            &current_snapshot,
            &sibling_instances,
            &sibling_exports,
        );
        let visibility_conflicts = find_visibility_conflicts(
            &release_instance,
            &current_snapshot,
            &sibling_instances,
            &sibling_exports,
        );
        let compatibility_failures = find_compatibility_failures(
            &release_instance,
            &canonical_tracks,
            &track_instances,
            &managed_files,
            &filesystem_findings,
        );

        synchronize_issue(
            &self.repository,
            &release_instance,
            IssueType::UndistinguishableReleaseInstance,
            path_conflicts.first().map(|summary| {
                (
                    summary.clone(),
                    render_issue_details(
                        "Managed output cannot safely distinguish this release instance:",
                        &path_conflicts,
                    ),
                )
            }),
            verified_at_unix_seconds,
        )?;
        synchronize_issue(
            &self.repository,
            &release_instance,
            IssueType::PlayerVisibilityCollision,
            visibility_conflicts.first().map(|summary| {
                (
                    summary.clone(),
                    render_issue_details(
                        "Exported player-visible metadata collides with another release instance:",
                        &visibility_conflicts,
                    ),
                )
            }),
            verified_at_unix_seconds,
        )?;
        synchronize_issue(
            &self.repository,
            &release_instance,
            IssueType::PlayerCompatibilityFailure,
            compatibility_failures.first().map(|summary| {
                (
                    summary.clone(),
                    render_issue_details(
                        "Managed output or export compatibility verification failed:",
                        &compatibility_failures,
                    ),
                )
            }),
            verified_at_unix_seconds,
        )?;

        let mut updated_snapshot = current_snapshot.clone();
        updated_snapshot.compatibility.verified = path_conflicts.is_empty()
            && visibility_conflicts.is_empty()
            && compatibility_failures.is_empty();
        updated_snapshot.compatibility.warnings = current_snapshot
            .compatibility
            .warnings
            .iter()
            .cloned()
            .chain(path_conflicts.iter().cloned())
            .chain(visibility_conflicts.iter().cloned())
            .chain(compatibility_failures.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self.repository
            .update_exported_metadata_snapshot(&updated_snapshot)
            .map_err(map_repository_error)?;

        let mut issue_types = Vec::new();
        if !path_conflicts.is_empty() {
            issue_types.push(IssueType::UndistinguishableReleaseInstance);
        }
        if !visibility_conflicts.is_empty() {
            issue_types.push(IssueType::PlayerVisibilityCollision);
        }
        if !compatibility_failures.is_empty() {
            issue_types.push(IssueType::PlayerCompatibilityFailure);
        }

        Ok(CompatibilityVerificationReport {
            verified: updated_snapshot.compatibility.verified,
            warnings: updated_snapshot.compatibility.warnings,
            issue_types,
        })
    }
}

fn load_release<R>(
    repository: &R,
    release_instance: &ReleaseInstance,
) -> Result<Release, CompatibilityVerificationError>
where
    R: ReleaseRepository,
{
    let release_id =
        release_instance
            .release_id
            .clone()
            .ok_or_else(|| CompatibilityVerificationError {
                kind: CompatibilityVerificationErrorKind::Conflict,
                message: format!(
                    "release instance {} has no canonical release",
                    release_instance.id.as_uuid()
                ),
            })?;
    repository
        .get_release(&release_id)
        .map_err(map_repository_error)?
        .ok_or_else(|| CompatibilityVerificationError {
            kind: CompatibilityVerificationErrorKind::NotFound,
            message: format!("no release found for {}", release_id.as_uuid()),
        })
}

fn load_sibling_instances<R>(
    repository: &R,
    releases: &[Release],
    current_release_instance_id: &ReleaseInstanceId,
) -> Result<Vec<ReleaseInstance>, CompatibilityVerificationError>
where
    R: ReleaseInstanceRepository,
{
    let mut instances = Vec::new();
    for release in releases {
        let page = repository
            .list_release_instances(&ReleaseInstanceListQuery {
                release_id: Some(release.id.clone()),
                state: None,
                format_family: None,
                page: PageRequest::new(100, 0),
            })
            .map_err(map_repository_error)?;
        instances.extend(
            page.items
                .into_iter()
                .filter(|item| item.id != *current_release_instance_id),
        );
    }
    Ok(instances)
}

fn load_sibling_exports<R>(
    repository: &R,
    siblings: &[ReleaseInstance],
) -> Result<HashMap<ReleaseInstanceId, ExportedMetadataSnapshot>, CompatibilityVerificationError>
where
    R: ExportRepository,
{
    let mut exports = HashMap::new();
    for sibling in siblings {
        if let Some(snapshot) = repository
            .get_latest_exported_metadata(&sibling.id)
            .map_err(map_repository_error)?
        {
            exports.insert(sibling.id.clone(), snapshot);
        }
    }
    Ok(exports)
}

#[derive(Debug, Clone)]
struct FilesystemFindings {
    missing_files: Vec<PathBuf>,
    artwork_missing: Option<String>,
}

fn inspect_filesystem(
    managed_paths: &[PathBuf],
    artwork_file_name: Option<&str>,
) -> FilesystemFindings {
    let missing_files = managed_paths
        .iter()
        .filter(|path| !path.is_file())
        .cloned()
        .collect::<Vec<_>>();
    let artwork_missing = artwork_file_name.and_then(|name| {
        if managed_paths.is_empty() {
            return Some(name.to_string());
        }
        let parent = managed_paths
            .iter()
            .filter_map(|path| path.parent().map(Path::to_path_buf))
            .collect::<BTreeSet<_>>();
        if parent.len() != 1 {
            return Some(name.to_string());
        }
        let artwork_path = parent
            .iter()
            .next()
            .expect("parent should exist")
            .join(name);
        (!artwork_path.is_file()).then(|| name.to_string())
    });

    FilesystemFindings {
        missing_files,
        artwork_missing,
    }
}

fn find_path_conflicts(
    release_instance: &ReleaseInstance,
    current_snapshot: &ExportedMetadataSnapshot,
    siblings: &[ReleaseInstance],
    sibling_exports: &HashMap<ReleaseInstanceId, ExportedMetadataSnapshot>,
) -> Vec<String> {
    siblings
        .iter()
        .filter_map(|sibling| {
            let sibling_snapshot = sibling_exports.get(&sibling.id)?;
            if !instances_are_distinct(release_instance, sibling) {
                return None;
            }
            (current_snapshot.path_components == sibling_snapshot.path_components).then(|| {
                format!(
                    "release instance {} shares managed path components with {}",
                    release_instance.id.as_uuid(),
                    sibling.id.as_uuid()
                )
            })
        })
        .collect()
}

fn find_visibility_conflicts(
    release_instance: &ReleaseInstance,
    current_snapshot: &ExportedMetadataSnapshot,
    siblings: &[ReleaseInstance],
    sibling_exports: &HashMap<ReleaseInstanceId, ExportedMetadataSnapshot>,
) -> Vec<String> {
    siblings
        .iter()
        .filter_map(|sibling| {
            let sibling_snapshot = sibling_exports.get(&sibling.id)?;
            if !instances_are_distinct(release_instance, sibling) {
                return None;
            }
            let same_visible_identity = current_snapshot.album_title
                == sibling_snapshot.album_title
                && current_snapshot.album_artist == sibling_snapshot.album_artist
                && current_snapshot.artist_credits == sibling_snapshot.artist_credits;
            same_visible_identity.then(|| {
                format!(
                    "release instance {} renders the same player-visible album identity as {}",
                    release_instance.id.as_uuid(),
                    sibling.id.as_uuid()
                )
            })
        })
        .collect()
}

fn find_compatibility_failures(
    release_instance: &ReleaseInstance,
    canonical_tracks: &[crate::domain::track::Track],
    track_instances: &[crate::domain::track_instance::TrackInstance],
    managed_files: &[crate::domain::file::FileRecord],
    filesystem: &FilesystemFindings,
) -> Vec<String> {
    let mut failures = Vec::new();
    if track_instances.len() != canonical_tracks.len() {
        failures.push(format!(
            "release instance {} has {} track instances for {} canonical tracks",
            release_instance.id.as_uuid(),
            track_instances.len(),
            canonical_tracks.len()
        ));
    }
    if managed_files.len() != canonical_tracks.len() {
        failures.push(format!(
            "release instance {} has {} managed files for {} canonical tracks",
            release_instance.id.as_uuid(),
            managed_files.len(),
            canonical_tracks.len()
        ));
    }

    let mut files_by_track = HashMap::<_, usize>::new();
    for file in managed_files {
        *files_by_track
            .entry(file.track_instance_id.clone())
            .or_default() += 1;
    }
    for track_instance in track_instances {
        let file_count = files_by_track
            .get(&track_instance.id)
            .copied()
            .unwrap_or_default();
        if file_count != 1 {
            failures.push(format!(
                "track instance {} has {} managed files instead of 1",
                track_instance.id.as_uuid(),
                file_count
            ));
        }
    }

    if !filesystem.missing_files.is_empty() {
        failures.push(format!(
            "release instance {} is missing managed files on disk: {}",
            release_instance.id.as_uuid(),
            filesystem
                .missing_files
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(artwork) = &filesystem.artwork_missing {
        failures.push(format!(
            "release instance {} expected managed artwork {} but it was not found",
            release_instance.id.as_uuid(),
            artwork
        ));
    }
    failures
}

fn instances_are_distinct(left: &ReleaseInstance, right: &ReleaseInstance) -> bool {
    left.release_id != right.release_id
        || left.source_id != right.source_id
        || left.technical_variant != right.technical_variant
}

fn render_issue_details(prefix: &str, findings: &[String]) -> String {
    let mut details = prefix.to_string();
    for finding in findings {
        details.push_str("\n- ");
        details.push_str(finding);
    }
    details
}

fn synchronize_issue<R>(
    repository: &R,
    release_instance: &ReleaseInstance,
    issue_type: IssueType,
    desired: Option<(String, String)>,
    changed_at_unix_seconds: i64,
) -> Result<(), CompatibilityVerificationError>
where
    R: IssueCommandRepository + IssueRepository,
{
    let subject = IssueSubject::ReleaseInstance(release_instance.id.clone());
    let existing = repository
        .list_issues(&IssueListQuery {
            state: Some(IssueState::Open),
            issue_type: Some(issue_type.clone()),
            subject: Some(subject.clone()),
            page: PageRequest::new(20, 0),
        })
        .map_err(map_repository_error)?;

    match desired {
        Some((summary, details)) => {
            if let Some(mut issue) = existing.items.into_iter().next() {
                if issue.summary != summary || issue.details != Some(details.clone()) {
                    issue.summary = summary;
                    issue.details = Some(details);
                    repository
                        .update_issue(&issue)
                        .map_err(map_repository_error)?;
                }
            } else {
                repository
                    .create_issue(&Issue::open(
                        issue_type,
                        subject,
                        summary,
                        Some(details),
                        changed_at_unix_seconds,
                    ))
                    .map_err(map_repository_error)?;
            }
        }
        None => {
            for mut issue in existing.items {
                issue
                    .resolve(changed_at_unix_seconds)
                    .map_err(map_issue_lifecycle_error)?;
                repository
                    .update_issue(&issue)
                    .map_err(map_repository_error)?;
            }
        }
    }

    Ok(())
}

fn map_repository_error(error: RepositoryError) -> CompatibilityVerificationError {
    CompatibilityVerificationError {
        kind: match error.kind {
            RepositoryErrorKind::NotFound => CompatibilityVerificationErrorKind::NotFound,
            RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                CompatibilityVerificationErrorKind::Conflict
            }
            RepositoryErrorKind::Storage => CompatibilityVerificationErrorKind::Storage,
        },
        message: error.message,
    }
}

fn map_issue_lifecycle_error(
    error: crate::domain::issue::IssueLifecycleError,
) -> CompatibilityVerificationError {
    CompatibilityVerificationError {
        kind: CompatibilityVerificationErrorKind::Conflict,
        message: match error {
            crate::domain::issue::IssueLifecycleError::AlreadyResolved => {
                "issue is already resolved".to_string()
            }
            crate::domain::issue::IssueLifecycleError::AlreadySuppressed => {
                "issue is already suppressed".to_string()
            }
            crate::domain::issue::IssueLifecycleError::ResolvedIssueCannotBeSuppressed => {
                "resolved issues cannot be suppressed".to_string()
            }
            crate::domain::issue::IssueLifecycleError::SuppressedIssueCannotBeResolved => {
                "suppressed issues cannot be resolved".to_string()
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::application::repository::{ReleaseGroupSearchQuery, ReleaseListQuery};
    use crate::domain::artist::Artist;
    use crate::domain::exported_metadata_snapshot::{CompatibilityReport, QualifierVisibility};
    use crate::domain::file::FileRecord;
    use crate::domain::release::{PartialDate, ReleaseEdition};
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, ProvenanceSnapshot, ReleaseInstanceState, TechnicalVariant,
    };
    use crate::domain::track::{Track, TrackPosition};
    use crate::domain::track_instance::{AudioProperties, TrackInstance};
    use crate::support::ids::{
        ArtistId, ExportedMetadataSnapshotId, FileId, ImportBatchId, IssueId, ReleaseGroupId,
        ReleaseId, SourceId, TrackId, TrackInstanceId,
    };
    use crate::support::pagination::Page;

    #[tokio::test(flavor = "current_thread")]
    async fn verifier_marks_release_instance_verified_when_outputs_are_distinct() {
        let root = test_root("compatibility-verified");
        let repository = InMemoryCompatibilityRepository::new(&root);
        repository.write_managed_audio();
        repository.write_managed_artwork();
        let service = CompatibilityVerificationService::new(repository.clone());

        let report = service
            .verify_release_instance(&repository.current_release_instance.id, 200)
            .await
            .expect("verification should succeed");

        assert!(report.verified);
        assert!(report.issue_types.is_empty());
        assert!(repository.open_issue_types().is_empty());
        assert!(repository.current_snapshot().compatibility.verified);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verifier_opens_distinguishability_issue_for_shared_path_components() {
        let root = test_root("compatibility-path-collision");
        let repository = InMemoryCompatibilityRepository::new(&root);
        repository.write_managed_audio();
        repository.write_managed_artwork();
        repository.make_sibling_share_path_components();
        let service = CompatibilityVerificationService::new(repository.clone());

        let report = service
            .verify_release_instance(&repository.current_release_instance.id, 200)
            .await
            .expect("verification should succeed");

        assert!(!report.verified);
        assert!(
            report
                .issue_types
                .contains(&IssueType::UndistinguishableReleaseInstance)
        );
        assert!(
            repository
                .open_issue_types()
                .contains(&IssueType::UndistinguishableReleaseInstance)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verifier_opens_player_visibility_collision_issue() {
        let root = test_root("compatibility-visibility-collision");
        let repository = InMemoryCompatibilityRepository::new(&root);
        repository.write_managed_audio();
        repository.write_managed_artwork();
        repository.make_sibling_share_visible_identity();
        let service = CompatibilityVerificationService::new(repository.clone());

        let report = service
            .verify_release_instance(&repository.current_release_instance.id, 200)
            .await
            .expect("verification should succeed");

        assert!(!report.verified);
        assert!(
            report
                .issue_types
                .contains(&IssueType::PlayerVisibilityCollision)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verifier_opens_compatibility_failure_issue_for_missing_outputs() {
        let root = test_root("compatibility-missing-outputs");
        let repository = InMemoryCompatibilityRepository::new(&root);
        repository.write_managed_audio();
        let service = CompatibilityVerificationService::new(repository.clone());

        let report = service
            .verify_release_instance(&repository.current_release_instance.id, 200)
            .await
            .expect("verification should succeed");

        assert!(!report.verified);
        assert!(
            report
                .issue_types
                .contains(&IssueType::PlayerCompatibilityFailure)
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("expected managed artwork"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn managed_path_fixture_remains_stable_across_verification_runs() {
        let root = test_root("compatibility-managed-path-stability");
        let repository = InMemoryCompatibilityRepository::new(&root);
        repository.write_managed_audio();
        repository.write_managed_artwork();
        let service = CompatibilityVerificationService::new(repository.clone());

        let first_report = service
            .verify_release_instance(&repository.current_release_instance.id, 200)
            .await
            .expect("first verification should succeed");
        let first_path = repository.current_snapshot().path_components.join("/");
        let second_report = service
            .verify_release_instance(&repository.current_release_instance.id, 201)
            .await
            .expect("second verification should succeed");
        let second_path = repository.current_snapshot().path_components.join("/");

        assert_eq!(
            render_regression_fixture(
                first_report.verified,
                &first_report.issue_types,
                &first_path,
                &second_path,
                &second_report.warnings,
            ),
            include_str!("../../tests/golden/compatibility_managed_path_stability.txt")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn visibility_collision_fixture_matches_expected_summary() {
        let root = test_root("compatibility-visibility-fixture");
        let repository = InMemoryCompatibilityRepository::new(&root);
        repository.write_managed_audio();
        repository.write_managed_artwork();
        repository.make_sibling_share_visible_identity();
        let service = CompatibilityVerificationService::new(repository.clone());

        let report = service
            .verify_release_instance(&repository.current_release_instance.id, 200)
            .await
            .expect("verification should succeed");

        assert_eq!(
            render_visibility_fixture(
                report.verified,
                &report.issue_types,
                &repository.current_snapshot().album_title,
                &repository.sibling_snapshot().album_title,
                &report.warnings,
            ),
            include_str!("../../tests/golden/compatibility_visibility_collision.txt")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[derive(Clone)]
    struct InMemoryCompatibilityRepository {
        release_group: ReleaseGroup,
        current_release: Release,
        sibling_release: Release,
        current_release_instance: ReleaseInstance,
        sibling_release_instance: ReleaseInstance,
        tracks: Arc<Vec<Track>>,
        track_instances: Arc<Vec<TrackInstance>>,
        files: Arc<Vec<FileRecord>>,
        exports: Arc<Mutex<Vec<ExportedMetadataSnapshot>>>,
        issues: Arc<Mutex<Vec<Issue>>>,
    }

    impl InMemoryCompatibilityRepository {
        fn new(root: &Path) -> Self {
            let artist_id = ArtistId::new();
            let release_group = ReleaseGroup {
                id: ReleaseGroupId::new(),
                primary_artist_id: artist_id.clone(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
                musicbrainz_release_group_id: None,
            };
            let current_release = Release {
                id: ReleaseId::new(),
                release_group_id: release_group.id.clone(),
                primary_artist_id: artist_id.clone(),
                title: "Kid A".to_string(),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: ReleaseEdition {
                    edition_title: Some("2000 CD".to_string()),
                    disambiguation: None,
                    country: None,
                    label: None,
                    catalog_number: None,
                    release_date: Some(PartialDate {
                        year: 2000,
                        month: Some(10),
                        day: Some(2),
                    }),
                },
            };
            let sibling_release = Release {
                id: ReleaseId::new(),
                release_group_id: release_group.id.clone(),
                primary_artist_id: artist_id,
                title: "Kid A".to_string(),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: ReleaseEdition {
                    edition_title: Some("2016 Vinyl".to_string()),
                    disambiguation: None,
                    country: None,
                    label: None,
                    catalog_number: None,
                    release_date: Some(PartialDate {
                        year: 2016,
                        month: Some(9),
                        day: Some(23),
                    }),
                },
            };
            let current_release_instance = test_release_instance(
                current_release.id.clone(),
                SourceId::new(),
                root.join("managed/current/01 - Everything in Its Right Place.flac"),
            );
            let sibling_release_instance = test_release_instance(
                sibling_release.id.clone(),
                SourceId::new(),
                root.join("managed/sibling/01 - Everything in Its Right Place.flac"),
            );
            let track = Track {
                id: TrackId::new(),
                release_id: current_release.id.clone(),
                position: TrackPosition {
                    disc_number: 1,
                    track_number: 1,
                },
                title: "Everything in Its Right Place".to_string(),
                musicbrainz_track_id: None,
                duration_ms: Some(240_000),
            };
            let track_instance = TrackInstance {
                id: TrackInstanceId::new(),
                release_instance_id: current_release_instance.id.clone(),
                track_id: track.id.clone(),
                observed_position: TrackPosition {
                    disc_number: 1,
                    track_number: 1,
                },
                observed_title: Some(track.title.clone()),
                audio_properties: AudioProperties {
                    duration_ms: Some(240_000),
                    sample_rate_hz: Some(44_100),
                    bit_depth: Some(16),
                    bitrate_kbps: None,
                    format_family: FormatFamily::Flac,
                },
            };
            let file = FileRecord {
                id: FileId::new(),
                track_instance_id: track_instance.id.clone(),
                role: FileRole::Managed,
                format_family: FormatFamily::Flac,
                path: root.join("managed/current/01 - Everything in Its Right Place.flac"),
                checksum: None,
                size_bytes: 8,
            };
            let exports = vec![
                ExportedMetadataSnapshot {
                    id: ExportedMetadataSnapshotId::new(),
                    release_instance_id: current_release_instance.id.clone(),
                    export_profile: "generic_player".to_string(),
                    album_title: "Kid A [2000 CD]".to_string(),
                    album_artist: "Radiohead".to_string(),
                    artist_credits: vec!["Radiohead".to_string()],
                    edition_visibility: QualifierVisibility::TagsAndPath,
                    technical_visibility: QualifierVisibility::PathOnly,
                    path_components: vec!["Radiohead".to_string(), "Kid A [2000 CD]".to_string()],
                    primary_artwork_filename: Some("cover.jpg".to_string()),
                    compatibility: CompatibilityReport {
                        verified: true,
                        warnings: vec!["initial renderer warning".to_string()],
                    },
                    rendered_at_unix_seconds: 100,
                },
                ExportedMetadataSnapshot {
                    id: ExportedMetadataSnapshotId::new(),
                    release_instance_id: sibling_release_instance.id.clone(),
                    export_profile: "generic_player".to_string(),
                    album_title: "Kid A [2016 Vinyl]".to_string(),
                    album_artist: "Radiohead".to_string(),
                    artist_credits: vec!["Radiohead".to_string()],
                    edition_visibility: QualifierVisibility::TagsAndPath,
                    technical_visibility: QualifierVisibility::PathOnly,
                    path_components: vec![
                        "Radiohead".to_string(),
                        "Kid A [2016 Vinyl]".to_string(),
                    ],
                    primary_artwork_filename: Some("cover.jpg".to_string()),
                    compatibility: CompatibilityReport {
                        verified: true,
                        warnings: Vec::new(),
                    },
                    rendered_at_unix_seconds: 101,
                },
            ];

            Self {
                release_group,
                current_release,
                sibling_release,
                current_release_instance,
                sibling_release_instance,
                tracks: Arc::new(vec![track]),
                track_instances: Arc::new(vec![track_instance]),
                files: Arc::new(vec![file]),
                exports: Arc::new(Mutex::new(exports)),
                issues: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn current_snapshot(&self) -> ExportedMetadataSnapshot {
            self.exports
                .lock()
                .expect("exports should lock")
                .iter()
                .find(|snapshot| snapshot.release_instance_id == self.current_release_instance.id)
                .cloned()
                .expect("current snapshot should exist")
        }

        fn sibling_snapshot(&self) -> ExportedMetadataSnapshot {
            self.exports
                .lock()
                .expect("exports should lock")
                .iter()
                .find(|snapshot| snapshot.release_instance_id == self.sibling_release_instance.id)
                .cloned()
                .expect("sibling snapshot should exist")
        }

        fn open_issue_types(&self) -> Vec<IssueType> {
            self.issues
                .lock()
                .expect("issues should lock")
                .iter()
                .filter(|issue| issue.state == IssueState::Open)
                .map(|issue| issue.issue_type.clone())
                .collect()
        }

        fn make_sibling_share_path_components(&self) {
            let mut exports = self.exports.lock().expect("exports should lock");
            let current = exports[0].path_components.clone();
            exports[1].path_components = current;
        }

        fn make_sibling_share_visible_identity(&self) {
            let mut exports = self.exports.lock().expect("exports should lock");
            exports[1].album_title = exports[0].album_title.clone();
            exports[1].album_artist = exports[0].album_artist.clone();
            exports[1].artist_credits = exports[0].artist_credits.clone();
        }

        fn write_managed_audio(&self) {
            let audio_path = &self.files[0].path;
            fs::create_dir_all(audio_path.parent().expect("audio parent should exist"))
                .expect("audio parent should be created");
            fs::write(audio_path, b"flac-data").expect("audio file should be written");
        }

        fn write_managed_artwork(&self) {
            let snapshot = self.current_snapshot();
            let artwork = self.files[0]
                .path
                .parent()
                .expect("audio parent should exist")
                .join(
                    snapshot
                        .primary_artwork_filename
                        .expect("artwork should exist"),
                );
            fs::write(artwork, b"jpeg-data").expect("artwork should be written");
        }
    }

    impl ReleaseRepository for InMemoryCompatibilityRepository {
        fn find_artist_by_musicbrainz_id(
            &self,
            _musicbrainz_artist_id: &str,
        ) -> Result<Option<Artist>, RepositoryError> {
            Ok(None)
        }

        fn get_release_group(
            &self,
            id: &ReleaseGroupId,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok((self.release_group.id == *id).then_some(self.release_group.clone()))
        }

        fn find_release_group_by_musicbrainz_id(
            &self,
            _musicbrainz_release_group_id: &str,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(None)
        }

        fn get_release(&self, id: &ReleaseId) -> Result<Option<Release>, RepositoryError> {
            Ok(if *id == self.current_release.id {
                Some(self.current_release.clone())
            } else if *id == self.sibling_release.id {
                Some(self.sibling_release.clone())
            } else {
                None
            })
        }

        fn find_release_by_musicbrainz_id(
            &self,
            _musicbrainz_release_id: &str,
        ) -> Result<Option<Release>, RepositoryError> {
            Ok(None)
        }

        fn search_release_groups(
            &self,
            query: &ReleaseGroupSearchQuery,
        ) -> Result<Page<ReleaseGroup>, RepositoryError> {
            Ok(Page {
                items: vec![self.release_group.clone()],
                request: query.page,
                total: 1,
            })
        }

        fn list_releases(
            &self,
            query: &ReleaseListQuery,
        ) -> Result<Page<Release>, RepositoryError> {
            let mut items = vec![self.current_release.clone(), self.sibling_release.clone()];
            if let Some(release_group_id) = &query.release_group_id {
                items.retain(|release| &release.release_group_id == release_group_id);
            }
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }

        fn list_tracks_for_release(
            &self,
            release_id: &ReleaseId,
        ) -> Result<Vec<Track>, RepositoryError> {
            Ok(if *release_id == self.current_release.id {
                self.tracks.as_ref().clone()
            } else {
                Vec::new()
            })
        }
    }

    impl ReleaseInstanceRepository for InMemoryCompatibilityRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(if *id == self.current_release_instance.id {
                Some(self.current_release_instance.clone())
            } else if *id == self.sibling_release_instance.id {
                Some(self.sibling_release_instance.clone())
            } else {
                None
            })
        }

        fn list_release_instances(
            &self,
            query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            let mut items = vec![
                self.current_release_instance.clone(),
                self.sibling_release_instance.clone(),
            ];
            if let Some(release_id) = &query.release_id {
                items.retain(|instance| instance.release_id.as_ref() == Some(release_id));
            }
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(vec![self.current_release_instance.clone()])
        }

        fn list_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            page: &PageRequest,
        ) -> Result<Page<crate::domain::candidate_match::CandidateMatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: *page,
                total: 0,
            })
        }

        fn get_candidate_match(
            &self,
            _id: &crate::support::ids::CandidateMatchId,
        ) -> Result<Option<crate::domain::candidate_match::CandidateMatch>, RepositoryError>
        {
            Ok(None)
        }

        fn list_track_instances_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<TrackInstance>, RepositoryError> {
            Ok(
                if *release_instance_id == self.current_release_instance.id {
                    self.track_instances.as_ref().clone()
                } else {
                    Vec::new()
                },
            )
        }

        fn list_files_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
            role: Option<FileRole>,
        ) -> Result<Vec<FileRecord>, RepositoryError> {
            if *release_instance_id != self.current_release_instance.id {
                return Ok(Vec::new());
            }
            Ok(self
                .files
                .iter()
                .filter(|file| role.as_ref().is_none_or(|expected| &file.role == expected))
                .cloned()
                .collect())
        }
    }

    impl ExportRepository for InMemoryCompatibilityRepository {
        fn get_latest_exported_metadata(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(self
                .exports
                .lock()
                .expect("exports should lock")
                .iter()
                .filter(|snapshot| snapshot.release_instance_id == *release_instance_id)
                .max_by_key(|snapshot| snapshot.rendered_at_unix_seconds)
                .cloned())
        }

        fn list_exported_metadata(
            &self,
            query: &crate::application::repository::ExportedMetadataListQuery,
        ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
            let mut items = self.exports.lock().expect("exports should lock").clone();
            if let Some(release_instance_id) = &query.release_instance_id {
                items.retain(|snapshot| &snapshot.release_instance_id == release_instance_id);
            }
            if let Some(album_title) = &query.album_title {
                items.retain(|snapshot| &snapshot.album_title == album_title);
            }
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }

        fn get_exported_metadata(
            &self,
            id: &ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(self
                .exports
                .lock()
                .expect("exports should lock")
                .iter()
                .find(|snapshot| snapshot.id == *id)
                .cloned())
        }
    }

    impl ExportCommandRepository for InMemoryCompatibilityRepository {
        fn create_exported_metadata_snapshot(
            &self,
            snapshot: &ExportedMetadataSnapshot,
        ) -> Result<(), RepositoryError> {
            self.exports
                .lock()
                .expect("exports should lock")
                .push(snapshot.clone());
            Ok(())
        }

        fn update_exported_metadata_snapshot(
            &self,
            snapshot: &ExportedMetadataSnapshot,
        ) -> Result<(), RepositoryError> {
            let mut exports = self.exports.lock().expect("exports should lock");
            let stored = exports
                .iter_mut()
                .find(|stored| stored.id == snapshot.id)
                .ok_or_else(|| RepositoryError {
                    kind: RepositoryErrorKind::NotFound,
                    message: "snapshot not found".to_string(),
                })?;
            *stored = snapshot.clone();
            Ok(())
        }
    }

    impl IssueRepository for InMemoryCompatibilityRepository {
        fn get_issue(&self, id: &IssueId) -> Result<Option<Issue>, RepositoryError> {
            Ok(self
                .issues
                .lock()
                .expect("issues should lock")
                .iter()
                .find(|issue| issue.id == *id)
                .cloned())
        }

        fn list_issues(&self, query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
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
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl IssueCommandRepository for InMemoryCompatibilityRepository {
        fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("issues should lock")
                .push(issue.clone());
            Ok(())
        }

        fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            let mut issues = self.issues.lock().expect("issues should lock");
            let stored = issues
                .iter_mut()
                .find(|stored| stored.id == issue.id)
                .ok_or_else(|| RepositoryError {
                    kind: RepositoryErrorKind::NotFound,
                    message: "issue not found".to_string(),
                })?;
            *stored = issue.clone();
            Ok(())
        }
    }

    fn test_release_instance(
        release_id: ReleaseId,
        source_id: SourceId,
        source_path: PathBuf,
    ) -> ReleaseInstance {
        ReleaseInstance {
            id: ReleaseInstanceId::new(),
            import_batch_id: ImportBatchId::new(),
            source_id,
            release_id: Some(release_id),
            state: ReleaseInstanceState::Imported,
            technical_variant: TechnicalVariant {
                format_family: FormatFamily::Flac,
                bitrate_mode: BitrateMode::Lossless,
                bitrate_kbps: None,
                sample_rate_hz: Some(44_100),
                bit_depth: Some(16),
                track_count: 1,
                total_duration_seconds: 240,
            },
            provenance: ProvenanceSnapshot {
                ingest_origin: crate::domain::release_instance::IngestOrigin::WatchDirectory,
                original_source_path: source_path.display().to_string(),
                imported_at_unix_seconds: 10,
                gazelle_reference: None,
            },
        }
    }

    fn test_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("discern-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("temp root should exist");
        root
    }

    fn render_regression_fixture(
        verified: bool,
        issue_types: &[IssueType],
        first_path: &str,
        second_path: &str,
        warnings: &[String],
    ) -> String {
        format!(
            "verified={verified}\nissues={}\nfirst_path={first_path}\nsecond_path={second_path}\nwarnings={}\n",
            render_issue_types(issue_types),
            warnings.join("|"),
        )
    }

    fn render_visibility_fixture(
        verified: bool,
        issue_types: &[IssueType],
        current_title: &str,
        sibling_title: &str,
        warnings: &[String],
    ) -> String {
        format!(
            "verified={verified}\nissues={}\ncurrent_title={current_title}\nsibling_title={sibling_title}\nwarning_count={}\nhas_visibility_collision={}\n",
            render_issue_types(issue_types),
            warnings.len(),
            warnings
                .iter()
                .any(|warning| warning.contains("player-visible metadata")),
        )
    }

    fn render_issue_types(issue_types: &[IssueType]) -> String {
        if issue_types.is_empty() {
            return "none".to_string();
        }

        issue_types
            .iter()
            .map(|issue_type| match issue_type {
                IssueType::UndistinguishableReleaseInstance => "undistinguishable_release_instance",
                IssueType::PlayerVisibilityCollision => "player_visibility_collision",
                IssueType::PlayerCompatibilityFailure => "player_compatibility_failure",
                IssueType::AmbiguousReleaseMatch => "ambiguous_release_match",
                IssueType::UnmatchedRelease => "unmatched_release",
                IssueType::ConflictingMetadata => "conflicting_metadata",
                IssueType::InconsistentTrackCount => "inconsistent_track_count",
                IssueType::MissingTracks => "missing_tracks",
                IssueType::CorruptFile => "corrupt_file",
                IssueType::UnsupportedFormat => "unsupported_format",
                IssueType::DuplicateReleaseInstance => "duplicate_release_instance",
                IssueType::MissingArtwork => "missing_artwork",
                IssueType::BrokenTags => "broken_tags",
                IssueType::MultiDiscAmbiguity => "multi_disc_ambiguity",
                IssueType::CompilationArtistAmbiguity => "compilation_artist_ambiguity",
            })
            .collect::<Vec<_>>()
            .join("|")
    }
}
