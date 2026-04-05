use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use tokio::task;

use crate::application::config::{ExportPolicy, PathPolicy, StoragePolicy};
use crate::application::repository::{
    ExportRepository, ImportBatchRepository, ReleaseInstanceCommandRepository,
    ReleaseInstanceRepository, ReleaseRepository, RepositoryError, RepositoryErrorKind,
    SourceRepository, StagingManifestRepository,
};
use crate::domain::file::{FileRecord, FileRole};
use crate::domain::import_batch::ImportMode;
use crate::domain::release::{PartialDate, Release};
use crate::domain::release_instance::{
    BitrateMode, FormatFamily, ReleaseInstance, ReleaseInstanceState,
};
use crate::domain::staging_manifest::StagedReleaseGroup;
use crate::domain::track::Track;
use crate::domain::track_instance::{AudioProperties, TrackInstance};
use crate::support::ids::{FileId, ReleaseInstanceId, TrackInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizationReport {
    pub managed_root: PathBuf,
    pub organized_files: Vec<PathBuf>,
    pub mode: ImportMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizationError {
    pub kind: OrganizationErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrganizationErrorKind {
    NotFound,
    Conflict,
    Storage,
}

pub struct FileOrganizationService<R> {
    repository: R,
}

impl<R> FileOrganizationService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> FileOrganizationService<R>
where
    R: ExportRepository
        + ImportBatchRepository
        + ReleaseInstanceCommandRepository
        + ReleaseInstanceRepository
        + ReleaseRepository
        + SourceRepository
        + StagingManifestRepository,
{
    pub async fn organize_release_instance(
        &self,
        storage: &StoragePolicy,
        export: &ExportPolicy,
        release_instance_id: &ReleaseInstanceId,
    ) -> Result<OrganizationReport, OrganizationError> {
        let release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| OrganizationError {
                kind: OrganizationErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        let batch = self
            .repository
            .get_import_batch(&release_instance.import_batch_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| OrganizationError {
                kind: OrganizationErrorKind::NotFound,
                message: format!(
                    "no import batch found for {}",
                    release_instance.import_batch_id.as_uuid()
                ),
            })?;
        let source = self
            .repository
            .get_source(&release_instance.source_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| OrganizationError {
                kind: OrganizationErrorKind::NotFound,
                message: format!(
                    "no source found for {}",
                    release_instance.source_id.as_uuid()
                ),
            })?;
        let release_id = release_instance
            .release_id
            .clone()
            .ok_or_else(|| OrganizationError {
                kind: OrganizationErrorKind::Conflict,
                message: format!(
                    "release instance {} has no canonical release",
                    release_instance.id.as_uuid()
                ),
            })?;
        let release = self
            .repository
            .get_release(&release_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| OrganizationError {
                kind: OrganizationErrorKind::NotFound,
                message: format!("no release found for {}", release_id.as_uuid()),
            })?;
        let export_snapshot = self
            .repository
            .get_latest_exported_metadata(&release_instance.id)
            .map_err(map_repository_error)?
            .ok_or_else(|| OrganizationError {
                kind: OrganizationErrorKind::NotFound,
                message: format!(
                    "no exported metadata snapshot found for {}",
                    release_instance.id.as_uuid()
                ),
            })?;
        let tracks = self
            .repository
            .list_tracks_for_release(&release.id)
            .map_err(map_repository_error)?;
        let manifests = self
            .repository
            .list_staging_manifests_for_batch(&release_instance.import_batch_id)
            .map_err(map_repository_error)?;
        let existing_track_instances = self
            .repository
            .list_track_instances_for_release_instance(&release_instance.id)
            .map_err(map_repository_error)?;
        let existing_managed_files = self
            .repository
            .list_files_for_release_instance(&release_instance.id, Some(FileRole::Managed))
            .map_err(map_repository_error)?;

        let release_directory = render_release_directory(
            &storage.managed_library_root,
            &export.path_templates,
            &release,
            &release_instance,
            &export_snapshot.album_artist,
            &source.display_name,
        )?;
        let plans = build_file_plans(
            &storage.managed_library_root,
            &release_directory,
            &release_instance,
            &tracks,
            &manifests,
            &export.path_templates,
            &source.display_name,
        )?;
        detect_collisions(&plans)?;

        let managed_root = storage.managed_library_root.clone();
        let existing_paths: BTreeSet<PathBuf> = existing_managed_files
            .iter()
            .map(|file| file.path.clone())
            .collect();
        let mode = batch.mode.clone();
        let verified_files = task::spawn_blocking(move || {
            execute_file_operations(&managed_root, &plans, &existing_paths, &mode)
        })
        .await
        .map_err(|error| OrganizationError {
            kind: OrganizationErrorKind::Storage,
            message: format!("organization task failed to join: {error}"),
        })??;

        let (track_instances, files) = build_persisted_artifacts(
            &release_instance,
            &tracks,
            &verified_files,
            &existing_track_instances,
            &existing_managed_files,
        )?;
        self.repository
            .replace_track_instances_and_files(&release_instance.id, &track_instances, &files)
            .map_err(map_repository_error)?;

        let mut updated_release_instance = release_instance.clone();
        updated_release_instance.state = ReleaseInstanceState::Imported;
        self.repository
            .update_release_instance(&updated_release_instance)
            .map_err(map_repository_error)?;

        Ok(OrganizationReport {
            managed_root: storage.managed_library_root.clone(),
            organized_files: verified_files
                .into_iter()
                .map(|item| item.target_path)
                .collect(),
            mode: batch.mode,
        })
    }
}

#[derive(Debug, Clone)]
struct OrganizationPlan {
    source_path: PathBuf,
    target_path: PathBuf,
    track: Track,
}

#[derive(Debug, Clone)]
struct VerifiedFile {
    source_path: PathBuf,
    target_path: PathBuf,
    track: Track,
    size_bytes: u64,
}

fn build_file_plans(
    managed_root: &Path,
    release_directory: &Path,
    release_instance: &ReleaseInstance,
    tracks: &[Track],
    manifests: &[crate::domain::staging_manifest::StagingManifest],
    path_policy: &PathPolicy,
    source_name: &str,
) -> Result<Vec<OrganizationPlan>, OrganizationError> {
    let mut ordered_tracks = tracks.to_vec();
    ordered_tracks.sort_by_key(|track| (track.position.disc_number, track.position.track_number));
    let group = resolve_manifest_group(release_instance, manifests, ordered_tracks.len())?;
    let mut source_paths = group.file_paths.clone();
    source_paths.sort();
    if source_paths.len() != ordered_tracks.len() {
        return Err(OrganizationError {
            kind: OrganizationErrorKind::Conflict,
            message: format!(
                "release instance {} had {} files but {} canonical tracks",
                release_instance.id.as_uuid(),
                source_paths.len(),
                ordered_tracks.len()
            ),
        });
    }

    let mut targets = Vec::new();
    for (source_path, track) in source_paths.into_iter().zip(ordered_tracks) {
        let extension = source_path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_else(|| {
                extension_for_format(&release_instance.technical_variant.format_family)
            });
        let relative = render_track_relative_path(track_file_name(
            &track,
            release_instance.technical_variant.track_count,
            extension,
            &path_policy.character_replacement,
        ));
        let target_path = managed_root.join(release_directory).join(relative);
        assert_target_under_root(managed_root, &target_path, source_name)?;
        targets.push(OrganizationPlan {
            source_path,
            target_path,
            track,
        });
    }
    Ok(targets)
}

fn detect_collisions(plans: &[OrganizationPlan]) -> Result<(), OrganizationError> {
    let mut seen = BTreeSet::new();
    for plan in plans {
        if !seen.insert(plan.target_path.clone()) {
            return Err(OrganizationError {
                kind: OrganizationErrorKind::Conflict,
                message: format!(
                    "managed path collision detected at {}",
                    plan.target_path.display()
                ),
            });
        }
    }
    Ok(())
}

fn execute_file_operations(
    managed_root: &Path,
    plans: &[OrganizationPlan],
    existing_paths: &BTreeSet<PathBuf>,
    mode: &ImportMode,
) -> Result<Vec<VerifiedFile>, OrganizationError> {
    let mut verified = Vec::with_capacity(plans.len());
    for plan in plans {
        if let Some(parent) = plan.target_path.parent() {
            fs::create_dir_all(parent).map_err(|error| OrganizationError {
                kind: OrganizationErrorKind::Storage,
                message: format!("failed to create {}: {error}", parent.display()),
            })?;
        }

        let target_exists = plan.target_path.exists();
        if target_exists && !existing_paths.contains(&plan.target_path) {
            return Err(OrganizationError {
                kind: OrganizationErrorKind::Conflict,
                message: format!(
                    "managed path collision detected at {}",
                    plan.target_path.display()
                ),
            });
        }

        if !target_exists {
            match mode {
                ImportMode::Copy => {
                    fs::copy(&plan.source_path, &plan.target_path).map_err(|error| {
                        OrganizationError {
                            kind: OrganizationErrorKind::Storage,
                            message: format!(
                                "failed to copy {} to {}: {error}",
                                plan.source_path.display(),
                                plan.target_path.display()
                            ),
                        }
                    })?;
                }
                ImportMode::Hardlink => {
                    fs::hard_link(&plan.source_path, &plan.target_path).map_err(|error| {
                        OrganizationError {
                            kind: OrganizationErrorKind::Storage,
                            message: format!(
                                "failed to hardlink {} to {}: {error}",
                                plan.source_path.display(),
                                plan.target_path.display()
                            ),
                        }
                    })?;
                }
                ImportMode::Move => {
                    if let Err(error) = fs::rename(&plan.source_path, &plan.target_path) {
                        if error.raw_os_error() == Some(18) {
                            fs::copy(&plan.source_path, &plan.target_path).map_err(|copy_error| {
                                OrganizationError {
                                    kind: OrganizationErrorKind::Storage,
                                    message: format!(
                                        "failed to copy {} to {} during cross-device move: {copy_error}",
                                        plan.source_path.display(),
                                        plan.target_path.display()
                                    ),
                                }
                            })?;
                            fs::remove_file(&plan.source_path).map_err(|remove_error| {
                                OrganizationError {
                                    kind: OrganizationErrorKind::Storage,
                                    message: format!(
                                        "failed to remove {} after cross-device move: {remove_error}",
                                        plan.source_path.display()
                                    ),
                                }
                            })?;
                        } else {
                            return Err(OrganizationError {
                                kind: OrganizationErrorKind::Storage,
                                message: format!(
                                    "failed to move {} to {}: {error}",
                                    plan.source_path.display(),
                                    plan.target_path.display()
                                ),
                            });
                        }
                    }
                }
            }
        }

        assert_target_under_root(managed_root, &plan.target_path, "managed_root")?;
        let metadata = fs::metadata(&plan.target_path).map_err(|error| OrganizationError {
            kind: OrganizationErrorKind::Storage,
            message: format!(
                "failed to verify organized file {}: {error}",
                plan.target_path.display()
            ),
        })?;
        if !metadata.is_file() {
            return Err(OrganizationError {
                kind: OrganizationErrorKind::Conflict,
                message: format!(
                    "organized path {} was not a file",
                    plan.target_path.display()
                ),
            });
        }

        verified.push(VerifiedFile {
            source_path: plan.source_path.clone(),
            target_path: plan.target_path.clone(),
            track: plan.track.clone(),
            size_bytes: metadata.len(),
        });
    }
    Ok(verified)
}

fn build_persisted_artifacts(
    release_instance: &ReleaseInstance,
    tracks: &[Track],
    verified_files: &[VerifiedFile],
    existing_track_instances: &[TrackInstance],
    existing_managed_files: &[FileRecord],
) -> Result<(Vec<TrackInstance>, Vec<FileRecord>), OrganizationError> {
    let mut existing_track_ids = HashMap::new();
    for track_instance in existing_track_instances {
        existing_track_ids.insert(track_instance.track_id.clone(), track_instance.id.clone());
    }
    let mut existing_managed_ids = HashMap::new();
    for file in existing_managed_files {
        existing_managed_ids.insert(file.path.clone(), file.id.clone());
    }

    let mut track_instances = Vec::new();
    let mut files = Vec::new();
    let tracks_by_id: HashMap<_, _> = tracks
        .iter()
        .map(|track| (track.id.clone(), track))
        .collect();

    for verified in verified_files {
        let track = tracks_by_id
            .get(&verified.track.id)
            .ok_or_else(|| OrganizationError {
                kind: OrganizationErrorKind::Conflict,
                message: format!(
                    "missing canonical track {} during persistence",
                    verified.track.id.as_uuid()
                ),
            })?;
        let track_instance_id = existing_track_ids
            .get(&track.id)
            .cloned()
            .unwrap_or_else(TrackInstanceId::new);
        track_instances.push(TrackInstance {
            id: track_instance_id.clone(),
            release_instance_id: release_instance.id.clone(),
            track_id: track.id.clone(),
            observed_position: track.position.clone(),
            observed_title: Some(track.title.clone()),
            audio_properties: AudioProperties {
                format_family: release_instance.technical_variant.format_family.clone(),
                duration_ms: track.duration_ms,
                bitrate_kbps: release_instance.technical_variant.bitrate_kbps,
                sample_rate_hz: release_instance.technical_variant.sample_rate_hz,
                bit_depth: release_instance.technical_variant.bit_depth,
            },
        });
        files.push(FileRecord {
            id: FileId::new(),
            track_instance_id: track_instance_id.clone(),
            role: FileRole::Source,
            format_family: release_instance.technical_variant.format_family.clone(),
            path: verified.source_path.clone(),
            checksum: None,
            size_bytes: verified.size_bytes,
        });
        files.push(FileRecord {
            id: existing_managed_ids
                .get(&verified.target_path)
                .cloned()
                .unwrap_or_else(FileId::new),
            track_instance_id,
            role: FileRole::Managed,
            format_family: release_instance.technical_variant.format_family.clone(),
            path: verified.target_path.clone(),
            checksum: None,
            size_bytes: verified.size_bytes,
        });
    }

    Ok((track_instances, files))
}

fn render_release_directory(
    managed_root: &Path,
    path_policy: &PathPolicy,
    release: &Release,
    release_instance: &ReleaseInstance,
    album_artist: &str,
    source_name: &str,
) -> Result<PathBuf, OrganizationError> {
    let release_components = render_template_components(
        &path_policy.release_template,
        &render_template_values(release, release_instance, album_artist, source_name),
        &path_policy.character_replacement,
    );
    let instance_components = render_template_components(
        &path_policy.release_instance_template,
        &render_template_values(release, release_instance, album_artist, source_name),
        &path_policy.character_replacement,
    );
    let relative = release_components
        .into_iter()
        .chain(instance_components)
        .collect::<PathBuf>();
    let full = managed_root.join(&relative);
    if full.to_string_lossy().len() > path_policy.max_path_length {
        return Err(OrganizationError {
            kind: OrganizationErrorKind::Conflict,
            message: format!(
                "managed release path exceeded max length at {}",
                full.display()
            ),
        });
    }
    Ok(relative)
}

fn render_template_values(
    release: &Release,
    release_instance: &ReleaseInstance,
    album_artist: &str,
    source_name: &str,
) -> BTreeMap<&'static str, String> {
    let mut values = BTreeMap::new();
    values.insert("album_artist", album_artist.to_string());
    values.insert("release_title", release.title.clone());
    values.insert(
        "release_year",
        release
            .edition
            .release_date
            .as_ref()
            .map(|date| date.year.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
    );
    values.insert(
        "edition_label",
        default_edition_label(release).unwrap_or_else(|| "edition".to_string()),
    );
    values.insert(
        "format_family",
        match release_instance.technical_variant.format_family {
            FormatFamily::Flac => "FLAC".to_string(),
            FormatFamily::Mp3 => "MP3".to_string(),
        },
    );
    values.insert(
        "bitrate_mode",
        match release_instance.technical_variant.bitrate_mode {
            BitrateMode::Constant => "constant".to_string(),
            BitrateMode::Variable => "variable".to_string(),
            BitrateMode::Lossless => "lossless".to_string(),
        },
    );
    values.insert(
        "bitrate_kbps",
        release_instance
            .technical_variant
            .bitrate_kbps
            .map(|value| value.to_string())
            .unwrap_or_else(|| "na".to_string()),
    );
    values.insert(
        "sample_rate_hz",
        release_instance
            .technical_variant
            .sample_rate_hz
            .map(|value| value.to_string())
            .unwrap_or_else(|| "na".to_string()),
    );
    values.insert(
        "bit_depth",
        release_instance
            .technical_variant
            .bit_depth
            .map(|value| value.to_string())
            .unwrap_or_else(|| "na".to_string()),
    );
    values.insert("source_name", source_name.to_string());
    values
}

fn render_template_components(
    template: &str,
    values: &BTreeMap<&'static str, String>,
    replacement: &str,
) -> Vec<String> {
    let mut rendered = template.to_string();
    for (placeholder, value) in values {
        rendered = rendered.replace(&format!("{{{placeholder}}}"), value);
    }
    rendered
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| sanitize_component(segment, replacement))
        .collect()
}

fn sanitize_component(value: &str, replacement: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_ascii_control()
            || matches!(
                character,
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|'
            )
        {
            sanitized.push_str(replacement);
        } else {
            sanitized.push(character);
        }
    }
    let sanitized = sanitized.trim().trim_matches('.').to_string();
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn track_file_name(track: &Track, total_tracks: u16, extension: &str, replacement: &str) -> String {
    let prefix = if total_tracks > 99 || track.position.disc_number > 1 {
        format!(
            "{}-{:02}",
            track.position.disc_number, track.position.track_number
        )
    } else {
        format!("{:02}", track.position.track_number)
    };
    let title = sanitize_component(&track.title, replacement);
    format!("{prefix} - {title}.{extension}")
}

fn render_track_relative_path(file_name: String) -> PathBuf {
    PathBuf::from(file_name)
}

fn extension_for_format(format_family: &FormatFamily) -> &'static str {
    match format_family {
        FormatFamily::Flac => "flac",
        FormatFamily::Mp3 => "mp3",
    }
}

fn assert_target_under_root(
    managed_root: &Path,
    target_path: &Path,
    label: &str,
) -> Result<(), OrganizationError> {
    if !target_path.starts_with(managed_root) {
        return Err(OrganizationError {
            kind: OrganizationErrorKind::Conflict,
            message: format!("rendered {label} path escaped managed root"),
        });
    }
    Ok(())
}

fn resolve_manifest_group<'a>(
    release_instance: &ReleaseInstance,
    manifests: &'a [crate::domain::staging_manifest::StagingManifest],
    expected_tracks: usize,
) -> Result<&'a StagedReleaseGroup, OrganizationError> {
    let expected_extension = match release_instance.technical_variant.format_family {
        FormatFamily::Flac => "flac",
        FormatFamily::Mp3 => "mp3",
    };
    let expected_root = Path::new(&release_instance.provenance.original_source_path);
    let candidates: Vec<_> = manifests
        .iter()
        .flat_map(|manifest| manifest.grouping.groups.iter())
        .filter(|group| group.file_paths.len() == expected_tracks)
        .filter(|group| {
            group.file_paths.iter().all(|path| {
                path.extension()
                    .and_then(|value| value.to_str())
                    .map(|value| value.eq_ignore_ascii_case(expected_extension))
                    .unwrap_or(false)
            })
        })
        .collect();

    if candidates.is_empty() {
        return Err(OrganizationError {
            kind: OrganizationErrorKind::Conflict,
            message: format!(
                "no staged {} group matched release instance {}",
                expected_extension,
                release_instance.id.as_uuid()
            ),
        });
    }

    let rooted: Vec<_> = candidates
        .iter()
        .copied()
        .filter(|group| {
            group
                .file_paths
                .first()
                .and_then(|path| path.parent())
                .map(|parent| parent == expected_root || expected_root.starts_with(parent))
                .unwrap_or(false)
        })
        .collect();

    match rooted.as_slice() {
        [group] => Ok(*group),
        [] => match candidates.as_slice() {
            [group] => Ok(*group),
            _ => Err(OrganizationError {
                kind: OrganizationErrorKind::Conflict,
                message: format!(
                    "multiple staged groups matched release instance {}",
                    release_instance.id.as_uuid()
                ),
            }),
        },
        _ => Err(OrganizationError {
            kind: OrganizationErrorKind::Conflict,
            message: format!(
                "multiple staged roots matched release instance {}",
                release_instance.id.as_uuid()
            ),
        }),
    }
}

fn default_edition_label(release: &Release) -> Option<String> {
    release.edition.edition_title.clone().or_else(|| {
        release
            .edition
            .release_date
            .as_ref()
            .map(render_partial_date)
    })
}

fn render_partial_date(date: &PartialDate) -> String {
    match (date.month, date.day) {
        (Some(month), Some(day)) => format!("{:04}-{:02}-{:02}", date.year, month, day),
        (Some(month), None) => format!("{:04}-{:02}", date.year, month),
        _ => date.year.to_string(),
    }
}

fn map_repository_error(error: RepositoryError) -> OrganizationError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => OrganizationErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            OrganizationErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => OrganizationErrorKind::Storage,
    };
    OrganizationError {
        kind,
        message: error.message,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::application::config::ValidatedRuntimeConfig;
    use crate::application::repository::{
        ExportedMetadataListQuery, ImportBatchListQuery, ReleaseGroupSearchQuery,
        ReleaseInstanceListQuery, ReleaseListQuery,
    };
    use crate::domain::artist::Artist;
    use crate::domain::exported_metadata_snapshot::{
        CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
    };
    use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus};
    use crate::domain::release::{PartialDate, ReleaseEdition};
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::source::{Source, SourceKind, SourceLocator};
    use crate::domain::staging_manifest::{
        GroupingDecision, GroupingStrategy, StagedReleaseGroup, StagingManifest,
        StagingManifestSource,
    };
    use crate::support::ids::{
        ArtistId, ExportedMetadataSnapshotId, ImportBatchId, MusicBrainzReleaseGroupId,
        MusicBrainzReleaseId, ReleaseGroupId, ReleaseId, SourceId, StagingManifestId, TrackId,
    };
    use crate::support::pagination::{Page, PageRequest};

    #[tokio::test(flavor = "current_thread")]
    async fn service_organizes_tracks_into_deterministic_paths() {
        let temp_root = test_root("organize-copy");
        let source_root = temp_root.join("incoming");
        fs::create_dir_all(&source_root).expect("source root should exist");
        let source_path = source_root.join("01 - Track.flac");
        fs::write(&source_path, b"flac-data").expect("source file should exist");

        let repository =
            InMemoryOrganizationRepository::new(ImportMode::Copy, vec![source_path.clone()]);
        let mut config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        config.storage.managed_library_root = temp_root.join("managed");
        let service = FileOrganizationService::new(repository.clone());

        let report = service
            .organize_release_instance(
                &config.storage,
                &config.export,
                &repository.release_instance.id,
            )
            .await
            .expect("organization should succeed");

        assert_eq!(report.mode, ImportMode::Copy);
        let target = report.organized_files[0].clone();
        assert!(target.exists());
        assert!(source_path.exists());
        assert!(target.ends_with(Path::new(
            "Radiohead/Kid A/2000 - 2000-10-02/FLAC-lossless-na-44100-16/Incoming/01 - Everything in Its Right Place.flac"
        )));

        let stored_tracks = repository
            .list_track_instances_for_release_instance(&repository.release_instance.id)
            .expect("track instances should load");
        assert_eq!(stored_tracks.len(), 1);
        let stored_files = repository
            .list_files_for_release_instance(
                &repository.release_instance.id,
                Some(FileRole::Managed),
            )
            .expect("managed files should load");
        assert_eq!(stored_files.len(), 1);
        assert_eq!(stored_files[0].path, target);

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn service_detects_collisions_before_mutation() {
        let temp_root = test_root("organize-collision");
        let source_root = temp_root.join("incoming");
        let managed_root = temp_root.join("managed");
        fs::create_dir_all(&source_root).expect("source root should exist");
        fs::create_dir_all(&managed_root).expect("managed root should exist");
        let source_path = source_root.join("01 - Track.flac");
        fs::write(&source_path, b"flac-data").expect("source file should exist");

        let mut config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        config.storage.managed_library_root = managed_root.clone();
        let collision_path = managed_root.join(
            "Radiohead/Kid A/2000 - 2000-10-02/FLAC-lossless-na-44100-16/Incoming/01 - Everything in Its Right Place.flac",
        );
        fs::create_dir_all(collision_path.parent().expect("parent should exist"))
            .expect("collision parent should exist");
        fs::write(&collision_path, b"other").expect("collision file should exist");

        let service = FileOrganizationService::new(InMemoryOrganizationRepository::new(
            ImportMode::Copy,
            vec![source_path.clone()],
        ));
        let error = service
            .organize_release_instance(
                &config.storage,
                &config.export,
                &service.repository.release_instance.id,
            )
            .await
            .expect_err("collision should fail");
        assert_eq!(error.kind, OrganizationErrorKind::Conflict);
        assert!(source_path.exists());

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn service_moves_files_when_requested() {
        let temp_root = test_root("organize-move");
        let source_root = temp_root.join("incoming");
        fs::create_dir_all(&source_root).expect("source root should exist");
        let source_path = source_root.join("01 - Track.mp3");
        fs::write(&source_path, b"mp3-data").expect("source file should exist");

        let repository =
            InMemoryOrganizationRepository::new(ImportMode::Move, vec![source_path.clone()]);
        let mut config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        config.storage.managed_library_root = temp_root.join("managed");
        let service = FileOrganizationService::new(repository);

        let report = service
            .organize_release_instance(
                &config.storage,
                &config.export,
                &service.repository.release_instance.id,
            )
            .await
            .expect("move organization should succeed");

        assert!(report.organized_files[0].exists());
        assert!(!source_path.exists());

        let _ = fs::remove_dir_all(temp_root);
    }

    #[derive(Clone)]
    struct InMemoryOrganizationRepository {
        batch: ImportBatch,
        source: Source,
        release_group: ReleaseGroup,
        release: Release,
        release_instance: ReleaseInstance,
        tracks: Arc<Vec<Track>>,
        manifests: Arc<Vec<StagingManifest>>,
        exports: Arc<Vec<ExportedMetadataSnapshot>>,
        track_instances: Arc<Mutex<Vec<TrackInstance>>>,
        files: Arc<Mutex<Vec<FileRecord>>>,
    }

    impl InMemoryOrganizationRepository {
        fn new(mode: ImportMode, file_paths: Vec<PathBuf>) -> Self {
            let artist_id = ArtistId::new();
            let source = Source {
                id: SourceId::new(),
                kind: SourceKind::WatchDirectory,
                display_name: "Incoming".to_string(),
                locator: SourceLocator::FilesystemPath(
                    file_paths
                        .first()
                        .and_then(|path| path.parent())
                        .unwrap_or_else(|| Path::new("/tmp"))
                        .to_path_buf(),
                ),
                external_reference: None,
            };
            let batch = ImportBatch {
                id: ImportBatchId::new(),
                source_id: source.id.clone(),
                mode,
                status: ImportBatchStatus::Submitted,
                requested_by: BatchRequester::System,
                created_at_unix_seconds: 1,
                received_paths: vec![
                    file_paths
                        .first()
                        .and_then(|path| path.parent())
                        .unwrap_or_else(|| Path::new("/tmp"))
                        .to_path_buf(),
                ],
            };
            let release_group = ReleaseGroup {
                id: ReleaseGroupId::new(),
                primary_artist_id: artist_id.clone(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
                musicbrainz_release_group_id: MusicBrainzReleaseGroupId::parse_str(
                    "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
                )
                .ok(),
            };
            let release = Release {
                id: ReleaseId::new(),
                release_group_id: release_group.id.clone(),
                primary_artist_id: artist_id,
                title: "Kid A".to_string(),
                musicbrainz_release_id: MusicBrainzReleaseId::parse_str(
                    "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb",
                )
                .ok(),
                discogs_release_id: None,
                edition: ReleaseEdition {
                    edition_title: None,
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
            let format_family = match file_paths
                .first()
                .and_then(|path| path.extension())
                .and_then(|value| value.to_str())
                .unwrap_or("flac")
            {
                "mp3" => FormatFamily::Mp3,
                _ => FormatFamily::Flac,
            };
            let release_instance = ReleaseInstance {
                id: ReleaseInstanceId::new(),
                import_batch_id: batch.id.clone(),
                source_id: source.id.clone(),
                release_id: Some(release.id.clone()),
                state: ReleaseInstanceState::Tagging,
                technical_variant: crate::domain::release_instance::TechnicalVariant {
                    format_family: format_family.clone(),
                    bitrate_mode: if matches!(format_family, FormatFamily::Mp3) {
                        BitrateMode::Variable
                    } else {
                        BitrateMode::Lossless
                    },
                    bitrate_kbps: if matches!(format_family, FormatFamily::Mp3) {
                        Some(320)
                    } else {
                        None
                    },
                    sample_rate_hz: Some(44_100),
                    bit_depth: if matches!(format_family, FormatFamily::Flac) {
                        Some(16)
                    } else {
                        None
                    },
                    track_count: file_paths.len() as u16,
                    total_duration_seconds: 240,
                },
                provenance: crate::domain::release_instance::ProvenanceSnapshot {
                    ingest_origin: crate::domain::release_instance::IngestOrigin::WatchDirectory,
                    original_source_path: file_paths
                        .first()
                        .and_then(|path| path.parent())
                        .unwrap_or_else(|| Path::new("/tmp"))
                        .display()
                        .to_string(),
                    imported_at_unix_seconds: 2,
                    gazelle_reference: None,
                },
            };
            let track = Track {
                id: TrackId::new(),
                release_id: release.id.clone(),
                position: crate::domain::track::TrackPosition {
                    disc_number: 1,
                    track_number: 1,
                },
                title: "Everything in Its Right Place".to_string(),
                musicbrainz_track_id: None,
                duration_ms: Some(240_000),
            };
            let manifests = vec![StagingManifest {
                id: StagingManifestId::new(),
                batch_id: batch.id.clone(),
                source: StagingManifestSource {
                    kind: SourceKind::WatchDirectory,
                    source_path: file_paths
                        .first()
                        .and_then(|path| path.parent())
                        .unwrap_or_else(|| Path::new("/tmp"))
                        .to_path_buf(),
                },
                discovered_files: Vec::new(),
                auxiliary_files: Vec::new(),
                grouping: GroupingDecision {
                    strategy: GroupingStrategy::CommonParentDirectory,
                    groups: vec![StagedReleaseGroup {
                        key: "album".to_string(),
                        file_paths,
                        auxiliary_paths: Vec::new(),
                    }],
                    notes: Vec::new(),
                },
                captured_at_unix_seconds: 3,
            }];
            let exports = vec![ExportedMetadataSnapshot {
                id: ExportedMetadataSnapshotId::new(),
                release_instance_id: release_instance.id.clone(),
                export_profile: "generic_player".to_string(),
                album_title: "Kid A [2000]".to_string(),
                album_artist: "Radiohead".to_string(),
                artist_credits: vec!["Radiohead".to_string()],
                edition_visibility: QualifierVisibility::TagsAndPath,
                technical_visibility: QualifierVisibility::PathOnly,
                path_components: vec!["Radiohead".to_string(), "Kid A [2000]".to_string()],
                primary_artwork_filename: Some("cover.jpg".to_string()),
                compatibility: CompatibilityReport {
                    verified: true,
                    warnings: Vec::new(),
                },
                rendered_at_unix_seconds: 4,
            }];

            Self {
                batch,
                source,
                release_group,
                release,
                release_instance,
                tracks: Arc::new(vec![track]),
                manifests: Arc::new(manifests),
                exports: Arc::new(exports),
                track_instances: Arc::new(Mutex::new(Vec::new())),
                files: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl ReleaseRepository for InMemoryOrganizationRepository {
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
            Ok(Page {
                items: vec![self.release.clone()],
                request: query.page,
                total: 1,
            })
        }
        fn list_tracks_for_release(
            &self,
            release_id: &ReleaseId,
        ) -> Result<Vec<Track>, RepositoryError> {
            Ok(if *release_id == self.release.id {
                self.tracks.as_ref().clone()
            } else {
                Vec::new()
            })
        }
    }

    impl ReleaseInstanceRepository for InMemoryOrganizationRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok((self.release_instance.id == *id).then_some(self.release_instance.clone()))
        }
        fn list_release_instances(
            &self,
            query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            Ok(Page {
                items: vec![self.release_instance.clone()],
                request: query.page,
                total: 1,
            })
        }
        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(vec![self.release_instance.clone()])
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
            Ok(if *release_instance_id == self.release_instance.id {
                self.track_instances
                    .lock()
                    .expect("track instances should lock")
                    .clone()
            } else {
                Vec::new()
            })
        }
        fn list_files_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
            role: Option<FileRole>,
        ) -> Result<Vec<FileRecord>, RepositoryError> {
            if *release_instance_id != self.release_instance.id {
                return Ok(Vec::new());
            }
            Ok(self
                .files
                .lock()
                .expect("files should lock")
                .iter()
                .filter(|file| role.as_ref().is_none_or(|expected| &file.role == expected))
                .cloned()
                .collect())
        }
    }

    impl ReleaseInstanceCommandRepository for InMemoryOrganizationRepository {
        fn create_release_instance(
            &self,
            _release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
        fn update_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            drop(
                self.track_instances
                    .lock()
                    .expect("track instances should lock"),
            );
            if release_instance.id != self.release_instance.id {
                return Err(RepositoryError {
                    kind: RepositoryErrorKind::NotFound,
                    message: "release instance not found".to_string(),
                });
            }
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
            release_instance_id: &ReleaseInstanceId,
            track_instances: &[TrackInstance],
            files: &[FileRecord],
        ) -> Result<(), RepositoryError> {
            if *release_instance_id != self.release_instance.id {
                return Err(RepositoryError {
                    kind: RepositoryErrorKind::NotFound,
                    message: "release instance not found".to_string(),
                });
            }
            *self
                .track_instances
                .lock()
                .expect("track instances should lock") = track_instances.to_vec();
            *self.files.lock().expect("files should lock") = files.to_vec();
            Ok(())
        }
    }

    impl ImportBatchRepository for InMemoryOrganizationRepository {
        fn get_import_batch(
            &self,
            id: &ImportBatchId,
        ) -> Result<Option<ImportBatch>, RepositoryError> {
            Ok((self.batch.id == *id).then_some(self.batch.clone()))
        }
        fn list_import_batches(
            &self,
            query: &ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            Ok(Page {
                items: vec![self.batch.clone()],
                request: query.page,
                total: 1,
            })
        }
    }

    impl SourceRepository for InMemoryOrganizationRepository {
        fn get_source(&self, id: &SourceId) -> Result<Option<Source>, RepositoryError> {
            Ok((self.source.id == *id).then_some(self.source.clone()))
        }
        fn find_source_by_locator(
            &self,
            _locator: &SourceLocator,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(None)
        }
    }

    impl StagingManifestRepository for InMemoryOrganizationRepository {
        fn list_staging_manifests_for_batch(
            &self,
            batch_id: &ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(if *batch_id == self.batch.id {
                self.manifests.as_ref().clone()
            } else {
                Vec::new()
            })
        }
    }

    impl ExportRepository for InMemoryOrganizationRepository {
        fn get_latest_exported_metadata(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(self
                .exports
                .iter()
                .find(|item| item.release_instance_id == *release_instance_id)
                .cloned())
        }
        fn list_exported_metadata(
            &self,
            query: &ExportedMetadataListQuery,
        ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(Page {
                items: self.exports.as_ref().clone(),
                request: query.page,
                total: self.exports.len() as u64,
            })
        }
        fn get_exported_metadata(
            &self,
            id: &ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(self.exports.iter().find(|item| item.id == *id).cloned())
        }
    }

    fn test_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("discern-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("temp root should exist");
        root
    }
}
