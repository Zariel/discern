use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use id3::frame::ExtendedText;
use id3::{TagLike, Version};
use tokio::task;

use crate::application::config::{ExportPolicy, TaggingPolicy};
use crate::application::repository::{
    ExportRepository, ReleaseInstanceRepository, ReleaseRepository, RepositoryError,
    RepositoryErrorKind, StagingManifestRepository,
};
use crate::domain::export_profile::PlayerMetadataField;
use crate::domain::release::Release;
use crate::domain::release_group::ReleaseGroup;
use crate::domain::release_instance::{FormatFamily, ReleaseInstance};
use crate::domain::staging_manifest::StagedReleaseGroup;
use crate::domain::track::Track;
use crate::support::ids::ReleaseInstanceId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggingReport {
    pub written_files: Vec<PathBuf>,
    pub format_family: FormatFamily,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggingError {
    pub kind: TaggingErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaggingErrorKind {
    NotFound,
    Conflict,
    Storage,
    Unsupported,
}

pub struct TagWriterService<R> {
    repository: R,
}

impl<R> TagWriterService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> TagWriterService<R>
where
    R: ExportRepository + ReleaseInstanceRepository + ReleaseRepository + StagingManifestRepository,
{
    pub async fn write_release_instance_tags(
        &self,
        export_policy: &ExportPolicy,
        tagging_policy: &TaggingPolicy,
        release_instance_id: &ReleaseInstanceId,
    ) -> Result<TaggingReport, TaggingError> {
        let release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| TaggingError {
                kind: TaggingErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        let release_id = release_instance
            .release_id
            .clone()
            .ok_or_else(|| TaggingError {
                kind: TaggingErrorKind::Conflict,
                message: format!(
                    "release instance {} has no canonical release",
                    release_instance.id.as_uuid()
                ),
            })?;
        let release = self
            .repository
            .get_release(&release_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| TaggingError {
                kind: TaggingErrorKind::NotFound,
                message: format!("no release found for {}", release_id.as_uuid()),
            })?;
        let release_group = self
            .repository
            .get_release_group(&release.release_group_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| TaggingError {
                kind: TaggingErrorKind::NotFound,
                message: format!(
                    "no release group found for {}",
                    release.release_group_id.as_uuid()
                ),
            })?;
        let export_snapshot = self
            .repository
            .get_latest_exported_metadata(&release_instance.id)
            .map_err(map_repository_error)?
            .ok_or_else(|| TaggingError {
                kind: TaggingErrorKind::NotFound,
                message: format!(
                    "no exported metadata snapshot found for {}",
                    release_instance.id.as_uuid()
                ),
            })?;
        let export_profile = export_policy
            .profiles
            .iter()
            .find(|profile| profile.name == export_snapshot.export_profile)
            .ok_or_else(|| TaggingError {
                kind: TaggingErrorKind::Conflict,
                message: format!(
                    "export profile {} was not available during tagging",
                    export_snapshot.export_profile
                ),
            })?;
        let tracks = self
            .repository
            .list_tracks_for_release(&release.id)
            .map_err(map_repository_error)?;
        if tracks.is_empty() {
            return Err(TaggingError {
                kind: TaggingErrorKind::Conflict,
                message: format!("release {} had no canonical tracks", release.id.as_uuid()),
            });
        }

        let manifests = self
            .repository
            .list_staging_manifests_for_batch(&release_instance.import_batch_id)
            .map_err(map_repository_error)?;
        let write_plans = build_write_plans(&release_instance, &tracks, &manifests)?;
        let tagging_policy = tagging_policy.clone();
        let release = release.clone();
        let release_group = release_group.clone();
        let export_snapshot = export_snapshot.clone();
        let export_profile = export_profile.clone();

        task::spawn_blocking(move || {
            let write_context = TagWriteContext {
                tagging_policy: &tagging_policy,
                release: &release,
                release_group: &release_group,
                export_snapshot: &export_snapshot,
                exported_fields: &export_profile.exported_fields,
                write_internal_ids: export_profile.write_internal_ids,
            };
            for write_plan in &write_plans {
                match release_instance.technical_variant.format_family {
                    FormatFamily::Flac => {
                        write_flac_tags(&write_plan.path, &write_context, write_plan)?
                    }
                    FormatFamily::Mp3 => {
                        write_mp3_tags(&write_plan.path, &write_context, write_plan)?
                    }
                }
            }

            Ok(TaggingReport {
                written_files: write_plans.into_iter().map(|plan| plan.path).collect(),
                format_family: release_instance.technical_variant.format_family,
            })
        })
        .await
        .map_err(|error| TaggingError {
            kind: TaggingErrorKind::Storage,
            message: format!("tagging task failed to join: {error}"),
        })?
    }
}

#[derive(Debug, Clone)]
struct TagWritePlan {
    path: PathBuf,
    track: Track,
    total_tracks_on_disc: u16,
    total_discs: u16,
}

struct TagWriteContext<'a> {
    tagging_policy: &'a TaggingPolicy,
    release: &'a Release,
    release_group: &'a ReleaseGroup,
    export_snapshot: &'a crate::domain::exported_metadata_snapshot::ExportedMetadataSnapshot,
    exported_fields: &'a [PlayerMetadataField],
    write_internal_ids: bool,
}

fn build_write_plans(
    release_instance: &ReleaseInstance,
    tracks: &[Track],
    manifests: &[crate::domain::staging_manifest::StagingManifest],
) -> Result<Vec<TagWritePlan>, TaggingError> {
    let mut ordered_tracks = tracks.to_vec();
    ordered_tracks.sort_by_key(|track| (track.position.disc_number, track.position.track_number));

    let group = resolve_manifest_group(release_instance, manifests, ordered_tracks.len())?;
    let mut file_paths: Vec<_> = group.file_paths.clone();
    file_paths.sort();

    if file_paths.len() != ordered_tracks.len() {
        return Err(TaggingError {
            kind: TaggingErrorKind::Conflict,
            message: format!(
                "release instance {} had {} files but {} canonical tracks",
                release_instance.id.as_uuid(),
                file_paths.len(),
                ordered_tracks.len()
            ),
        });
    }

    let mut per_disc_counts = BTreeMap::<u16, u16>::new();
    for track in &ordered_tracks {
        *per_disc_counts
            .entry(track.position.disc_number)
            .or_insert(0) += 1;
    }
    let total_discs = per_disc_counts.len() as u16;

    Ok(file_paths
        .into_iter()
        .zip(ordered_tracks)
        .map(|(path, track)| TagWritePlan {
            path,
            total_tracks_on_disc: *per_disc_counts
                .get(&track.position.disc_number)
                .expect("disc count should exist"),
            total_discs,
            track,
        })
        .collect())
}

fn resolve_manifest_group<'a>(
    release_instance: &ReleaseInstance,
    manifests: &'a [crate::domain::staging_manifest::StagingManifest],
    expected_tracks: usize,
) -> Result<&'a StagedReleaseGroup, TaggingError> {
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
                    .and_then(|extension| extension.to_str())
                    .map(|extension| extension.eq_ignore_ascii_case(expected_extension))
                    .unwrap_or(false)
            })
        })
        .collect();

    if candidates.is_empty() {
        return Err(TaggingError {
            kind: TaggingErrorKind::Conflict,
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
            common_parent(group)
                .map(|parent| parent == expected_root || expected_root.starts_with(parent))
                .unwrap_or(false)
        })
        .collect();

    match rooted.as_slice() {
        [group] => Ok(*group),
        [] => match candidates.as_slice() {
            [group] => Ok(*group),
            _ => Err(TaggingError {
                kind: TaggingErrorKind::Conflict,
                message: format!(
                    "multiple staged groups matched release instance {}",
                    release_instance.id.as_uuid()
                ),
            }),
        },
        _ => Err(TaggingError {
            kind: TaggingErrorKind::Conflict,
            message: format!(
                "multiple staged roots matched release instance {}",
                release_instance.id.as_uuid()
            ),
        }),
    }
}

fn common_parent(group: &StagedReleaseGroup) -> Option<&Path> {
    group.file_paths.first().and_then(|path| path.parent())
}

fn write_flac_tags(
    path: &Path,
    write_context: &TagWriteContext<'_>,
    write_plan: &TagWritePlan,
) -> Result<(), TaggingError> {
    let mut tag = match metaflac::Tag::read_from_path(path) {
        Ok(tag) => tag,
        Err(_) => {
            let mut tag = metaflac::Tag::new();
            let mut stream_info = metaflac::block::StreamInfo::new();
            stream_info.sample_rate = 44_100;
            stream_info.num_channels = 2;
            stream_info.bits_per_sample = 16;
            stream_info.md5 = vec![0; 16];
            tag.push_block(metaflac::Block::StreamInfo(stream_info));
            tag
        }
    };

    apply_vorbis_unknown_policy(&mut tag, write_context.tagging_policy);
    clear_managed_vorbis_fields(&mut tag);

    let artist_value = write_context.export_snapshot.artist_credits.join("; ");
    for field in write_context.exported_fields {
        match field {
            PlayerMetadataField::Album => {
                tag.set_vorbis(
                    "ALBUM",
                    vec![write_context.export_snapshot.album_title.clone()],
                );
            }
            PlayerMetadataField::AlbumArtist => {
                tag.set_vorbis(
                    "ALBUMARTIST",
                    vec![write_context.export_snapshot.album_artist.clone()],
                );
            }
            PlayerMetadataField::Artist => {
                tag.set_vorbis("ARTIST", vec![artist_value.clone()]);
            }
            PlayerMetadataField::Title => {
                tag.set_vorbis("TITLE", vec![write_plan.track.title.clone()]);
            }
            PlayerMetadataField::TrackNumber => {
                tag.set_vorbis(
                    "TRACKNUMBER",
                    vec![write_plan.track.position.track_number.to_string()],
                );
            }
            PlayerMetadataField::TotalTracks => {
                tag.set_vorbis(
                    "TOTALTRACKS",
                    vec![write_plan.total_tracks_on_disc.to_string()],
                );
            }
            PlayerMetadataField::DiscNumber => {
                tag.set_vorbis(
                    "DISCNUMBER",
                    vec![write_plan.track.position.disc_number.to_string()],
                );
            }
            PlayerMetadataField::TotalDiscs => {
                tag.set_vorbis("TOTALDISCS", vec![write_plan.total_discs.to_string()]);
            }
            PlayerMetadataField::Date => {
                if let Some(release_date) = &write_context.release.edition.release_date {
                    tag.set_vorbis("DATE", vec![release_date.year.to_string()]);
                }
            }
            PlayerMetadataField::Genre => {}
            PlayerMetadataField::MusicBrainzIdentifiers => {
                if write_context.write_internal_ids {
                    set_musicbrainz_vorbis_fields(
                        &mut tag,
                        write_context.release,
                        write_context.release_group,
                        &write_plan.track,
                    );
                }
            }
        }
    }

    tag.write_to_path(path).map_err(|error| TaggingError {
        kind: TaggingErrorKind::Storage,
        message: format!("failed to write FLAC tags to {}: {error}", path.display()),
    })
}

fn write_mp3_tags(
    path: &Path,
    write_context: &TagWriteContext<'_>,
    write_plan: &TagWritePlan,
) -> Result<(), TaggingError> {
    let existing_tag = id3::Tag::read_from_path(path).unwrap_or_else(|_| id3::Tag::new());
    let version = match write_context.tagging_policy.mp3_id3v2_version {
        crate::config::Id3v2Version::V23 => Version::Id3v23,
        crate::config::Id3v2Version::V24 => Version::Id3v24,
    };
    let mut tag = build_base_id3_tag(&existing_tag, write_context.tagging_policy, version);
    clear_managed_id3_fields(&mut tag);

    let artist_value = write_context.export_snapshot.artist_credits.join("; ");
    for field in write_context.exported_fields {
        match field {
            PlayerMetadataField::Album => tag.set_album(&write_context.export_snapshot.album_title),
            PlayerMetadataField::AlbumArtist => {
                tag.set_album_artist(&write_context.export_snapshot.album_artist)
            }
            PlayerMetadataField::Artist => tag.set_artist(&artist_value),
            PlayerMetadataField::Title => tag.set_title(&write_plan.track.title),
            PlayerMetadataField::TrackNumber => {
                tag.set_track(write_plan.track.position.track_number.into())
            }
            PlayerMetadataField::TotalTracks => {
                tag.set_total_tracks(write_plan.total_tracks_on_disc.into())
            }
            PlayerMetadataField::DiscNumber => {
                tag.set_disc(write_plan.track.position.disc_number.into())
            }
            PlayerMetadataField::TotalDiscs => tag.set_total_discs(write_plan.total_discs.into()),
            PlayerMetadataField::Date => {
                if let Some(release_date) = &write_context.release.edition.release_date {
                    tag.set_year(release_date.year.into());
                }
            }
            PlayerMetadataField::Genre => {}
            PlayerMetadataField::MusicBrainzIdentifiers => {
                if write_context.write_internal_ids {
                    set_musicbrainz_id3_fields(
                        &mut tag,
                        write_context.release,
                        write_context.release_group,
                        &write_plan.track,
                    );
                }
            }
        }
    }

    tag.write_to_path(path, version)
        .map_err(|error| TaggingError {
            kind: TaggingErrorKind::Storage,
            message: format!("failed to write MP3 tags to {}: {error}", path.display()),
        })
}

fn clear_managed_vorbis_fields(tag: &mut metaflac::Tag) {
    for key in managed_vorbis_keys() {
        tag.remove_vorbis(&key);
    }
}

fn apply_vorbis_unknown_policy(tag: &mut metaflac::Tag, tagging_policy: &TaggingPolicy) {
    let selected = selected_keys(&tagging_policy.selected_tag_keys);
    let managed = managed_vorbis_keys();
    let keys: Vec<_> = tag
        .vorbis_comments()
        .map(|comments| comments.comments.keys().cloned().collect())
        .unwrap_or_default();

    for key in keys {
        if managed.contains(&key) {
            continue;
        }
        let keep = match tagging_policy.unknown_tag_policy {
            crate::config::UnknownTagPolicy::DropUnknown => false,
            crate::config::UnknownTagPolicy::PreserveUnknown => true,
            crate::config::UnknownTagPolicy::PreserveSelected => {
                selected.contains(&normalize_key(&key))
            }
        };
        if !keep {
            tag.remove_vorbis(&key);
        }
    }
}

fn build_base_id3_tag(
    existing_tag: &id3::Tag,
    tagging_policy: &TaggingPolicy,
    version: Version,
) -> id3::Tag {
    let mut base_tag = id3::Tag::with_version(version);
    let selected = selected_keys(&tagging_policy.selected_tag_keys);

    for frame in existing_tag.frames() {
        let keep = match tagging_policy.unknown_tag_policy {
            crate::config::UnknownTagPolicy::DropUnknown => false,
            crate::config::UnknownTagPolicy::PreserveUnknown => keep_unknown_id3_frame(frame),
            crate::config::UnknownTagPolicy::PreserveSelected => {
                keep_selected_id3_frame(frame, &selected)
            }
        };
        if keep {
            base_tag.add_frame(frame.clone());
        }
    }

    base_tag
}

fn clear_managed_id3_fields(tag: &mut id3::Tag) {
    for frame_id in [
        "TALB", "TPE1", "TPE2", "TIT2", "TRCK", "TPOS", "TYER", "TDRC", "TCON",
    ] {
        tag.remove(frame_id);
    }
    for description in [
        "MusicBrainz Album Id",
        "MusicBrainz Release Group Id",
        "MusicBrainz Track Id",
    ] {
        tag.remove_extended_text(Some(description), None);
    }
}

fn keep_unknown_id3_frame(frame: &id3::Frame) -> bool {
    if is_managed_id3_frame(frame) {
        return false;
    }
    true
}

fn keep_selected_id3_frame(frame: &id3::Frame, selected: &BTreeSet<String>) -> bool {
    if is_managed_id3_frame(frame) {
        return false;
    }
    if frame.id() == "TXXX"
        && let Some(ext) = frame.content().extended_text()
    {
        return selected.contains(&normalize_key(&ext.description));
    }
    false
}

fn is_managed_id3_frame(frame: &id3::Frame) -> bool {
    if matches!(
        frame.id(),
        "TALB" | "TPE1" | "TPE2" | "TIT2" | "TRCK" | "TPOS" | "TYER" | "TDRC" | "TCON"
    ) {
        return true;
    }
    frame.id() == "TXXX"
        && frame
            .content()
            .extended_text()
            .map(|ext| {
                matches!(
                    ext.description.as_str(),
                    "MusicBrainz Album Id"
                        | "MusicBrainz Release Group Id"
                        | "MusicBrainz Track Id"
                )
            })
            .unwrap_or(false)
}

fn set_musicbrainz_vorbis_fields(
    tag: &mut metaflac::Tag,
    release: &Release,
    release_group: &ReleaseGroup,
    track: &Track,
) {
    if let Some(id) = &release.musicbrainz_release_id {
        tag.set_vorbis("MUSICBRAINZ_ALBUMID", vec![id.as_uuid().to_string()]);
    }
    if let Some(id) = &release_group.musicbrainz_release_group_id {
        tag.set_vorbis("MUSICBRAINZ_RELEASEGROUPID", vec![id.as_uuid().to_string()]);
    }
    if let Some(id) = &track.musicbrainz_track_id {
        tag.set_vorbis("MUSICBRAINZ_TRACKID", vec![id.as_uuid().to_string()]);
    }
}

fn set_musicbrainz_id3_fields(
    tag: &mut id3::Tag,
    release: &Release,
    release_group: &ReleaseGroup,
    track: &Track,
) {
    if let Some(id) = &release.musicbrainz_release_id {
        tag.add_frame(ExtendedText {
            description: "MusicBrainz Album Id".to_string(),
            value: id.as_uuid().to_string(),
        });
    }
    if let Some(id) = &release_group.musicbrainz_release_group_id {
        tag.add_frame(ExtendedText {
            description: "MusicBrainz Release Group Id".to_string(),
            value: id.as_uuid().to_string(),
        });
    }
    if let Some(id) = &track.musicbrainz_track_id {
        tag.add_frame(ExtendedText {
            description: "MusicBrainz Track Id".to_string(),
            value: id.as_uuid().to_string(),
        });
    }
}

fn managed_vorbis_keys() -> BTreeSet<String> {
    [
        "ALBUM",
        "ALBUMARTIST",
        "ARTIST",
        "TITLE",
        "TRACKNUMBER",
        "TOTALTRACKS",
        "DISCNUMBER",
        "TOTALDISCS",
        "DATE",
        "GENRE",
        "MUSICBRAINZ_ALBUMID",
        "MUSICBRAINZ_RELEASEGROUPID",
        "MUSICBRAINZ_TRACKID",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn selected_keys(keys: &[String]) -> BTreeSet<String> {
    keys.iter().map(|key| normalize_key(key)).collect()
}

fn normalize_key(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

fn map_repository_error(error: RepositoryError) -> TaggingError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => TaggingErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            TaggingErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => TaggingErrorKind::Storage,
    };
    TaggingError {
        kind,
        message: error.message,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use super::*;
    use crate::application::config::ValidatedRuntimeConfig;
    use crate::application::repository::{
        ExportedMetadataListQuery, ReleaseGroupSearchQuery, ReleaseInstanceListQuery,
        ReleaseListQuery,
    };
    use crate::domain::artist::Artist;
    use crate::domain::exported_metadata_snapshot::{
        CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
    };
    use crate::domain::release::{PartialDate, ReleaseEdition};
    use crate::domain::release_group::ReleaseGroupKind;
    use crate::domain::release_instance::{
        BitrateMode, IngestOrigin, ProvenanceSnapshot, TechnicalVariant,
    };
    use crate::domain::staging_manifest::{
        AuxiliaryFile, GroupingDecision, GroupingStrategy, StagedReleaseGroup, StagingManifest,
        StagingManifestSource,
    };
    use crate::support::ids::{
        ArtistId, ExportedMetadataSnapshotId, ImportBatchId, MusicBrainzReleaseGroupId,
        MusicBrainzReleaseId, MusicBrainzTrackId, ReleaseGroupId, ReleaseId, ReleaseInstanceId,
        SourceId, StagingManifestId, TrackId,
    };
    use crate::support::pagination::{Page, PageRequest};

    #[tokio::test(flavor = "current_thread")]
    async fn writer_applies_canonical_tags_idempotently_for_mp3_and_flac() {
        let temp_root = test_root("tag-writer-idempotent");
        let mp3_path = temp_root.join("disc1-01.mp3");
        let flac_path = temp_root.join("disc1-01.flac");
        fs::write(&mp3_path, b"mp3 audio").expect("mp3 fixture should exist");
        create_minimal_flac(&flac_path);

        let repository =
            InMemoryTagRepository::new(vec![mp3_path.clone()], vec![flac_path.clone()]);
        let config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        let service = TagWriterService::new(repository.clone());

        service
            .write_release_instance_tags(
                &config.export,
                &config.export.tagging,
                &repository.mp3_release_instance_id,
            )
            .await
            .expect("mp3 tagging should succeed");
        service
            .write_release_instance_tags(
                &config.export,
                &config.export.tagging,
                &repository.mp3_release_instance_id,
            )
            .await
            .expect("repeated mp3 tagging should succeed");
        service
            .write_release_instance_tags(
                &config.export,
                &config.export.tagging,
                &repository.flac_release_instance_id,
            )
            .await
            .expect("flac tagging should succeed");
        service
            .write_release_instance_tags(
                &config.export,
                &config.export.tagging,
                &repository.flac_release_instance_id,
            )
            .await
            .expect("repeated flac tagging should succeed");

        let mp3_tag = id3::Tag::read_from_path(&mp3_path).expect("mp3 tag should load");
        assert_eq!(mp3_tag.album(), Some("Kid A [2000]"));
        assert_eq!(mp3_tag.artist(), Some("Radiohead"));
        assert_eq!(mp3_tag.title(), Some("Everything in Its Right Place"));
        assert_eq!(mp3_tag.track(), Some(1));
        assert_eq!(mp3_tag.total_tracks(), Some(1));
        assert!(mp3_tag.extended_texts().any(|item| {
            item.description == "MusicBrainz Album Id"
                && item.value
                    == repository
                        .release
                        .musicbrainz_release_id
                        .clone()
                        .unwrap()
                        .as_uuid()
                        .to_string()
        }));

        let flac_tag = metaflac::Tag::read_from_path(&flac_path).expect("flac tag should load");
        let comments = flac_tag
            .vorbis_comments()
            .expect("vorbis comments should exist");
        assert_eq!(
            comments.get("ALBUM").unwrap(),
            &vec!["Kid A [2000]".to_string()]
        );
        assert_eq!(
            comments.get("TITLE").unwrap(),
            &vec!["Everything in Its Right Place".to_string()]
        );
        assert_eq!(comments.get("TRACKNUMBER").unwrap(), &vec!["1".to_string()]);
        assert_eq!(
            comments.get("MUSICBRAINZ_ALBUMID").unwrap(),
            &vec![
                repository
                    .release
                    .musicbrainz_release_id
                    .clone()
                    .unwrap()
                    .as_uuid()
                    .to_string()
            ]
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn writer_preserves_only_selected_unknown_mp3_tags() {
        let temp_root = test_root("tag-writer-selected");
        let mp3_path = temp_root.join("disc1-01.mp3");
        fs::write(&mp3_path, b"mp3 audio").expect("mp3 fixture should exist");

        let mut existing = id3::Tag::new();
        existing.add_frame(ExtendedText {
            description: "Keep Key".to_string(),
            value: "keep".to_string(),
        });
        existing.add_frame(ExtendedText {
            description: "Drop Key".to_string(),
            value: "drop".to_string(),
        });
        existing
            .write_to_path(&mp3_path, Version::Id3v24)
            .expect("seed tag should write");

        let repository = InMemoryTagRepository::new(vec![mp3_path.clone()], vec![]);
        let mut config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        config.export.tagging.unknown_tag_policy =
            crate::config::UnknownTagPolicy::PreserveSelected;
        config.export.tagging.selected_tag_keys = vec!["keep_key".to_string()];
        let service = TagWriterService::new(repository.clone());

        service
            .write_release_instance_tags(
                &config.export,
                &config.export.tagging,
                &repository.mp3_release_instance_id,
            )
            .await
            .expect("tagging should succeed");

        let tag = id3::Tag::read_from_path(&mp3_path).expect("mp3 tag should load");
        assert!(
            tag.extended_texts()
                .any(|item| item.description == "Keep Key" && item.value == "keep")
        );
        assert!(
            !tag.extended_texts()
                .any(|item| item.description == "Drop Key")
        );

        let _ = fs::remove_dir_all(temp_root);
    }

    #[derive(Clone)]
    struct InMemoryTagRepository {
        release_group: ReleaseGroup,
        release: Release,
        tracks: Arc<Vec<Track>>,
        mp3_release_instance: ReleaseInstance,
        flac_release_instance: ReleaseInstance,
        manifests: Arc<Vec<StagingManifest>>,
        exports: Arc<Vec<ExportedMetadataSnapshot>>,
        mp3_release_instance_id: ReleaseInstanceId,
        flac_release_instance_id: ReleaseInstanceId,
    }

    impl InMemoryTagRepository {
        fn new(mp3_paths: Vec<PathBuf>, flac_paths: Vec<PathBuf>) -> Self {
            let artist_id = ArtistId::new();
            let release_group = ReleaseGroup {
                id: ReleaseGroupId::new(),
                primary_artist_id: artist_id.clone(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
                musicbrainz_release_group_id: MusicBrainzReleaseGroupId::parse_str(
                    "44444444-4444-4444-8444-444444444444",
                )
                .ok(),
            };
            let release = Release {
                id: ReleaseId::new(),
                release_group_id: release_group.id.clone(),
                primary_artist_id: artist_id,
                title: "Kid A".to_string(),
                musicbrainz_release_id: MusicBrainzReleaseId::parse_str(
                    "55555555-5555-4555-8555-555555555555",
                )
                .ok(),
                discogs_release_id: None,
                edition: ReleaseEdition {
                    edition_title: None,
                    disambiguation: None,
                    country: Some("GB".to_string()),
                    label: None,
                    catalog_number: None,
                    release_date: Some(PartialDate {
                        year: 2000,
                        month: Some(10),
                        day: Some(2),
                    }),
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
                musicbrainz_track_id: MusicBrainzTrackId::parse_str(
                    "66666666-6666-4666-8666-666666666666",
                )
                .ok(),
                duration_ms: Some(250_000),
            };
            let batch_id = ImportBatchId::new();
            let source_id = SourceId::new();
            let mp3_release_instance_id = ReleaseInstanceId::new();
            let flac_release_instance_id = ReleaseInstanceId::new();
            let mp3_release_instance = ReleaseInstance {
                id: mp3_release_instance_id.clone(),
                import_batch_id: batch_id.clone(),
                source_id: source_id.clone(),
                release_id: Some(release.id.clone()),
                state: crate::domain::release_instance::ReleaseInstanceState::RenderingExport,
                technical_variant: TechnicalVariant {
                    format_family: FormatFamily::Mp3,
                    bitrate_mode: BitrateMode::Variable,
                    bitrate_kbps: Some(320),
                    sample_rate_hz: Some(44_100),
                    bit_depth: None,
                    track_count: 1,
                    total_duration_seconds: 250,
                },
                provenance: ProvenanceSnapshot {
                    ingest_origin: IngestOrigin::ManualAdd,
                    original_source_path: mp3_paths
                        .first()
                        .and_then(|path| path.parent())
                        .unwrap_or_else(|| Path::new("/tmp"))
                        .display()
                        .to_string(),
                    imported_at_unix_seconds: 1,
                    gazelle_reference: None,
                },
            };
            let flac_release_instance = ReleaseInstance {
                id: flac_release_instance_id.clone(),
                import_batch_id: batch_id.clone(),
                source_id,
                release_id: Some(release.id.clone()),
                state: crate::domain::release_instance::ReleaseInstanceState::RenderingExport,
                technical_variant: TechnicalVariant {
                    format_family: FormatFamily::Flac,
                    bitrate_mode: BitrateMode::Lossless,
                    bitrate_kbps: None,
                    sample_rate_hz: Some(44_100),
                    bit_depth: Some(16),
                    track_count: 1,
                    total_duration_seconds: 250,
                },
                provenance: ProvenanceSnapshot {
                    ingest_origin: IngestOrigin::ManualAdd,
                    original_source_path: flac_paths
                        .first()
                        .and_then(|path| path.parent())
                        .unwrap_or_else(|| Path::new("/tmp"))
                        .display()
                        .to_string(),
                    imported_at_unix_seconds: 2,
                    gazelle_reference: None,
                },
            };
            let manifests = vec![
                StagingManifest {
                    id: StagingManifestId::new(),
                    batch_id: batch_id.clone(),
                    source: StagingManifestSource {
                        kind: crate::domain::source::SourceKind::ManualAdd,
                        source_path: mp3_paths
                            .first()
                            .and_then(|path| path.parent())
                            .unwrap_or_else(|| Path::new("/tmp"))
                            .to_path_buf(),
                    },
                    discovered_files: Vec::new(),
                    auxiliary_files: Vec::new(),
                    grouping: GroupingDecision {
                        strategy: GroupingStrategy::ManualManifest,
                        groups: vec![StagedReleaseGroup {
                            key: "mp3".to_string(),
                            file_paths: mp3_paths,
                            auxiliary_paths: Vec::new(),
                        }],
                        notes: Vec::new(),
                    },
                    captured_at_unix_seconds: 1,
                },
                StagingManifest {
                    id: StagingManifestId::new(),
                    batch_id,
                    source: StagingManifestSource {
                        kind: crate::domain::source::SourceKind::ManualAdd,
                        source_path: flac_paths
                            .first()
                            .and_then(|path| path.parent())
                            .unwrap_or_else(|| Path::new("/tmp"))
                            .to_path_buf(),
                    },
                    discovered_files: Vec::new(),
                    auxiliary_files: Vec::<AuxiliaryFile>::new(),
                    grouping: GroupingDecision {
                        strategy: GroupingStrategy::ManualManifest,
                        groups: vec![StagedReleaseGroup {
                            key: "flac".to_string(),
                            file_paths: flac_paths,
                            auxiliary_paths: Vec::new(),
                        }],
                        notes: Vec::new(),
                    },
                    captured_at_unix_seconds: 2,
                },
            ];
            let exports = vec![
                ExportedMetadataSnapshot {
                    id: ExportedMetadataSnapshotId::new(),
                    release_instance_id: mp3_release_instance_id.clone(),
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
                    rendered_at_unix_seconds: 10,
                },
                ExportedMetadataSnapshot {
                    id: ExportedMetadataSnapshotId::new(),
                    release_instance_id: flac_release_instance_id.clone(),
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
                    rendered_at_unix_seconds: 11,
                },
            ];

            Self {
                release_group,
                release: release.clone(),
                tracks: Arc::new(vec![track]),
                mp3_release_instance,
                flac_release_instance,
                manifests: Arc::new(manifests),
                exports: Arc::new(exports),
                mp3_release_instance_id,
                flac_release_instance_id,
            }
        }
    }

    impl ReleaseRepository for InMemoryTagRepository {
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
            if *release_id == self.release.id {
                Ok(self.tracks.as_ref().clone())
            } else {
                Ok(Vec::new())
            }
        }
    }

    impl ReleaseInstanceRepository for InMemoryTagRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(if *id == self.mp3_release_instance.id {
                Some(self.mp3_release_instance.clone())
            } else if *id == self.flac_release_instance.id {
                Some(self.flac_release_instance.clone())
            } else {
                None
            })
        }

        fn list_release_instances(
            &self,
            query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            Ok(Page {
                items: vec![
                    self.mp3_release_instance.clone(),
                    self.flac_release_instance.clone(),
                ],
                request: query.page,
                total: 2,
            })
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(vec![
                self.mp3_release_instance.clone(),
                self.flac_release_instance.clone(),
            ])
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
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<crate::domain::track_instance::TrackInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _role: Option<crate::domain::file::FileRole>,
        ) -> Result<Vec<crate::domain::file::FileRecord>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl ExportRepository for InMemoryTagRepository {
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

    impl StagingManifestRepository for InMemoryTagRepository {
        fn list_staging_manifests_for_batch(
            &self,
            _batch_id: &ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(self.manifests.as_ref().clone())
        }
    }

    fn test_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("discern-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("temp root should exist");
        root
    }

    fn create_minimal_flac(path: &Path) {
        let mut tag = metaflac::Tag::new();
        let mut stream_info = metaflac::block::StreamInfo::new();
        stream_info.sample_rate = 44_100;
        stream_info.num_channels = 2;
        stream_info.bits_per_sample = 16;
        stream_info.md5 = vec![0; 16];
        tag.push_block(metaflac::Block::StreamInfo(stream_info));
        tag.write_to_path(path)
            .expect("minimal flac should be written");
    }
}
