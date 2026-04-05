use crate::application::config::ExportPolicy;
use crate::application::repository::{
    ExportCommandRepository, ManualOverrideListQuery, ManualOverrideRepository,
    ReleaseInstanceRepository, ReleaseRepository, RepositoryError, RepositoryErrorKind,
};
use crate::domain::export_profile::{
    ArtworkPolicy, EditionVisibilityPolicy, ExportProfile, QualifierVisibilityPolicy,
};
use crate::domain::exported_metadata_snapshot::{
    CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
};
use crate::domain::manual_override::{ManualOverride, OverrideField, OverrideSubject};
use crate::domain::release::PartialDate;
use crate::domain::release_instance::{BitrateMode, FormatFamily, ReleaseInstance};
use crate::support::ids::{ExportedMetadataSnapshotId, ReleaseInstanceId};
use crate::support::pagination::PageRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRenderingError {
    pub kind: ExportRenderingErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportRenderingErrorKind {
    NotFound,
    Conflict,
    Storage,
}

pub struct ExportRenderingService<R> {
    repository: R,
}

impl<R> ExportRenderingService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> ExportRenderingService<R>
where
    R: ExportCommandRepository
        + ManualOverrideRepository
        + ReleaseInstanceRepository
        + ReleaseRepository,
{
    pub fn render_release_instance_snapshot(
        &self,
        export_policy: &ExportPolicy,
        release_instance_id: &ReleaseInstanceId,
        rendered_at_unix_seconds: i64,
    ) -> Result<ExportedMetadataSnapshot, ExportRenderingError> {
        let release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| ExportRenderingError {
                kind: ExportRenderingErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        let release_id =
            release_instance
                .release_id
                .clone()
                .ok_or_else(|| ExportRenderingError {
                    kind: ExportRenderingErrorKind::Conflict,
                    message: format!(
                        "release instance {} has no canonical release",
                        release_instance_id.as_uuid()
                    ),
                })?;
        let release = self
            .repository
            .get_release(&release_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| ExportRenderingError {
                kind: ExportRenderingErrorKind::NotFound,
                message: format!("no release found for {}", release_id.as_uuid()),
            })?;
        let release_group = self
            .repository
            .get_release_group(&release.release_group_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| ExportRenderingError {
                kind: ExportRenderingErrorKind::NotFound,
                message: format!(
                    "no release group found for {}",
                    release.release_group_id.as_uuid()
                ),
            })?;

        let profile = export_policy
            .profiles
            .iter()
            .find(|profile| profile.name == export_policy.default_profile)
            .ok_or_else(|| ExportRenderingError {
                kind: ExportRenderingErrorKind::Conflict,
                message: format!(
                    "default export profile {} was not available",
                    export_policy.default_profile
                ),
            })?;

        let release_overrides =
            self.list_subject_overrides(OverrideSubject::Release(release_id.clone()))?;
        let instance_overrides = self.list_subject_overrides(OverrideSubject::ReleaseInstance(
            release_instance.id.clone(),
        ))?;

        let base_album_title = latest_override_value(&release_overrides, OverrideField::Title)
            .unwrap_or_else(|| release.title.clone());
        let edition_qualifier =
            latest_override_value(&release_overrides, OverrideField::EditionQualifier)
                .or_else(|| default_edition_qualifier(&release));
        let album_title =
            render_album_title(&base_album_title, edition_qualifier.as_deref(), profile);
        let album_artist = latest_override_value(&release_overrides, OverrideField::AlbumArtist)
            .unwrap_or_else(|| release_group.title_artist_fallback());
        let artist_credit = latest_override_value(&release_overrides, OverrideField::ArtistCredit)
            .map(|value| vec![value])
            .unwrap_or_else(|| vec![album_artist.clone()]);
        let path_components = render_path_components(
            &release_instance,
            &album_artist,
            &album_title,
            profile,
            edition_qualifier.as_deref(),
        );
        let artwork = latest_override_value(&instance_overrides, OverrideField::ArtworkSelection)
            .or_else(|| match &profile.artwork {
                ArtworkPolicy::SidecarFile { file_name, .. } => Some(file_name.clone()),
            });
        let compatibility = CompatibilityReport {
            verified: true,
            warnings: compatibility_warnings(
                profile,
                &release_instance,
                edition_qualifier.as_deref(),
            ),
        };
        let snapshot = ExportedMetadataSnapshot {
            id: ExportedMetadataSnapshotId::new(),
            release_instance_id: release_instance.id,
            export_profile: profile.name.clone(),
            album_title,
            album_artist,
            artist_credits: artist_credit,
            edition_visibility: map_visibility(&profile.edition_visibility),
            technical_visibility: map_variant_policy(&profile.technical_visibility),
            path_components,
            primary_artwork_filename: artwork,
            compatibility,
            rendered_at_unix_seconds,
        };
        self.repository
            .create_exported_metadata_snapshot(&snapshot)
            .map_err(map_repository_error)?;
        Ok(snapshot)
    }

    fn list_subject_overrides(
        &self,
        subject: OverrideSubject,
    ) -> Result<Vec<ManualOverride>, ExportRenderingError> {
        Ok(self
            .repository
            .list_manual_overrides(&ManualOverrideListQuery {
                subject: Some(subject),
                field: None,
                page: PageRequest::new(50, 0),
            })
            .map_err(map_repository_error)?
            .items)
    }
}

fn latest_override_value(overrides: &[ManualOverride], field: OverrideField) -> Option<String> {
    overrides
        .iter()
        .find(|override_record| override_record.field == field)
        .map(|override_record| override_record.value.clone())
}

fn default_edition_qualifier(release: &crate::domain::release::Release) -> Option<String> {
    release.edition.edition_title.clone().or_else(|| {
        release
            .edition
            .release_date
            .as_ref()
            .map(render_partial_date)
    })
}

fn render_album_title(
    base_title: &str,
    edition_qualifier: Option<&str>,
    profile: &ExportProfile,
) -> String {
    match profile.edition_visibility {
        EditionVisibilityPolicy::Hidden => base_title.to_string(),
        EditionVisibilityPolicy::AlbumTitleWhenNeeded => edition_qualifier
            .map(|qualifier| format!("{base_title} [{qualifier}]"))
            .unwrap_or_else(|| base_title.to_string()),
        EditionVisibilityPolicy::AlbumTitleAlways => {
            let qualifier = edition_qualifier.unwrap_or("Edition");
            format!("{base_title} [{qualifier}]")
        }
    }
}

fn render_path_components(
    release_instance: &ReleaseInstance,
    album_artist: &str,
    album_title: &str,
    profile: &ExportProfile,
    edition_qualifier: Option<&str>,
) -> Vec<String> {
    let mut leaf = album_title.to_string();
    if matches!(
        profile.technical_visibility,
        QualifierVisibilityPolicy::PathOnly | QualifierVisibilityPolicy::TagsAndPath
    ) {
        leaf.push_str(&format!(" [{}]", technical_descriptor(release_instance)));
    }
    if matches!(profile.edition_visibility, EditionVisibilityPolicy::Hidden)
        && let Some(edition_qualifier) = edition_qualifier
    {
        leaf.push_str(&format!(" [{edition_qualifier}]"));
    }
    vec![album_artist.to_string(), leaf]
}

fn compatibility_warnings(
    profile: &ExportProfile,
    release_instance: &ReleaseInstance,
    edition_qualifier: Option<&str>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if matches!(profile.edition_visibility, EditionVisibilityPolicy::Hidden)
        && edition_qualifier.is_none()
    {
        warnings.push("edition distinguishability depends on path rendering".to_string());
    }
    if matches!(
        profile.technical_visibility,
        QualifierVisibilityPolicy::Hidden
    ) && release_instance.technical_variant.format_family == FormatFamily::Mp3
    {
        warnings.push("technical qualifiers are hidden for a lossy variant".to_string());
    }
    warnings
}

fn technical_descriptor(release_instance: &ReleaseInstance) -> String {
    let format = match release_instance.technical_variant.format_family {
        FormatFamily::Flac => "FLAC",
        FormatFamily::Mp3 => "MP3",
    };
    let bitrate = match release_instance.technical_variant.bitrate_mode {
        BitrateMode::Lossless => "lossless".to_string(),
        _ => release_instance
            .technical_variant
            .bitrate_kbps
            .map(|value| format!("{value}kbps"))
            .unwrap_or_else(|| "unknown-bitrate".to_string()),
    };
    format!("{format} {bitrate}")
}

fn render_partial_date(date: &PartialDate) -> String {
    match (date.month, date.day) {
        (Some(month), Some(day)) => format!("{:04}-{:02}-{:02}", date.year, month, day),
        (Some(month), None) => format!("{:04}-{:02}", date.year, month),
        (None, _) => format!("{:04}", date.year),
    }
}

fn map_visibility(policy: &EditionVisibilityPolicy) -> QualifierVisibility {
    match policy {
        EditionVisibilityPolicy::Hidden => QualifierVisibility::Hidden,
        EditionVisibilityPolicy::AlbumTitleWhenNeeded => QualifierVisibility::TagsAndPath,
        EditionVisibilityPolicy::AlbumTitleAlways => QualifierVisibility::TagsAndPath,
    }
}

fn map_variant_policy(policy: &QualifierVisibilityPolicy) -> QualifierVisibility {
    match policy {
        QualifierVisibilityPolicy::Hidden => QualifierVisibility::Hidden,
        QualifierVisibilityPolicy::PathOnly => QualifierVisibility::PathOnly,
        QualifierVisibilityPolicy::TagsAndPath => QualifierVisibility::TagsAndPath,
    }
}

fn map_repository_error(error: RepositoryError) -> ExportRenderingError {
    ExportRenderingError {
        kind: match error.kind {
            RepositoryErrorKind::NotFound => ExportRenderingErrorKind::NotFound,
            RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                ExportRenderingErrorKind::Conflict
            }
            RepositoryErrorKind::Storage => ExportRenderingErrorKind::Storage,
        },
        message: error.message,
    }
}

trait ReleaseGroupArtistFallback {
    fn title_artist_fallback(&self) -> String;
}

impl ReleaseGroupArtistFallback for crate::domain::release_group::ReleaseGroup {
    fn title_artist_fallback(&self) -> String {
        "Unknown Artist".to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::application::config::ValidatedRuntimeConfig;
    use crate::application::repository::{
        ExportRepository, ManualOverrideCommandRepository, ManualOverrideRepository,
    };
    use crate::config::AppConfig;
    use crate::domain::manual_override::{ManualOverride, OverrideField, OverrideSubject};
    use crate::domain::release::{Release, ReleaseEdition};
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        ReleaseInstanceState, TechnicalVariant,
    };
    use crate::support::ids::{ReleaseGroupId, ReleaseId, ReleaseInstanceId, SourceId};
    use crate::support::pagination::Page;

    #[test]
    fn service_renders_generic_player_snapshot_with_qualifiers() {
        let repository = InMemoryExportRepository::seeded();
        let service = ExportRenderingService::new(repository.clone());
        let config = ValidatedRuntimeConfig::from_validated_app_config(&AppConfig::default());

        let snapshot = service
            .render_release_instance_snapshot(&config.export, &repository.release_instance_id, 500)
            .expect("render should succeed");

        assert_eq!(snapshot.album_title, "Kid A [2011 CD]");
        assert_eq!(snapshot.album_artist, "Unknown Artist");
        assert_eq!(snapshot.technical_visibility, QualifierVisibility::PathOnly);
        assert_eq!(
            snapshot.path_components,
            vec![
                "Unknown Artist".to_string(),
                "Kid A [2011 CD] [FLAC lossless]".to_string()
            ]
        );
        assert_eq!(
            snapshot.primary_artwork_filename,
            Some("cover.jpg".to_string())
        );
        assert_eq!(
            render_snapshot_golden(&snapshot),
            include_str!("../../tests/golden/export_snapshot_generic_player.txt")
        );
    }

    #[test]
    fn service_applies_manual_override_precedence_to_rendered_output() {
        let repository = InMemoryExportRepository::seeded();
        repository
            .create_manual_override(&ManualOverride {
                id: crate::support::ids::ManualOverrideId::new(),
                subject: OverrideSubject::Release(repository.release_id.clone()),
                field: OverrideField::Title,
                value: "Kid A (Operator Edit)".to_string(),
                note: None,
                created_by: "operator".to_string(),
                created_at_unix_seconds: 200,
            })
            .expect("override should persist");
        repository
            .create_manual_override(&ManualOverride {
                id: crate::support::ids::ManualOverrideId::new(),
                subject: OverrideSubject::ReleaseInstance(repository.release_instance_id.clone()),
                field: OverrideField::ArtworkSelection,
                value: "folder.jpg".to_string(),
                note: None,
                created_by: "operator".to_string(),
                created_at_unix_seconds: 201,
            })
            .expect("override should persist");
        let service = ExportRenderingService::new(repository.clone());
        let config = ValidatedRuntimeConfig::from_validated_app_config(&AppConfig::default());

        let snapshot = service
            .render_release_instance_snapshot(&config.export, &repository.release_instance_id, 600)
            .expect("render should succeed");

        assert_eq!(snapshot.album_title, "Kid A (Operator Edit) [2011 CD]");
        assert_eq!(
            snapshot.primary_artwork_filename,
            Some("folder.jpg".to_string())
        );
        assert_eq!(repository.stored_snapshots().len(), 1);
    }

    #[derive(Clone)]
    struct InMemoryExportRepository {
        release_id: ReleaseId,
        release_instance_id: ReleaseInstanceId,
        release_groups: Arc<HashMap<ReleaseGroupId, ReleaseGroup>>,
        releases: Arc<HashMap<ReleaseId, Release>>,
        release_instances: Arc<HashMap<ReleaseInstanceId, ReleaseInstance>>,
        manual_overrides: Arc<Mutex<Vec<ManualOverride>>>,
        snapshots: Arc<Mutex<Vec<ExportedMetadataSnapshot>>>,
    }

    impl InMemoryExportRepository {
        fn seeded() -> Self {
            let release_group_id = ReleaseGroupId::new();
            let release_id = ReleaseId::new();
            let release_instance_id = ReleaseInstanceId::new();
            let release_group = ReleaseGroup {
                id: release_group_id.clone(),
                primary_artist_id: crate::support::ids::ArtistId::new(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
                musicbrainz_release_group_id: None,
            };
            let release = Release {
                id: release_id.clone(),
                release_group_id: release_group_id.clone(),
                primary_artist_id: release_group.primary_artist_id.clone(),
                title: "Kid A".to_string(),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: ReleaseEdition {
                    edition_title: Some("2011 CD".to_string()),
                    ..ReleaseEdition::default()
                },
            };
            let release_instance = ReleaseInstance {
                id: release_instance_id.clone(),
                import_batch_id: crate::support::ids::ImportBatchId::new(),
                source_id: SourceId::new(),
                release_id: Some(release_id.clone()),
                state: ReleaseInstanceState::Matched,
                technical_variant: TechnicalVariant {
                    format_family: FormatFamily::Flac,
                    bitrate_mode: BitrateMode::Lossless,
                    bitrate_kbps: None,
                    sample_rate_hz: Some(44_100),
                    bit_depth: Some(16),
                    track_count: 10,
                    total_duration_seconds: 2_900,
                },
                provenance: ProvenanceSnapshot {
                    ingest_origin: IngestOrigin::ManualAdd,
                    original_source_path: "/incoming/Kid A".to_string(),
                    imported_at_unix_seconds: 100,
                    gazelle_reference: None,
                },
            };
            Self {
                release_id: release_id.clone(),
                release_instance_id: release_instance_id.clone(),
                release_groups: Arc::new(HashMap::from([(release_group_id, release_group)])),
                releases: Arc::new(HashMap::from([(release_id, release)])),
                release_instances: Arc::new(HashMap::from([(
                    release_instance_id,
                    release_instance,
                )])),
                manual_overrides: Arc::new(Mutex::new(Vec::new())),
                snapshots: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn stored_snapshots(&self) -> Vec<ExportedMetadataSnapshot> {
            self.snapshots
                .lock()
                .expect("snapshots should lock")
                .clone()
        }
    }

    impl ReleaseInstanceRepository for InMemoryExportRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(self.release_instances.get(id).cloned())
        }

        fn list_release_instances(
            &self,
            _query: &crate::application::repository::ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn list_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _page: &PageRequest,
        ) -> Result<Page<crate::domain::candidate_match::CandidateMatch>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn get_candidate_match(
            &self,
            _id: &crate::support::ids::CandidateMatchId,
        ) -> Result<Option<crate::domain::candidate_match::CandidateMatch>, RepositoryError>
        {
            unimplemented!("not needed in export tests")
        }

        fn list_track_instances_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<crate::domain::track_instance::TrackInstance>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _role: Option<crate::domain::file::FileRole>,
        ) -> Result<Vec<crate::domain::file::FileRecord>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }
    }

    impl ReleaseRepository for InMemoryExportRepository {
        fn find_artist_by_musicbrainz_id(
            &self,
            _musicbrainz_artist_id: &str,
        ) -> Result<Option<crate::domain::artist::Artist>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn get_release_group(
            &self,
            id: &ReleaseGroupId,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(self.release_groups.get(id).cloned())
        }

        fn find_release_group_by_musicbrainz_id(
            &self,
            _musicbrainz_release_group_id: &str,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn get_release(&self, id: &ReleaseId) -> Result<Option<Release>, RepositoryError> {
            Ok(self.releases.get(id).cloned())
        }

        fn find_release_by_musicbrainz_id(
            &self,
            _musicbrainz_release_id: &str,
        ) -> Result<Option<Release>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn search_release_groups(
            &self,
            _query: &crate::application::repository::ReleaseGroupSearchQuery,
        ) -> Result<Page<ReleaseGroup>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn list_releases(
            &self,
            _query: &crate::application::repository::ReleaseListQuery,
        ) -> Result<Page<Release>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn list_tracks_for_release(
            &self,
            _release_id: &ReleaseId,
        ) -> Result<Vec<crate::domain::track::Track>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }
    }

    impl ManualOverrideRepository for InMemoryExportRepository {
        fn get_manual_override(
            &self,
            _id: &crate::support::ids::ManualOverrideId,
        ) -> Result<Option<ManualOverride>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn list_manual_overrides(
            &self,
            query: &ManualOverrideListQuery,
        ) -> Result<Page<ManualOverride>, RepositoryError> {
            let items = self
                .manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .iter()
                .filter(|override_record| {
                    query
                        .subject
                        .as_ref()
                        .is_none_or(|subject| &override_record.subject == subject)
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

    impl ManualOverrideCommandRepository for InMemoryExportRepository {
        fn create_manual_override(
            &self,
            override_record: &ManualOverride,
        ) -> Result<(), RepositoryError> {
            self.manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .push(override_record.clone());
            Ok(())
        }
    }

    impl ExportCommandRepository for InMemoryExportRepository {
        fn create_exported_metadata_snapshot(
            &self,
            snapshot: &ExportedMetadataSnapshot,
        ) -> Result<(), RepositoryError> {
            self.snapshots
                .lock()
                .expect("snapshots should lock")
                .push(snapshot.clone());
            Ok(())
        }

        fn update_exported_metadata_snapshot(
            &self,
            snapshot: &ExportedMetadataSnapshot,
        ) -> Result<(), RepositoryError> {
            let mut snapshots = self.snapshots.lock().expect("snapshots should lock");
            let stored = snapshots
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

    impl ExportRepository for InMemoryExportRepository {
        fn get_latest_exported_metadata(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn list_exported_metadata(
            &self,
            _query: &crate::application::repository::ExportedMetadataListQuery,
        ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }

        fn get_exported_metadata(
            &self,
            _id: &ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            unimplemented!("not needed in export tests")
        }
    }

    fn render_snapshot_golden(snapshot: &ExportedMetadataSnapshot) -> String {
        format!(
            concat!(
                "album_title={}\n",
                "album_artist={}\n",
                "artist_credits={}\n",
                "edition_visibility={:?}\n",
                "technical_visibility={:?}\n",
                "path_components={}\n",
                "primary_artwork_filename={}\n",
                "compatibility_verified={}\n",
                "compatibility_warnings={}\n"
            ),
            snapshot.album_title,
            snapshot.album_artist,
            snapshot.artist_credits.join("; "),
            snapshot.edition_visibility,
            snapshot.technical_visibility,
            snapshot.path_components.join(" | "),
            snapshot.primary_artwork_filename.as_deref().unwrap_or(""),
            snapshot.compatibility.verified,
            snapshot.compatibility.warnings.join(" | "),
        )
    }
}
