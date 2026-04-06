use crate::api::inspection::{ExportPreviewResource, InspectionApi, ReleaseInstanceResource};
use crate::application::repository::{
    ExportRepository, ReleaseInstanceRepository, ReleaseRepository,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportPreviewScreen {
    pub release_instance_id: String,
    pub release_instance: ReleaseInstanceResource,
    pub preview: ExportPreviewResource,
    pub managed_path: String,
    pub artwork_summary: String,
    pub compatibility_warnings: Vec<String>,
}

pub struct ExportPreviewScreenLoader<R> {
    repository: R,
}

impl<R> ExportPreviewScreenLoader<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> ExportPreviewScreenLoader<R>
where
    R: ReleaseRepository + ReleaseInstanceRepository + ExportRepository + Clone,
{
    pub fn load(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
    ) -> Result<ExportPreviewScreen, String> {
        let request_id = request_id.into();
        let release_instance = InspectionApi::new(self.repository.clone())
            .get_release_instance(request_id.clone(), release_instance_id)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "release instance response was empty".to_string())?;
        let preview = InspectionApi::new(self.repository.clone())
            .get_export_preview(request_id, release_instance_id)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "export preview response was empty".to_string())?;

        Ok(ExportPreviewScreen {
            release_instance_id: release_instance_id.to_string(),
            managed_path: render_managed_path(&preview.path_components),
            artwork_summary: render_artwork_summary(&preview),
            compatibility_warnings: preview.compatibility.warnings.clone(),
            release_instance,
            preview,
        })
    }
}

fn render_managed_path(path_components: &[String]) -> String {
    path_components.join("/")
}

fn render_artwork_summary(preview: &ExportPreviewResource) -> String {
    match preview.primary_artwork_filename.as_deref() {
        Some(filename) => format!("sidecar artwork: {filename}"),
        None => "no primary artwork selected".to_string(),
    }
}

fn error_message<T>(envelope: crate::api::envelope::ApiEnvelope<T>) -> String {
    envelope
        .error
        .map(|error| error.message)
        .unwrap_or_else(|| "api request failed".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::repository::{
        ExportedMetadataListQuery, ReleaseGroupSearchQuery, ReleaseInstanceListQuery,
        ReleaseListQuery, RepositoryError,
    };
    use crate::domain::exported_metadata_snapshot::{
        CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
    };
    use crate::domain::release::Release;
    use crate::domain::release_group::ReleaseGroup;
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        ReleaseInstanceState, TechnicalVariant,
    };
    use crate::support::ids::{ReleaseGroupId, ReleaseId, ReleaseInstanceId};
    use crate::support::pagination::{Page, PageRequest};

    #[test]
    fn export_preview_loads_managed_path_and_artwork_summary() {
        let repository = InMemoryPreviewRepository::seeded(Some("cover.jpg".to_string()));
        let screen = ExportPreviewScreenLoader::new(repository)
            .load("req_preview", "33333333-3333-3333-3333-333333333333")
            .expect("export preview should load");

        assert_eq!(screen.preview.album_title, "Kid A [2000]");
        assert_eq!(screen.managed_path, "Radiohead/Kid A [2000 CD] [FLAC]");
        assert_eq!(screen.artwork_summary, "sidecar artwork: cover.jpg");
        assert_eq!(
            screen.compatibility_warnings,
            vec!["edition visible in path"]
        );
    }

    #[test]
    fn export_preview_reports_missing_artwork_conservatively() {
        let repository = InMemoryPreviewRepository::seeded(None);
        let screen = ExportPreviewScreenLoader::new(repository)
            .load(
                "req_preview_missing_art",
                "33333333-3333-3333-3333-333333333333",
            )
            .expect("export preview should load");

        assert_eq!(screen.artwork_summary, "no primary artwork selected");
    }

    #[derive(Clone)]
    struct InMemoryPreviewRepository {
        release: Release,
        release_group: ReleaseGroup,
        release_instance: ReleaseInstance,
        export: ExportedMetadataSnapshot,
    }

    impl InMemoryPreviewRepository {
        fn seeded(primary_artwork_filename: Option<String>) -> Self {
            let release_group = ReleaseGroup {
                id: ReleaseGroupId::parse_str("11111111-1111-1111-1111-111111111111")
                    .expect("uuid should parse"),
                primary_artist_id: crate::support::ids::ArtistId::new(),
                title: "Kid A".to_string(),
                kind: crate::domain::release_group::ReleaseGroupKind::Album,
                musicbrainz_release_group_id: None,
            };
            let release = Release {
                id: ReleaseId::parse_str("22222222-2222-2222-2222-222222222222")
                    .expect("uuid should parse"),
                release_group_id: release_group.id.clone(),
                primary_artist_id: release_group.primary_artist_id.clone(),
                title: "Kid A".to_string(),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: crate::domain::release::ReleaseEdition {
                    edition_title: Some("2000 CD".to_string()),
                    disambiguation: None,
                    country: Some("GB".to_string()),
                    label: Some("Parlophone".to_string()),
                    catalog_number: Some("7243".to_string()),
                    release_date: None,
                },
            };
            let release_instance = ReleaseInstance {
                id: ReleaseInstanceId::parse_str("33333333-3333-3333-3333-333333333333")
                    .expect("uuid should parse"),
                import_batch_id: crate::support::ids::ImportBatchId::new(),
                source_id: crate::support::ids::SourceId::new(),
                release_id: Some(release.id.clone()),
                state: ReleaseInstanceState::Verified,
                technical_variant: TechnicalVariant {
                    format_family: FormatFamily::Flac,
                    bitrate_mode: BitrateMode::Lossless,
                    bitrate_kbps: None,
                    sample_rate_hz: Some(44_100),
                    bit_depth: Some(16),
                    track_count: 2,
                    total_duration_seconds: 600,
                },
                provenance: ProvenanceSnapshot {
                    ingest_origin: IngestOrigin::ManualAdd,
                    original_source_path: "/tmp/kid-a".to_string(),
                    imported_at_unix_seconds: 100,
                    gazelle_reference: None,
                },
            };
            let export = ExportedMetadataSnapshot {
                id: crate::support::ids::ExportedMetadataSnapshotId::new(),
                release_instance_id: release_instance.id.clone(),
                export_profile: "generic_player".to_string(),
                album_title: "Kid A [2000]".to_string(),
                album_artist: "Radiohead".to_string(),
                artist_credits: vec!["Radiohead".to_string()],
                edition_visibility: QualifierVisibility::TagsAndPath,
                technical_visibility: QualifierVisibility::PathOnly,
                path_components: vec![
                    "Radiohead".to_string(),
                    "Kid A [2000 CD] [FLAC]".to_string(),
                ],
                primary_artwork_filename,
                compatibility: CompatibilityReport {
                    verified: true,
                    warnings: vec!["edition visible in path".to_string()],
                },
                rendered_at_unix_seconds: 120,
            };
            Self {
                release,
                release_group,
                release_instance,
                export,
            }
        }
    }

    impl ReleaseRepository for InMemoryPreviewRepository {
        fn find_artist_by_musicbrainz_id(
            &self,
            _musicbrainz_artist_id: &str,
        ) -> Result<Option<crate::domain::artist::Artist>, RepositoryError> {
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
            _query: &ReleaseGroupSearchQuery,
        ) -> Result<Page<ReleaseGroup>, RepositoryError> {
            Ok(Page {
                items: vec![self.release_group.clone()],
                request: PageRequest::default(),
                total: 1,
            })
        }

        fn list_releases(
            &self,
            _query: &ReleaseListQuery,
        ) -> Result<Page<Release>, RepositoryError> {
            Ok(Page {
                items: vec![self.release.clone()],
                request: PageRequest::default(),
                total: 1,
            })
        }

        fn list_tracks_for_release(
            &self,
            _release_id: &ReleaseId,
        ) -> Result<Vec<crate::domain::track::Track>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl ReleaseInstanceRepository for InMemoryPreviewRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok((self.release_instance.id == *id).then_some(self.release_instance.clone()))
        }

        fn list_release_instances(
            &self,
            _query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            Ok(Page {
                items: vec![self.release_instance.clone()],
                request: PageRequest::default(),
                total: 1,
            })
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(vec![self.release_instance.clone()])
        }

        fn list_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _page: &PageRequest,
        ) -> Result<Page<crate::domain::candidate_match::CandidateMatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
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

    impl ExportRepository for InMemoryPreviewRepository {
        fn get_latest_exported_metadata(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok((self.export.release_instance_id == *release_instance_id)
                .then_some(self.export.clone()))
        }

        fn list_exported_metadata(
            &self,
            _query: &ExportedMetadataListQuery,
        ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(Page {
                items: vec![self.export.clone()],
                request: PageRequest::default(),
                total: 1,
            })
        }

        fn get_exported_metadata(
            &self,
            id: &crate::support::ids::ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok((self.export.id == *id).then_some(self.export.clone()))
        }
    }
}
