use crate::api::inspection::{
    ExportPreviewResource, InspectionApi, ListReleaseInstancesRequest, ListReleasesRequest,
    ReleaseDetailResource, ReleaseGroupResource, ReleaseInstanceResource, ReleaseSummaryResource,
    SearchReleaseGroupsRequest,
};
use crate::application::repository::{
    ExportRepository, ReleaseInstanceRepository, ReleaseRepository,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LibrarySearchFilters {
    pub text: Option<String>,
    pub primary_artist_name: Option<String>,
    pub selected_release_group_id: Option<String>,
    pub limit: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibrarySearchScreen {
    pub filters: LibrarySearchFilters,
    pub release_groups: Vec<ReleaseGroupResource>,
    pub releases: Vec<ReleaseSummaryResource>,
    pub total_release_groups: u64,
    pub total_releases: u64,
}

pub struct LibrarySearchScreenLoader<R> {
    repository: R,
}

impl<R> LibrarySearchScreenLoader<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> LibrarySearchScreenLoader<R>
where
    R: ReleaseRepository + ReleaseInstanceRepository + ExportRepository + Clone,
{
    pub fn load(
        &self,
        request_id: impl Into<String>,
        filters: LibrarySearchFilters,
    ) -> Result<LibrarySearchScreen, String> {
        let request_id = request_id.into();
        let groups = InspectionApi::new(self.repository.clone())
            .search_release_groups(
                request_id.clone(),
                SearchReleaseGroupsRequest {
                    text: filters.text.clone(),
                    primary_artist_name: filters.primary_artist_name.clone(),
                    limit: normalize_limit(filters.limit),
                    offset: filters.offset,
                },
            )
            .map_err(|envelope| error_message(*envelope))?
            .clone();
        let releases = InspectionApi::new(self.repository.clone())
            .list_releases(
                request_id,
                ListReleasesRequest {
                    release_group_id: filters.selected_release_group_id.clone(),
                    text: filters.text.clone(),
                    limit: normalize_limit(filters.limit),
                    offset: filters.offset,
                },
            )
            .map_err(|envelope| error_message(*envelope))?
            .clone();

        Ok(LibrarySearchScreen {
            filters,
            release_groups: groups.data.unwrap_or_default(),
            releases: releases.data.unwrap_or_default(),
            total_release_groups: groups.meta.pagination.map(|value| value.total).unwrap_or(0),
            total_releases: releases
                .meta
                .pagination
                .map(|value| value.total)
                .unwrap_or(0),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseDetailScreen {
    pub release: ReleaseDetailResource,
    pub sibling_instances: Vec<ReleaseInstanceResource>,
}

pub struct ReleaseDetailScreenLoader<R> {
    repository: R,
}

impl<R> ReleaseDetailScreenLoader<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> ReleaseDetailScreenLoader<R>
where
    R: ReleaseRepository + ReleaseInstanceRepository + ExportRepository + Clone,
{
    pub fn load(
        &self,
        request_id: impl Into<String>,
        release_id: &str,
    ) -> Result<ReleaseDetailScreen, String> {
        let request_id = request_id.into();
        let release = InspectionApi::new(self.repository.clone())
            .get_release(request_id.clone(), release_id)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "release detail response was empty".to_string())?;
        let sibling_instances = InspectionApi::new(self.repository.clone())
            .list_release_instances(
                request_id,
                ListReleaseInstancesRequest {
                    release_id: Some(release_id.to_string()),
                    state: None,
                    format_family: None,
                    limit: 50,
                    offset: 0,
                },
            )
            .map_err(|envelope| error_message(*envelope))?
            .data
            .unwrap_or_default();
        Ok(ReleaseDetailScreen {
            release,
            sibling_instances,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseInstanceDetailScreen {
    pub release_instance: ReleaseInstanceResource,
    pub export_preview: Option<ExportPreviewResource>,
}

pub struct ReleaseInstanceDetailScreenLoader<R> {
    repository: R,
}

impl<R> ReleaseInstanceDetailScreenLoader<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> ReleaseInstanceDetailScreenLoader<R>
where
    R: ReleaseRepository + ReleaseInstanceRepository + ExportRepository + Clone,
{
    pub fn load(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
    ) -> Result<ReleaseInstanceDetailScreen, String> {
        let request_id = request_id.into();
        let release_instance = InspectionApi::new(self.repository.clone())
            .get_release_instance(request_id.clone(), release_instance_id)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "release instance response was empty".to_string())?;
        let export_preview = InspectionApi::new(self.repository.clone())
            .get_export_preview(request_id, release_instance_id)
            .ok()
            .and_then(|envelope| envelope.data);
        Ok(ReleaseInstanceDetailScreen {
            release_instance,
            export_preview,
        })
    }
}

fn normalize_limit(limit: u32) -> u32 {
    if limit == 0 { 50 } else { limit }
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
    use crate::domain::release::{PartialDate, Release, ReleaseEdition};
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        ReleaseInstanceState, TechnicalVariant,
    };
    use crate::domain::track::{Track, TrackPosition};
    use crate::support::ids::{ReleaseGroupId, ReleaseId, ReleaseInstanceId, TrackId};
    use crate::support::pagination::{Page, PageRequest};

    #[test]
    fn library_search_loads_groups_and_releases() {
        let repository = InMemoryInspectRepository::seeded();
        let screen = LibrarySearchScreenLoader::new(repository)
            .load(
                "req_search",
                LibrarySearchFilters {
                    text: Some("kid".to_string()),
                    primary_artist_name: Some("radio".to_string()),
                    selected_release_group_id: Some(
                        "11111111-1111-1111-1111-111111111111".to_string(),
                    ),
                    limit: 25,
                    offset: 0,
                },
            )
            .expect("search screen should load");

        assert_eq!(screen.release_groups.len(), 1);
        assert_eq!(screen.releases.len(), 1);
        assert_eq!(screen.release_groups[0].title, "Kid A");
        assert_eq!(screen.releases[0].title, "Kid A");
    }

    #[test]
    fn release_detail_loads_tracks_and_sibling_instances() {
        let repository = InMemoryInspectRepository::seeded();
        let screen = ReleaseDetailScreenLoader::new(repository)
            .load("req_release", "22222222-2222-2222-2222-222222222222")
            .expect("release detail should load");

        assert_eq!(screen.release.tracks.len(), 2);
        assert_eq!(screen.sibling_instances.len(), 1);
        assert_eq!(screen.release.release.title, "Kid A");
    }

    #[test]
    fn release_instance_detail_loads_export_preview_when_available() {
        let repository = InMemoryInspectRepository::seeded();
        let screen = ReleaseInstanceDetailScreenLoader::new(repository)
            .load(
                "req_release_instance",
                "33333333-3333-3333-3333-333333333333",
            )
            .expect("release instance detail should load");

        assert_eq!(
            screen.release_instance.state,
            crate::api::inspection::ReleaseInstanceStateValue::Verified
        );
        assert_eq!(
            screen
                .export_preview
                .expect("export preview should exist")
                .export_profile,
            "generic_player"
        );
    }

    #[derive(Clone)]
    struct InMemoryInspectRepository {
        release_group: ReleaseGroup,
        release: Release,
        release_instance: ReleaseInstance,
        tracks: Vec<Track>,
        export: ExportedMetadataSnapshot,
    }

    impl InMemoryInspectRepository {
        fn seeded() -> Self {
            let release_group = ReleaseGroup {
                id: ReleaseGroupId::parse_str("11111111-1111-1111-1111-111111111111")
                    .expect("uuid should parse"),
                primary_artist_id: crate::support::ids::ArtistId::new(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
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
                edition: ReleaseEdition {
                    edition_title: Some("2000 CD".to_string()),
                    disambiguation: None,
                    country: Some("GB".to_string()),
                    label: Some("Parlophone".to_string()),
                    catalog_number: Some("7243".to_string()),
                    release_date: Some(PartialDate {
                        year: 2000,
                        month: Some(10),
                        day: Some(2),
                    }),
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
            let tracks = vec![
                Track {
                    id: TrackId::new(),
                    release_id: release.id.clone(),
                    position: TrackPosition {
                        disc_number: 1,
                        track_number: 1,
                    },
                    title: "Everything In Its Right Place".to_string(),
                    musicbrainz_track_id: None,
                    duration_ms: Some(250_000),
                },
                Track {
                    id: TrackId::new(),
                    release_id: release.id.clone(),
                    position: TrackPosition {
                        disc_number: 1,
                        track_number: 2,
                    },
                    title: "Kid A".to_string(),
                    musicbrainz_track_id: None,
                    duration_ms: Some(290_000),
                },
            ];
            let export = ExportedMetadataSnapshot {
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
                rendered_at_unix_seconds: 120,
            };
            Self {
                release_group,
                release,
                release_instance,
                tracks,
                export,
            }
        }
    }

    impl ReleaseRepository for InMemoryInspectRepository {
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
            release_id: &ReleaseId,
        ) -> Result<Vec<Track>, RepositoryError> {
            Ok(if self.release.id == *release_id {
                self.tracks.clone()
            } else {
                Vec::new()
            })
        }
    }

    impl ReleaseInstanceRepository for InMemoryInspectRepository {
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
            let mut items = vec![self.release_instance.clone()];
            if let Some(release_id) = &query.release_id {
                items.retain(|item| item.release_id.as_ref() == Some(release_id));
            }
            Ok(Page {
                items,
                request: query.page,
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
            unreachable!()
        }

        fn get_candidate_match(
            &self,
            _id: &crate::support::ids::CandidateMatchId,
        ) -> Result<Option<crate::domain::candidate_match::CandidateMatch>, RepositoryError>
        {
            unreachable!()
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

    impl ExportRepository for InMemoryInspectRepository {
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
