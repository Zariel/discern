use crate::api::review::{
    CandidateMatchResource, ListCandidateMatchesRequest, ManualOverrideResource,
    MatchResolutionResource, ResolveMatchRequest, ReviewApi, SelectCandidateMatchRequest,
    UpdateReleaseInstanceMetadataRequest, UpdateReleaseMetadataRequest,
    UpdateTrackInstanceMetadataRequest,
};
use crate::application::matching::{DiscogsMetadataProvider, MusicBrainzMetadataProvider};
use crate::application::repository::{
    ImportBatchRepository, IngestEvidenceRepository, IssueCommandRepository, IssueRepository,
    ManualOverrideCommandRepository, ManualOverrideRepository, MetadataSnapshotCommandRepository,
    ReleaseCommandRepository, ReleaseInstanceCommandRepository, ReleaseInstanceRepository,
    ReleaseRepository, SourceRepository, StagingManifestRepository,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CandidateReviewFilters {
    pub selected_candidate_id: Option<String>,
    pub limit: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateReviewScreen {
    pub release_instance_id: String,
    pub filters: CandidateReviewFilters,
    pub candidates: Vec<CandidateMatchResource>,
    pub selected_candidate: Option<CandidateMatchResource>,
    pub total_candidates: u64,
}

pub struct CandidateReviewScreenLoader<R, P> {
    repository: R,
    provider: P,
}

impl<R, P> CandidateReviewScreenLoader<R, P> {
    pub fn new(repository: R, provider: P) -> Self {
        Self {
            repository,
            provider,
        }
    }
}

impl<R, P> CandidateReviewScreenLoader<R, P>
where
    R: Clone
        + ImportBatchRepository
        + IngestEvidenceRepository
        + IssueCommandRepository
        + IssueRepository
        + ManualOverrideCommandRepository
        + ManualOverrideRepository
        + MetadataSnapshotCommandRepository
        + ReleaseCommandRepository
        + ReleaseInstanceCommandRepository
        + ReleaseInstanceRepository
        + ReleaseRepository
        + SourceRepository
        + StagingManifestRepository,
    P: Clone + MusicBrainzMetadataProvider + DiscogsMetadataProvider,
{
    pub fn load(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        filters: CandidateReviewFilters,
    ) -> Result<CandidateReviewScreen, String> {
        let request_id = request_id.into();
        let envelope = ReviewApi::new(self.repository.clone(), self.provider.clone())
            .list_candidate_matches(
                request_id,
                release_instance_id,
                ListCandidateMatchesRequest {
                    limit: normalize_limit(filters.limit),
                    offset: filters.offset,
                },
            )
            .map_err(|envelope| error_message(*envelope))?;
        let candidates = envelope.data.unwrap_or_default();
        let selected_candidate = filters
            .selected_candidate_id
            .as_ref()
            .and_then(|selected_id| {
                candidates
                    .iter()
                    .find(|item| &item.id == selected_id)
                    .cloned()
            });
        Ok(CandidateReviewScreen {
            release_instance_id: release_instance_id.to_string(),
            filters,
            total_candidates: envelope
                .meta
                .pagination
                .map(|value| value.total)
                .unwrap_or(candidates.len() as u64),
            candidates,
            selected_candidate,
        })
    }

    pub async fn select_candidate(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        candidate_id: &str,
        request: SelectCandidateMatchRequest,
    ) -> Result<MatchResolutionResource, String> {
        ReviewApi::new(self.repository.clone(), self.provider.clone())
            .select_candidate_match(request_id, release_instance_id, candidate_id, request)
            .await
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "candidate selection response was empty".to_string())
    }

    pub fn resolve_match(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        request: ResolveMatchRequest,
    ) -> Result<MatchResolutionResource, String> {
        ReviewApi::new(self.repository.clone(), self.provider.clone())
            .resolve_match(request_id, release_instance_id, request)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "manual match resolution response was empty".to_string())
    }

    pub fn update_release_metadata(
        &self,
        request_id: impl Into<String>,
        release_id: &str,
        request: UpdateReleaseMetadataRequest,
    ) -> Result<Vec<ManualOverrideResource>, String> {
        ReviewApi::new(self.repository.clone(), self.provider.clone())
            .update_release_metadata(request_id, release_id, request)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "release override response was empty".to_string())
    }

    pub fn update_release_instance_metadata(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        request: UpdateReleaseInstanceMetadataRequest,
    ) -> Result<Vec<ManualOverrideResource>, String> {
        ReviewApi::new(self.repository.clone(), self.provider.clone())
            .update_release_instance_metadata(request_id, release_instance_id, request)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "release instance override response was empty".to_string())
    }

    pub fn update_track_instance_metadata(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        track_instance_id: &str,
        request: UpdateTrackInstanceMetadataRequest,
    ) -> Result<Vec<ManualOverrideResource>, String> {
        ReviewApi::new(self.repository.clone(), self.provider.clone())
            .update_track_instance_metadata(
                request_id,
                release_instance_id,
                track_instance_id,
                request,
            )
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "track override response was empty".to_string())
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
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::application::matching::{
        DiscogsReleaseCandidate, DiscogsReleaseQuery, MusicBrainzArtistCredit,
        MusicBrainzLabelInfo, MusicBrainzReleaseCandidate, MusicBrainzReleaseDetail,
        MusicBrainzReleaseGroupCandidate, MusicBrainzReleaseGroupRef,
    };
    use crate::application::repository::{
        ExportRepository, ExportedMetadataListQuery, ImportBatchListQuery, IssueListQuery,
        ManualOverrideListQuery, ReleaseGroupSearchQuery, ReleaseInstanceListQuery,
        ReleaseListQuery, RepositoryError, RepositoryErrorKind,
    };
    use crate::domain::artist::Artist;
    use crate::domain::candidate_match::{
        CandidateMatch, CandidateProvider, CandidateScore, CandidateSubject, EvidenceKind,
        EvidenceNote, ProviderProvenance,
    };
    use crate::domain::file::{FileRecord, FileRole};
    use crate::domain::import_batch::ImportBatch;
    use crate::domain::issue::{Issue, IssueSubject, IssueType};
    use crate::domain::manual_override::ManualOverride;
    use crate::domain::metadata_snapshot::MetadataSnapshot;
    use crate::domain::release::{Release, ReleaseEdition};
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        ReleaseInstanceState, TechnicalVariant,
    };
    use crate::domain::source::Source;
    use crate::domain::staging_manifest::StagingManifest;
    use crate::domain::track_instance::{AudioProperties, TrackInstance};
    use crate::support::ids::{
        CandidateMatchId, ReleaseGroupId, ReleaseId, ReleaseInstanceId, TrackId, TrackInstanceId,
    };
    use crate::support::pagination::{Page, PageRequest};

    #[test]
    fn candidate_review_loads_candidates_and_selected_item() {
        let repository = InMemoryReviewRepository::seeded();
        let selected_candidate_id = repository.first_candidate_id();
        let screen = CandidateReviewScreenLoader::new(repository, TestMetadataProvider)
            .load(
                "req_review",
                "33333333-3333-3333-3333-333333333333",
                CandidateReviewFilters {
                    selected_candidate_id: Some(selected_candidate_id.clone()),
                    limit: 10,
                    offset: 0,
                },
            )
            .expect("review screen should load");

        assert_eq!(screen.total_candidates, 2);
        assert_eq!(screen.candidates.len(), 2);
        assert_eq!(
            screen
                .selected_candidate
                .expect("selected candidate should exist")
                .id,
            selected_candidate_id
        );
    }

    #[tokio::test]
    async fn candidate_review_selects_candidate_through_review_api() {
        let repository = InMemoryReviewRepository::seeded();
        let candidate_id = repository.first_candidate_id();
        let resolution = CandidateReviewScreenLoader::new(repository.clone(), TestMetadataProvider)
            .select_candidate(
                "req_select",
                "33333333-3333-3333-3333-333333333333",
                &candidate_id,
                SelectCandidateMatchRequest {
                    selected_by: "operator".to_string(),
                    note: Some("preferred score".to_string()),
                    selected_at_unix_seconds: 240,
                },
            )
            .await
            .expect("candidate selection should succeed");

        assert_eq!(
            resolution.release_instance_id,
            "33333333-3333-3333-3333-333333333333"
        );
        assert_eq!(resolution.selected_candidate_id, Some(candidate_id));
        assert_eq!(
            repository.stored_release_instance().state,
            ReleaseInstanceState::Matched
        );
    }

    #[test]
    fn candidate_review_applies_manual_resolution_and_overrides() {
        let repository = InMemoryReviewRepository::seeded();
        let loader = CandidateReviewScreenLoader::new(repository.clone(), TestMetadataProvider);

        let resolution = loader
            .resolve_match(
                "req_resolve",
                "33333333-3333-3333-3333-333333333333",
                ResolveMatchRequest {
                    release_id: repository.existing_release_id.as_uuid().to_string(),
                    selected_by: "operator".to_string(),
                    note: Some("manual confirmation".to_string()),
                    selected_at_unix_seconds: 300,
                },
            )
            .expect("manual resolution should succeed");
        let release_overrides = loader
            .update_release_metadata(
                "req_release_override",
                &repository.existing_release_id.as_uuid().to_string(),
                UpdateReleaseMetadataRequest {
                    title: Some("Kid A (Operator)".to_string()),
                    album_artist: None,
                    artist_credit: None,
                    release_date: None,
                    edition_qualifier: Some("2000 CD".to_string()),
                    updated_by: "operator".to_string(),
                    note: Some("corrected title".to_string()),
                    updated_at_unix_seconds: 320,
                },
            )
            .expect("release overrides should succeed");
        let release_instance_overrides = loader
            .update_release_instance_metadata(
                "req_instance_override",
                "33333333-3333-3333-3333-333333333333",
                UpdateReleaseInstanceMetadataRequest {
                    artwork_selection: Some("cover-front".to_string()),
                    updated_by: "operator".to_string(),
                    note: Some("preferred artwork".to_string()),
                    updated_at_unix_seconds: 330,
                },
            )
            .expect("release instance overrides should succeed");
        let track_overrides = loader
            .update_track_instance_metadata(
                "req_track_override",
                "33333333-3333-3333-3333-333333333333",
                "44444444-4444-4444-4444-444444444444",
                UpdateTrackInstanceMetadataRequest {
                    title: Some("Everything in Its Right Place".to_string()),
                    updated_by: "operator".to_string(),
                    note: Some("capitalization".to_string()),
                    updated_at_unix_seconds: 340,
                },
            )
            .expect("track overrides should succeed");

        assert_eq!(
            resolution.release_id,
            repository.existing_release_id.as_uuid().to_string()
        );
        assert_eq!(release_overrides.len(), 2);
        assert_eq!(release_instance_overrides.len(), 1);
        assert_eq!(track_overrides.len(), 1);
        assert_eq!(repository.stored_manual_overrides().len(), 5);
    }

    #[derive(Clone)]
    struct InMemoryReviewRepository {
        release_instance: ReleaseInstance,
        existing_release_id: ReleaseId,
        release_groups: Arc<Mutex<HashMap<String, ReleaseGroup>>>,
        releases: Arc<Mutex<HashMap<String, Release>>>,
        artists: Arc<Mutex<HashMap<String, Artist>>>,
        release_instances: Arc<Mutex<HashMap<String, ReleaseInstance>>>,
        candidate_matches: Arc<Mutex<Vec<CandidateMatch>>>,
        track_instances: Arc<Mutex<Vec<TrackInstance>>>,
        manual_overrides: Arc<Mutex<Vec<ManualOverride>>>,
        issues: Arc<Mutex<Vec<Issue>>>,
    }

    impl InMemoryReviewRepository {
        fn seeded() -> Self {
            let existing_group = ReleaseGroup {
                id: ReleaseGroupId::parse_str("11111111-1111-1111-1111-111111111111")
                    .expect("uuid should parse"),
                primary_artist_id: crate::support::ids::ArtistId::new(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
                musicbrainz_release_group_id: None,
            };
            let existing_release = Release {
                id: ReleaseId::parse_str("22222222-2222-2222-2222-222222222222")
                    .expect("uuid should parse"),
                release_group_id: existing_group.id.clone(),
                primary_artist_id: existing_group.primary_artist_id.clone(),
                title: "Kid A".to_string(),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: ReleaseEdition {
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
                release_id: None,
                state: ReleaseInstanceState::NeedsReview,
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
                    original_source_path: "/tmp/kid-a".to_string(),
                    imported_at_unix_seconds: 100,
                    gazelle_reference: None,
                },
            };
            let track_instance = TrackInstance {
                id: TrackInstanceId::parse_str("44444444-4444-4444-4444-444444444444")
                    .expect("uuid should parse"),
                release_instance_id: release_instance.id.clone(),
                track_id: TrackId::new(),
                observed_position: crate::domain::track::TrackPosition {
                    disc_number: 1,
                    track_number: 1,
                },
                observed_title: Some("Everything In Its Right Place".to_string()),
                audio_properties: AudioProperties {
                    format_family: FormatFamily::Flac,
                    duration_ms: Some(250_000),
                    bitrate_kbps: None,
                    sample_rate_hz: Some(44_100),
                    bit_depth: Some(16),
                },
            };
            let candidate_matches = vec![
                CandidateMatch {
                    id: CandidateMatchId::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                        .expect("uuid should parse"),
                    release_instance_id: release_instance.id.clone(),
                    provider: CandidateProvider::MusicBrainz,
                    subject: CandidateSubject::Release {
                        provider_id: "mb-release-1".to_string(),
                    },
                    normalized_score: CandidateScore::new(0.92),
                    evidence_matches: vec![EvidenceNote {
                        kind: EvidenceKind::AlbumTitleMatch,
                        detail: "title aligned with local tags".to_string(),
                    }],
                    mismatches: Vec::new(),
                    unresolved_ambiguities: Vec::new(),
                    provider_provenance: ProviderProvenance {
                        provider_name: "musicbrainz".to_string(),
                        query: "kid a radiohead".to_string(),
                        fetched_at_unix_seconds: 150,
                    },
                },
                CandidateMatch {
                    id: CandidateMatchId::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                        .expect("uuid should parse"),
                    release_instance_id: release_instance.id.clone(),
                    provider: CandidateProvider::Discogs,
                    subject: CandidateSubject::Release {
                        provider_id: "discogs-42".to_string(),
                    },
                    normalized_score: CandidateScore::new(0.70),
                    evidence_matches: Vec::new(),
                    mismatches: vec![EvidenceNote {
                        kind: EvidenceKind::DateProximity,
                        detail: "year mismatch".to_string(),
                    }],
                    unresolved_ambiguities: vec!["secondary candidate".to_string()],
                    provider_provenance: ProviderProvenance {
                        provider_name: "discogs".to_string(),
                        query: "kid a".to_string(),
                        fetched_at_unix_seconds: 151,
                    },
                },
            ];

            Self {
                release_instance: release_instance.clone(),
                existing_release_id: existing_release.id.clone(),
                release_groups: Arc::new(Mutex::new(HashMap::from([(
                    existing_group.id.as_uuid().to_string(),
                    existing_group,
                )]))),
                releases: Arc::new(Mutex::new(HashMap::from([(
                    existing_release.id.as_uuid().to_string(),
                    existing_release,
                )]))),
                artists: Arc::new(Mutex::new(HashMap::new())),
                release_instances: Arc::new(Mutex::new(HashMap::from([(
                    release_instance.id.as_uuid().to_string(),
                    release_instance.clone(),
                )]))),
                candidate_matches: Arc::new(Mutex::new(candidate_matches)),
                track_instances: Arc::new(Mutex::new(vec![track_instance])),
                manual_overrides: Arc::new(Mutex::new(Vec::new())),
                issues: Arc::new(Mutex::new(vec![Issue::open(
                    IssueType::AmbiguousReleaseMatch,
                    IssueSubject::ReleaseInstance(release_instance.id.clone()),
                    "Ambiguous release match",
                    None,
                    120,
                )])),
            }
        }

        fn first_candidate_id(&self) -> String {
            self.candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .first()
                .expect("candidate should exist")
                .id
                .as_uuid()
                .to_string()
        }

        fn stored_release_instance(&self) -> ReleaseInstance {
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .get(&self.release_instance.id.as_uuid().to_string())
                .expect("release instance should exist")
                .clone()
        }

        fn stored_manual_overrides(&self) -> Vec<ManualOverride> {
            self.manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .clone()
        }
    }

    #[derive(Clone, Default)]
    struct TestMetadataProvider;

    impl MusicBrainzMetadataProvider for TestMetadataProvider {
        async fn search_releases(
            &self,
            _query: &str,
            _limit: u8,
        ) -> Result<Vec<MusicBrainzReleaseCandidate>, String> {
            Ok(Vec::new())
        }

        async fn search_release_groups(
            &self,
            _query: &str,
            _limit: u8,
        ) -> Result<Vec<MusicBrainzReleaseGroupCandidate>, String> {
            Ok(Vec::new())
        }

        async fn lookup_release(
            &self,
            release_id: &str,
        ) -> Result<MusicBrainzReleaseDetail, String> {
            Ok(MusicBrainzReleaseDetail {
                id: release_id.to_string(),
                title: "Kid A".to_string(),
                country: Some("GB".to_string()),
                date: Some("2000-10-02".to_string()),
                artist_credit: vec![MusicBrainzArtistCredit {
                    artist_id: "artist-1".to_string(),
                    artist_name: "Radiohead".to_string(),
                    artist_sort_name: "Radiohead".to_string(),
                }],
                release_group: Some(MusicBrainzReleaseGroupRef {
                    id: "group-1".to_string(),
                    title: "Kid A".to_string(),
                    primary_type: Some("Album".to_string()),
                }),
                label_info: vec![MusicBrainzLabelInfo {
                    catalog_number: Some("7243".to_string()),
                    label_name: Some("Parlophone".to_string()),
                }],
            })
        }
    }

    impl DiscogsMetadataProvider for TestMetadataProvider {
        async fn search_releases(
            &self,
            _query: &DiscogsReleaseQuery,
            _limit: u8,
        ) -> Result<Vec<DiscogsReleaseCandidate>, String> {
            Ok(Vec::new())
        }
    }

    impl ReleaseRepository for InMemoryReviewRepository {
        fn find_artist_by_musicbrainz_id(
            &self,
            musicbrainz_artist_id: &str,
        ) -> Result<Option<Artist>, RepositoryError> {
            Ok(self
                .artists
                .lock()
                .expect("artists should lock")
                .values()
                .find(|artist| {
                    artist
                        .musicbrainz_artist_id
                        .as_ref()
                        .is_some_and(|id| id.as_uuid().to_string() == musicbrainz_artist_id)
                })
                .cloned())
        }

        fn get_release_group(
            &self,
            id: &ReleaseGroupId,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(self
                .release_groups
                .lock()
                .expect("release groups should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn find_release_group_by_musicbrainz_id(
            &self,
            musicbrainz_release_group_id: &str,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(self
                .release_groups
                .lock()
                .expect("release groups should lock")
                .values()
                .find(|group| {
                    group
                        .musicbrainz_release_group_id
                        .as_ref()
                        .is_some_and(|id| id.as_uuid().to_string() == musicbrainz_release_group_id)
                })
                .cloned())
        }

        fn get_release(&self, id: &ReleaseId) -> Result<Option<Release>, RepositoryError> {
            Ok(self
                .releases
                .lock()
                .expect("releases should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn find_release_by_musicbrainz_id(
            &self,
            musicbrainz_release_id: &str,
        ) -> Result<Option<Release>, RepositoryError> {
            Ok(self
                .releases
                .lock()
                .expect("releases should lock")
                .values()
                .find(|release| {
                    release
                        .musicbrainz_release_id
                        .as_ref()
                        .is_some_and(|id| id.as_uuid().to_string() == musicbrainz_release_id)
                })
                .cloned())
        }

        fn search_release_groups(
            &self,
            _query: &ReleaseGroupSearchQuery,
        ) -> Result<Page<ReleaseGroup>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }

        fn list_releases(
            &self,
            _query: &ReleaseListQuery,
        ) -> Result<Page<Release>, RepositoryError> {
            let items = self
                .releases
                .lock()
                .expect("releases should lock")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            Ok(Page {
                total: items.len() as u64,
                items,
                request: PageRequest::default(),
            })
        }

        fn list_tracks_for_release(
            &self,
            _release_id: &ReleaseId,
        ) -> Result<Vec<crate::domain::track::Track>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl ReleaseCommandRepository for InMemoryReviewRepository {
        fn create_artist(&self, artist: &Artist) -> Result<(), RepositoryError> {
            self.artists
                .lock()
                .expect("artists should lock")
                .insert(artist.id.as_uuid().to_string(), artist.clone());
            Ok(())
        }

        fn create_release_group(
            &self,
            release_group: &ReleaseGroup,
        ) -> Result<(), RepositoryError> {
            self.release_groups
                .lock()
                .expect("release groups should lock")
                .insert(
                    release_group.id.as_uuid().to_string(),
                    release_group.clone(),
                );
            Ok(())
        }

        fn create_release(&self, release: &Release) -> Result<(), RepositoryError> {
            self.releases
                .lock()
                .expect("releases should lock")
                .insert(release.id.as_uuid().to_string(), release.clone());
            Ok(())
        }
    }

    impl ReleaseInstanceRepository for InMemoryReviewRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_release_instances(
            &self,
            _query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            let items = self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            Ok(Page {
                total: items.len() as u64,
                items,
                request: PageRequest::default(),
            })
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_candidate_matches(
            &self,
            release_instance_id: &ReleaseInstanceId,
            page: &PageRequest,
        ) -> Result<Page<CandidateMatch>, RepositoryError> {
            let items = self
                .candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .iter()
                .filter(|candidate| candidate.release_instance_id == *release_instance_id)
                .cloned()
                .collect::<Vec<_>>();
            let total = items.len() as u64;
            Ok(Page {
                items: items
                    .into_iter()
                    .skip(page.offset as usize)
                    .take(page.limit as usize)
                    .collect(),
                request: *page,
                total,
            })
        }

        fn get_candidate_match(
            &self,
            id: &CandidateMatchId,
        ) -> Result<Option<CandidateMatch>, RepositoryError> {
            Ok(self
                .candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .iter()
                .find(|candidate| candidate.id == *id)
                .cloned())
        }

        fn list_track_instances_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<TrackInstance>, RepositoryError> {
            Ok(self
                .track_instances
                .lock()
                .expect("track instances should lock")
                .iter()
                .filter(|track| track.release_instance_id == *release_instance_id)
                .cloned()
                .collect())
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _role: Option<FileRole>,
        ) -> Result<Vec<FileRecord>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl ReleaseInstanceCommandRepository for InMemoryReviewRepository {
        fn create_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .insert(
                    release_instance.id.as_uuid().to_string(),
                    release_instance.clone(),
                );
            Ok(())
        }

        fn update_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .insert(
                    release_instance.id.as_uuid().to_string(),
                    release_instance.clone(),
                );
            Ok(())
        }

        fn replace_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _matches: &[CandidateMatch],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_candidate_matches_for_provider(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _provider: &CandidateProvider,
            _matches: &[CandidateMatch],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_track_instances_and_files(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _track_instances: &[TrackInstance],
            _files: &[FileRecord],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl ManualOverrideRepository for InMemoryReviewRepository {
        fn get_manual_override(
            &self,
            id: &crate::support::ids::ManualOverrideId,
        ) -> Result<Option<ManualOverride>, RepositoryError> {
            Ok(self
                .manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .iter()
                .find(|item| item.id == *id)
                .cloned())
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
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl ManualOverrideCommandRepository for InMemoryReviewRepository {
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

    impl IssueRepository for InMemoryReviewRepository {
        fn get_issue(
            &self,
            id: &crate::support::ids::IssueId,
        ) -> Result<Option<Issue>, RepositoryError> {
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
                        .subject
                        .as_ref()
                        .is_none_or(|subject| &issue.subject == subject)
                })
                .filter(|issue| {
                    query
                        .issue_type
                        .as_ref()
                        .is_none_or(|kind| &issue.issue_type == kind)
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

    impl IssueCommandRepository for InMemoryReviewRepository {
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

    impl ImportBatchRepository for InMemoryReviewRepository {
        fn get_import_batch(
            &self,
            _id: &crate::support::ids::ImportBatchId,
        ) -> Result<Option<ImportBatch>, RepositoryError> {
            Ok(None)
        }

        fn list_import_batches(
            &self,
            _query: &ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }
    }

    impl IngestEvidenceRepository for InMemoryReviewRepository {
        fn list_ingest_evidence_for_batch(
            &self,
            _batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<crate::domain::ingest_evidence::IngestEvidenceRecord>, RepositoryError>
        {
            Ok(Vec::new())
        }
    }

    impl MetadataSnapshotCommandRepository for InMemoryReviewRepository {
        fn create_metadata_snapshots(
            &self,
            _snapshots: &[MetadataSnapshot],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl SourceRepository for InMemoryReviewRepository {
        fn get_source(
            &self,
            _id: &crate::support::ids::SourceId,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(None)
        }

        fn find_source_by_locator(
            &self,
            _locator: &crate::domain::source::SourceLocator,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(None)
        }
    }

    impl StagingManifestRepository for InMemoryReviewRepository {
        fn list_staging_manifests_for_batch(
            &self,
            _batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl ExportRepository for InMemoryReviewRepository {
        fn get_latest_exported_metadata(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<
            Option<crate::domain::exported_metadata_snapshot::ExportedMetadataSnapshot>,
            RepositoryError,
        > {
            Ok(None)
        }

        fn list_exported_metadata(
            &self,
            _query: &ExportedMetadataListQuery,
        ) -> Result<
            Page<crate::domain::exported_metadata_snapshot::ExportedMetadataSnapshot>,
            RepositoryError,
        > {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }

        fn get_exported_metadata(
            &self,
            _id: &crate::support::ids::ExportedMetadataSnapshotId,
        ) -> Result<
            Option<crate::domain::exported_metadata_snapshot::ExportedMetadataSnapshot>,
            RepositoryError,
        > {
            Ok(None)
        }
    }
}
