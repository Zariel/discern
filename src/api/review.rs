use serde::{Deserialize, Serialize};

use crate::api::envelope::ApiEnvelope;
use crate::api::error::{ApiError, ApiErrorCode};
use crate::api::inspection::ReleaseInstanceStateValue;
use crate::api::pagination::ApiPaginationMeta;
use crate::application::manual_metadata::{
    ManualMetadataService, ManualMetadataServiceError, ManualMetadataServiceErrorKind,
    OverrideInput,
};
use crate::application::matching::{
    DiscogsMetadataProvider, MatchingServiceError, MatchingServiceErrorKind,
    MusicBrainzMetadataProvider, ReleaseMatchingService, SelectedCandidateMatchReport,
};
use crate::application::repository::{
    IssueCommandRepository, IssueRepository, ManualOverrideCommandRepository,
    ManualOverrideRepository, ReleaseCommandRepository, ReleaseInstanceCommandRepository,
    ReleaseInstanceRepository, RepositoryError, RepositoryErrorKind,
};
use crate::domain::candidate_match::{
    CandidateMatch, CandidateProvider, CandidateSubject, EvidenceKind, EvidenceNote,
};
use crate::domain::manual_override::{ManualOverride, OverrideField};
use crate::support::ids::{CandidateMatchId, ReleaseId, ReleaseInstanceId, TrackInstanceId};
use crate::support::pagination::PageRequest;

pub type ApiResult<T> = Result<ApiEnvelope<T>, Box<ApiEnvelope<()>>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListCandidateMatchesRequest {
    pub limit: u32,
    pub offset: u64,
}

impl Default for ListCandidateMatchesRequest {
    fn default() -> Self {
        Self {
            limit: PageRequest::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectCandidateMatchRequest {
    pub selected_by: String,
    pub note: Option<String>,
    pub selected_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveMatchRequest {
    pub release_id: String,
    pub selected_by: String,
    pub note: Option<String>,
    pub selected_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateReleaseMetadataRequest {
    pub title: Option<String>,
    pub album_artist: Option<String>,
    pub artist_credit: Option<String>,
    pub release_date: Option<String>,
    pub edition_qualifier: Option<String>,
    pub updated_by: String,
    pub note: Option<String>,
    pub updated_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateReleaseInstanceMetadataRequest {
    pub artwork_selection: Option<String>,
    pub updated_by: String,
    pub note: Option<String>,
    pub updated_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateTrackInstanceMetadataRequest {
    pub title: Option<String>,
    pub updated_by: String,
    pub note: Option<String>,
    pub updated_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateMatchResource {
    pub id: String,
    pub release_instance_id: String,
    pub provider: CandidateProviderValue,
    pub subject: CandidateSubjectResource,
    pub normalized_score: f32,
    pub evidence_matches: Vec<EvidenceNoteResource>,
    pub mismatches: Vec<EvidenceNoteResource>,
    pub unresolved_ambiguities: Vec<String>,
    pub provider_name: String,
    pub provider_query: String,
    pub fetched_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateSubjectResource {
    pub kind: String,
    pub provider_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceNoteResource {
    pub kind: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchResolutionResource {
    pub release_instance_id: String,
    pub release_id: String,
    pub state: ReleaseInstanceStateValue,
    pub selected_candidate_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualOverrideResource {
    pub id: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub field: ManualOverrideFieldValue,
    pub value: String,
    pub note: Option<String>,
    pub created_by: String,
    pub created_at_unix_seconds: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateProviderValue {
    MusicBrainz,
    Discogs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManualOverrideFieldValue {
    Title,
    AlbumArtist,
    ArtistCredit,
    TrackTitle,
    ReleaseDate,
    EditionQualifier,
    ArtworkSelection,
    ReleaseMatch,
}

pub struct ReviewApi<R, P> {
    repository: R,
    provider: P,
}

impl<R, P> ReviewApi<R, P> {
    pub fn new(repository: R, provider: P) -> Self {
        Self {
            repository,
            provider,
        }
    }
}

impl<R, P> ReviewApi<R, P>
where
    R: Clone
        + crate::application::repository::ImportBatchRepository
        + crate::application::repository::IngestEvidenceRepository
        + IssueCommandRepository
        + IssueRepository
        + ManualOverrideCommandRepository
        + ManualOverrideRepository
        + crate::application::repository::MetadataSnapshotCommandRepository
        + ReleaseCommandRepository
        + ReleaseInstanceCommandRepository
        + ReleaseInstanceRepository
        + crate::application::repository::ReleaseRepository
        + crate::application::repository::SourceRepository
        + crate::application::repository::StagingManifestRepository,
    P: Clone + MusicBrainzMetadataProvider + DiscogsMetadataProvider,
{
    pub fn list_candidate_matches(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        request: ListCandidateMatchesRequest,
    ) -> ApiResult<Vec<CandidateMatchResource>> {
        let request_id = request_id.into();
        let release_instance_id = parse_release_instance_id(release_instance_id, &request_id)?;
        let page = self
            .repository
            .list_candidate_matches(
                &release_instance_id,
                &PageRequest::new(request.limit, request.offset),
            )
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        let items = page
            .items
            .iter()
            .map(CandidateMatchResource::from)
            .collect();
        Ok(ApiEnvelope::success_with_pagination(
            items,
            request_id,
            ApiPaginationMeta::from_page(&page),
        ))
    }

    pub async fn select_candidate_match(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        candidate_id: &str,
        request: SelectCandidateMatchRequest,
    ) -> ApiResult<MatchResolutionResource> {
        let request_id = request_id.into();
        let release_instance_id = parse_release_instance_id(release_instance_id, &request_id)?;
        let candidate_id = parse_candidate_id(candidate_id, &request_id)?;
        let report = ReleaseMatchingService::new(self.repository.clone(), self.provider.clone())
            .select_candidate_match(
                &release_instance_id,
                &candidate_id,
                &request.selected_by,
                request.note,
                request.selected_at_unix_seconds,
            )
            .await
            .map_err(|error| matching_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            MatchResolutionResource::from(&report),
            request_id,
        ))
    }

    pub fn resolve_match(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        request: ResolveMatchRequest,
    ) -> ApiResult<MatchResolutionResource> {
        let request_id = request_id.into();
        let release_instance_id = parse_release_instance_id(release_instance_id, &request_id)?;
        let release_id = parse_release_id(&request.release_id, &request_id)?;
        let release_instance =
            ReleaseMatchingService::new(self.repository.clone(), self.provider.clone())
                .apply_manual_release_override(
                    &release_instance_id,
                    &release_id,
                    &request.selected_by,
                    request.note,
                    request.selected_at_unix_seconds,
                )
                .map_err(|error| matching_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            MatchResolutionResource {
                release_instance_id: release_instance.id.as_uuid().to_string(),
                release_id: release_id.as_uuid().to_string(),
                state: release_instance.state.into(),
                selected_candidate_id: None,
            },
            request_id,
        ))
    }

    pub fn update_release_metadata(
        &self,
        request_id: impl Into<String>,
        release_id: &str,
        request: UpdateReleaseMetadataRequest,
    ) -> ApiResult<Vec<ManualOverrideResource>> {
        let request_id = request_id.into();
        let release_id = parse_release_id(release_id, &request_id)?;
        let overrides = ManualMetadataService::new(self.repository.clone())
            .apply_release_overrides(
                &release_id,
                release_override_inputs(&request),
                &request.updated_by,
                request.note,
                request.updated_at_unix_seconds,
            )
            .map_err(|error| manual_metadata_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            overrides.iter().map(ManualOverrideResource::from).collect(),
            request_id,
        ))
    }

    pub fn update_release_instance_metadata(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        request: UpdateReleaseInstanceMetadataRequest,
    ) -> ApiResult<Vec<ManualOverrideResource>> {
        let request_id = request_id.into();
        let release_instance_id = parse_release_instance_id(release_instance_id, &request_id)?;
        let overrides = ManualMetadataService::new(self.repository.clone())
            .apply_release_instance_overrides(
                &release_instance_id,
                release_instance_override_inputs(&request),
                &request.updated_by,
                request.note,
                request.updated_at_unix_seconds,
            )
            .map_err(|error| manual_metadata_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            overrides.iter().map(ManualOverrideResource::from).collect(),
            request_id,
        ))
    }

    pub fn update_track_instance_metadata(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
        track_instance_id: &str,
        request: UpdateTrackInstanceMetadataRequest,
    ) -> ApiResult<Vec<ManualOverrideResource>> {
        let request_id = request_id.into();
        let release_instance_id = parse_release_instance_id(release_instance_id, &request_id)?;
        let track_instance_id = parse_track_instance_id(track_instance_id, &request_id)?;
        let overrides = ManualMetadataService::new(self.repository.clone())
            .apply_track_overrides(
                &release_instance_id,
                &track_instance_id,
                track_override_inputs(&request),
                &request.updated_by,
                request.note,
                request.updated_at_unix_seconds,
            )
            .map_err(|error| manual_metadata_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            overrides.iter().map(ManualOverrideResource::from).collect(),
            request_id,
        ))
    }
}

impl From<&CandidateMatch> for CandidateMatchResource {
    fn from(value: &CandidateMatch) -> Self {
        Self {
            id: value.id.as_uuid().to_string(),
            release_instance_id: value.release_instance_id.as_uuid().to_string(),
            provider: value.provider.clone().into(),
            subject: CandidateSubjectResource::from(&value.subject),
            normalized_score: value.normalized_score.value(),
            evidence_matches: value
                .evidence_matches
                .iter()
                .map(EvidenceNoteResource::from)
                .collect(),
            mismatches: value
                .mismatches
                .iter()
                .map(EvidenceNoteResource::from)
                .collect(),
            unresolved_ambiguities: value.unresolved_ambiguities.clone(),
            provider_name: value.provider_provenance.provider_name.clone(),
            provider_query: value.provider_provenance.query.clone(),
            fetched_at_unix_seconds: value.provider_provenance.fetched_at_unix_seconds,
        }
    }
}

impl From<&CandidateSubject> for CandidateSubjectResource {
    fn from(value: &CandidateSubject) -> Self {
        match value {
            CandidateSubject::Release { provider_id } => Self {
                kind: "release".to_string(),
                provider_id: provider_id.clone(),
            },
            CandidateSubject::ReleaseGroup { provider_id } => Self {
                kind: "release_group".to_string(),
                provider_id: provider_id.clone(),
            },
        }
    }
}

impl From<&EvidenceNote> for EvidenceNoteResource {
    fn from(value: &EvidenceNote) -> Self {
        Self {
            kind: evidence_kind_name(&value.kind).to_string(),
            detail: value.detail.clone(),
        }
    }
}

impl From<&SelectedCandidateMatchReport> for MatchResolutionResource {
    fn from(value: &SelectedCandidateMatchReport) -> Self {
        Self {
            release_instance_id: value.release_instance.id.as_uuid().to_string(),
            release_id: value.release.id.as_uuid().to_string(),
            state: value.release_instance.state.clone().into(),
            selected_candidate_id: Some(value.candidate.id.as_uuid().to_string()),
        }
    }
}

impl From<&ManualOverride> for ManualOverrideResource {
    fn from(value: &ManualOverride) -> Self {
        let (subject_kind, subject_id) = match &value.subject {
            crate::domain::manual_override::OverrideSubject::Release(id) => {
                ("release".to_string(), id.as_uuid().to_string())
            }
            crate::domain::manual_override::OverrideSubject::ReleaseInstance(id) => {
                ("release_instance".to_string(), id.as_uuid().to_string())
            }
            crate::domain::manual_override::OverrideSubject::Track(id) => {
                ("track".to_string(), id.as_uuid().to_string())
            }
        };
        Self {
            id: value.id.as_uuid().to_string(),
            subject_kind,
            subject_id,
            field: value.field.clone().into(),
            value: value.value.clone(),
            note: value.note.clone(),
            created_by: value.created_by.clone(),
            created_at_unix_seconds: value.created_at_unix_seconds,
        }
    }
}

impl From<CandidateProvider> for CandidateProviderValue {
    fn from(value: CandidateProvider) -> Self {
        match value {
            CandidateProvider::MusicBrainz => Self::MusicBrainz,
            CandidateProvider::Discogs => Self::Discogs,
        }
    }
}

impl From<CandidateProviderValue> for CandidateProvider {
    fn from(value: CandidateProviderValue) -> Self {
        match value {
            CandidateProviderValue::MusicBrainz => Self::MusicBrainz,
            CandidateProviderValue::Discogs => Self::Discogs,
        }
    }
}

impl From<OverrideField> for ManualOverrideFieldValue {
    fn from(value: OverrideField) -> Self {
        match value {
            OverrideField::Title => Self::Title,
            OverrideField::AlbumArtist => Self::AlbumArtist,
            OverrideField::ArtistCredit => Self::ArtistCredit,
            OverrideField::TrackTitle => Self::TrackTitle,
            OverrideField::ReleaseDate => Self::ReleaseDate,
            OverrideField::EditionQualifier => Self::EditionQualifier,
            OverrideField::ArtworkSelection => Self::ArtworkSelection,
            OverrideField::ReleaseMatch => Self::ReleaseMatch,
        }
    }
}

fn release_override_inputs(request: &UpdateReleaseMetadataRequest) -> Vec<OverrideInput> {
    let mut overrides = Vec::new();
    push_override(&mut overrides, OverrideField::Title, request.title.clone());
    push_override(
        &mut overrides,
        OverrideField::AlbumArtist,
        request.album_artist.clone(),
    );
    push_override(
        &mut overrides,
        OverrideField::ArtistCredit,
        request.artist_credit.clone(),
    );
    push_override(
        &mut overrides,
        OverrideField::ReleaseDate,
        request.release_date.clone(),
    );
    push_override(
        &mut overrides,
        OverrideField::EditionQualifier,
        request.edition_qualifier.clone(),
    );
    overrides
}

fn release_instance_override_inputs(
    request: &UpdateReleaseInstanceMetadataRequest,
) -> Vec<OverrideInput> {
    let mut overrides = Vec::new();
    push_override(
        &mut overrides,
        OverrideField::ArtworkSelection,
        request.artwork_selection.clone(),
    );
    overrides
}

fn track_override_inputs(request: &UpdateTrackInstanceMetadataRequest) -> Vec<OverrideInput> {
    let mut overrides = Vec::new();
    push_override(
        &mut overrides,
        OverrideField::TrackTitle,
        request.title.clone(),
    );
    overrides
}

fn push_override(overrides: &mut Vec<OverrideInput>, field: OverrideField, value: Option<String>) {
    if let Some(value) = value {
        overrides.push(OverrideInput { field, value });
    }
}

fn evidence_kind_name(value: &EvidenceKind) -> &str {
    match value {
        EvidenceKind::ArtistMatch => "artist_match",
        EvidenceKind::AlbumTitleMatch => "album_title_match",
        EvidenceKind::TrackCountMatch => "track_count_match",
        EvidenceKind::DurationAlignment => "duration_alignment",
        EvidenceKind::DiscCountMatch => "disc_count_match",
        EvidenceKind::DateProximity => "date_proximity",
        EvidenceKind::LabelCatalogAlignment => "label_catalog_alignment",
        EvidenceKind::FilenameSimilarity => "filename_similarity",
        EvidenceKind::GazelleConsistency => "gazelle_consistency",
        EvidenceKind::Other(value) => value.as_str(),
    }
}

fn parse_release_instance_id(
    value: &str,
    request_id: &str,
) -> Result<ReleaseInstanceId, Box<ApiEnvelope<()>>> {
    ReleaseInstanceId::parse_str(value)
        .map_err(|_| invalid_id_envelope("release instance", value, request_id))
}

fn parse_candidate_id(
    value: &str,
    request_id: &str,
) -> Result<CandidateMatchId, Box<ApiEnvelope<()>>> {
    CandidateMatchId::parse_str(value)
        .map_err(|_| invalid_id_envelope("candidate", value, request_id))
}

fn parse_release_id(value: &str, request_id: &str) -> Result<ReleaseId, Box<ApiEnvelope<()>>> {
    ReleaseId::parse_str(value).map_err(|_| invalid_id_envelope("release", value, request_id))
}

fn parse_track_instance_id(
    value: &str,
    request_id: &str,
) -> Result<TrackInstanceId, Box<ApiEnvelope<()>>> {
    TrackInstanceId::parse_str(value)
        .map_err(|_| invalid_id_envelope("track instance", value, request_id))
}

fn invalid_id_envelope(kind: &str, value: &str, request_id: &str) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            format!("{kind} id '{value}' is not a valid UUID"),
            None,
        ),
        request_id.to_string(),
    ))
}

fn repository_error_envelope(error: RepositoryError, request_id: String) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            match error.kind {
                RepositoryErrorKind::NotFound => ApiErrorCode::NotFound,
                RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                    ApiErrorCode::Conflict
                }
                RepositoryErrorKind::Storage => ApiErrorCode::InternalError,
            },
            error.message,
            None,
        ),
        request_id,
    ))
}

fn matching_error_envelope(
    error: MatchingServiceError,
    request_id: String,
) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            match error.kind {
                MatchingServiceErrorKind::NotFound => ApiErrorCode::NotFound,
                MatchingServiceErrorKind::Conflict => ApiErrorCode::Conflict,
                MatchingServiceErrorKind::Storage | MatchingServiceErrorKind::Provider => {
                    ApiErrorCode::InternalError
                }
            },
            error.message,
            None,
        ),
        request_id,
    ))
}

fn manual_metadata_error_envelope(
    error: ManualMetadataServiceError,
    request_id: String,
) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            match error.kind {
                ManualMetadataServiceErrorKind::NotFound => ApiErrorCode::NotFound,
                ManualMetadataServiceErrorKind::Conflict => ApiErrorCode::Conflict,
                ManualMetadataServiceErrorKind::Storage => ApiErrorCode::InternalError,
            },
            error.message,
            None,
        ),
        request_id,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::application::matching::{
        DiscogsReleaseCandidate, DiscogsReleaseQuery, MusicBrainzArtistCredit,
        MusicBrainzLabelInfo, MusicBrainzReleaseCandidate, MusicBrainzReleaseDetail,
        MusicBrainzReleaseGroupCandidate, MusicBrainzReleaseGroupRef,
    };
    use crate::application::repository::{
        ExportedMetadataListQuery, ImportBatchListQuery, IssueListQuery, ManualOverrideListQuery,
        MetadataSnapshotCommandRepository, ReleaseGroupSearchQuery, ReleaseInstanceListQuery,
        ReleaseListQuery,
    };
    use crate::domain::artist::Artist;
    use crate::domain::candidate_match::{CandidateScore, ProviderProvenance};
    use crate::domain::file::{FileRecord, FileRole};
    use crate::domain::import_batch::ImportBatch;
    use crate::domain::ingest_evidence::IngestEvidenceRecord;
    use crate::domain::issue::{Issue, IssueSubject, IssueType};
    use crate::domain::job::Job;
    use crate::domain::metadata_snapshot::MetadataSnapshot;
    use crate::domain::release::{Release, ReleaseEdition};
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        ReleaseInstanceState, TechnicalVariant,
    };
    use crate::domain::source::{Source, SourceLocator};
    use crate::domain::staging_manifest::StagingManifest;
    use crate::domain::track::TrackPosition;
    use crate::domain::track_instance::{AudioProperties, TrackInstance};
    use crate::support::ids::{ArtistId, ReleaseGroupId, TrackId};
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn list_candidate_matches_returns_paginated_resources() {
        let repository = InMemoryReviewRepository::seeded();
        let api = ReviewApi::new(repository.clone(), TestMetadataProvider::default());

        let envelope = api
            .list_candidate_matches(
                "req_candidates",
                &repository.release_instance.id.as_uuid().to_string(),
                ListCandidateMatchesRequest {
                    limit: 1,
                    offset: 0,
                    ..ListCandidateMatchesRequest::default()
                },
            )
            .expect("candidate list should succeed");

        let data = envelope.data.expect("data should exist");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].provider, CandidateProviderValue::MusicBrainz);
        assert_eq!(
            envelope
                .meta
                .pagination
                .expect("pagination should exist")
                .next_offset,
            Some(1)
        );
    }

    #[tokio::test]
    async fn select_candidate_match_materializes_release_and_updates_instance() {
        let repository = InMemoryReviewRepository::seeded();
        let api = ReviewApi::new(repository.clone(), TestMetadataProvider::default());
        let candidate_id = repository
            .candidate_matches
            .lock()
            .expect("candidate matches should lock")[0]
            .id
            .as_uuid()
            .to_string();

        let envelope = api
            .select_candidate_match(
                "req_select",
                &repository.release_instance.id.as_uuid().to_string(),
                &candidate_id,
                SelectCandidateMatchRequest {
                    selected_by: "operator".to_string(),
                    note: Some("picked after review".to_string()),
                    selected_at_unix_seconds: 200,
                },
            )
            .await
            .expect("candidate selection should succeed");

        let resolution = envelope.data.expect("data should exist");
        assert_eq!(resolution.selected_candidate_id, Some(candidate_id));
        assert_eq!(resolution.state, ReleaseInstanceStateValue::Matched);
        assert!(repository.stored_release_instance().release_id.is_some());
        assert!(
            repository
                .stored_manual_overrides()
                .iter()
                .any(|item| item.field == OverrideField::ReleaseMatch)
        );
    }

    #[test]
    fn resolve_match_uses_existing_release_id() {
        let repository = InMemoryReviewRepository::seeded();
        let existing_release = repository.existing_release();
        let api = ReviewApi::new(repository.clone(), TestMetadataProvider::default());

        let envelope = api
            .resolve_match(
                "req_resolve",
                &repository.release_instance.id.as_uuid().to_string(),
                ResolveMatchRequest {
                    release_id: existing_release.id.as_uuid().to_string(),
                    selected_by: "operator".to_string(),
                    note: None,
                    selected_at_unix_seconds: 210,
                },
            )
            .expect("manual resolve should succeed");

        let resolution = envelope.data.expect("data should exist");
        assert_eq!(
            resolution.release_id,
            existing_release.id.as_uuid().to_string()
        );
        assert_eq!(resolution.selected_candidate_id, None);
    }

    #[test]
    fn update_release_metadata_creates_override_records() {
        let repository = InMemoryReviewRepository::seeded();
        let release_id = repository.existing_release().id.as_uuid().to_string();
        let api = ReviewApi::new(repository.clone(), TestMetadataProvider::default());

        let envelope = api
            .update_release_metadata(
                "req_release_override",
                &release_id,
                UpdateReleaseMetadataRequest {
                    title: Some("Kid A (Operator Edit)".to_string()),
                    album_artist: None,
                    artist_credit: None,
                    release_date: None,
                    edition_qualifier: Some("2000 XL".to_string()),
                    updated_by: "operator".to_string(),
                    note: Some("normalize export title".to_string()),
                    updated_at_unix_seconds: 300,
                },
            )
            .expect("release override should succeed");

        let overrides = envelope.data.expect("data should exist");
        assert_eq!(overrides.len(), 2);
        assert!(
            overrides
                .iter()
                .any(|item| item.field == ManualOverrideFieldValue::Title)
        );
        assert!(overrides.iter().any(|item| {
            item.field == ManualOverrideFieldValue::EditionQualifier && item.value == "2000 XL"
        }));
    }

    #[test]
    fn update_track_instance_metadata_creates_track_title_override() {
        let repository = InMemoryReviewRepository::seeded();
        let track_id = repository.track_instance.id.as_uuid().to_string();
        let api = ReviewApi::new(repository.clone(), TestMetadataProvider::default());

        let envelope = api
            .update_track_instance_metadata(
                "req_track_override",
                &repository.release_instance.id.as_uuid().to_string(),
                &track_id,
                UpdateTrackInstanceMetadataRequest {
                    title: Some("Everything In Its Right Place".to_string()),
                    updated_by: "operator".to_string(),
                    note: None,
                    updated_at_unix_seconds: 400,
                },
            )
            .expect("track override should succeed");

        let overrides = envelope.data.expect("data should exist");
        assert_eq!(overrides.len(), 1);
        assert_eq!(overrides[0].field, ManualOverrideFieldValue::TrackTitle);
    }

    #[derive(Clone)]
    struct InMemoryReviewRepository {
        release_instance: ReleaseInstance,
        track_instance: TrackInstance,
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
            let artist_id = ArtistId::new();
            let existing_group = ReleaseGroup {
                id: ReleaseGroupId::new(),
                primary_artist_id: artist_id.clone(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
                musicbrainz_release_group_id: None,
            };
            let existing_release = Release {
                id: ReleaseId::new(),
                release_group_id: existing_group.id.clone(),
                primary_artist_id: artist_id.clone(),
                title: "Kid A".to_string(),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: ReleaseEdition::default(),
            };
            let release_instance = ReleaseInstance {
                id: ReleaseInstanceId::new(),
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
                    original_source_path: "/imports/kid-a".to_string(),
                    imported_at_unix_seconds: 100,
                    gazelle_reference: None,
                },
            };
            let track_instance = TrackInstance {
                id: TrackInstanceId::new(),
                release_instance_id: release_instance.id.clone(),
                track_id: TrackId::new(),
                observed_position: TrackPosition {
                    disc_number: 1,
                    track_number: 1,
                },
                observed_title: Some("Everything in Its Right Place".to_string()),
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
                    id: CandidateMatchId::new(),
                    release_instance_id: release_instance.id.clone(),
                    provider: CandidateProvider::MusicBrainz,
                    subject: CandidateSubject::Release {
                        provider_id: "11111111-1111-1111-1111-111111111111".to_string(),
                    },
                    normalized_score: CandidateScore::new(0.95),
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
                    id: CandidateMatchId::new(),
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
                track_instance: track_instance.clone(),
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

        fn existing_release(&self) -> Release {
            self.releases
                .lock()
                .expect("releases should lock")
                .get(&self.existing_release_id.as_uuid().to_string())
                .expect("release should exist")
                .clone()
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
                    artist_id: "22222222-2222-2222-2222-222222222222".to_string(),
                    artist_name: "Radiohead".to_string(),
                    artist_sort_name: "Radiohead".to_string(),
                }],
                release_group: Some(MusicBrainzReleaseGroupRef {
                    id: "33333333-3333-3333-3333-333333333333".to_string(),
                    title: "Kid A".to_string(),
                    primary_type: Some("Album".to_string()),
                }),
                label_info: vec![MusicBrainzLabelInfo {
                    catalog_number: Some("XLCD782".to_string()),
                    label_name: Some("XL".to_string()),
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

    impl crate::application::repository::ReleaseRepository for InMemoryReviewRepository {
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
            Ok(Page {
                items: self
                    .releases
                    .lock()
                    .expect("releases should lock")
                    .values()
                    .cloned()
                    .collect(),
                request: PageRequest::default(),
                total: self.releases.lock().expect("releases should lock").len() as u64,
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
            Ok(Page {
                items: self
                    .release_instances
                    .lock()
                    .expect("release instances should lock")
                    .values()
                    .cloned()
                    .collect(),
                request: PageRequest::default(),
                total: 1,
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
                .filter(|issue| {
                    query
                        .state
                        .as_ref()
                        .is_none_or(|state| &issue.state == state)
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

    impl crate::application::repository::ImportBatchRepository for InMemoryReviewRepository {
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

    impl crate::application::repository::IngestEvidenceRepository for InMemoryReviewRepository {
        fn list_ingest_evidence_for_batch(
            &self,
            _batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<IngestEvidenceRecord>, RepositoryError> {
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

    impl crate::application::repository::SourceRepository for InMemoryReviewRepository {
        fn get_source(
            &self,
            _id: &crate::support::ids::SourceId,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(None)
        }

        fn find_source_by_locator(
            &self,
            _locator: &SourceLocator,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(None)
        }
    }

    impl crate::application::repository::StagingManifestRepository for InMemoryReviewRepository {
        fn list_staging_manifests_for_batch(
            &self,
            _batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl crate::application::repository::ExportRepository for InMemoryReviewRepository {
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

    impl crate::application::repository::JobRepository for InMemoryReviewRepository {
        fn get_job(
            &self,
            _id: &crate::support::ids::JobId,
        ) -> Result<Option<Job>, RepositoryError> {
            Ok(None)
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
}
