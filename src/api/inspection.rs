use serde::{Deserialize, Serialize};

use crate::api::envelope::ApiEnvelope;
use crate::api::error::{ApiError, ApiErrorCode};
use crate::api::pagination::ApiPaginationMeta;
use crate::application::repository::{
    ExportRepository, ReleaseGroupSearchQuery, ReleaseInstanceListQuery, ReleaseInstanceRepository,
    ReleaseListQuery, ReleaseRepository, RepositoryError, RepositoryErrorKind,
};
use crate::domain::exported_metadata_snapshot::{
    CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
};
use crate::domain::release::{PartialDate, Release, ReleaseEdition};
use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
use crate::domain::release_instance::{
    BitrateMode, FormatFamily, IngestOrigin, ReleaseInstance, ReleaseInstanceState,
};
use crate::domain::track::Track;
use crate::support::ids::{ReleaseId, ReleaseInstanceId};
use crate::support::pagination::PageRequest;

pub type ApiResult<T> = Result<ApiEnvelope<T>, Box<ApiEnvelope<()>>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListReleasesRequest {
    pub release_group_id: Option<String>,
    pub text: Option<String>,
    pub limit: u32,
    pub offset: u64,
}

impl Default for ListReleasesRequest {
    fn default() -> Self {
        Self {
            release_group_id: None,
            text: None,
            limit: PageRequest::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListReleaseInstancesRequest {
    pub release_id: Option<String>,
    pub state: Option<ReleaseInstanceStateValue>,
    pub format_family: Option<FormatFamilyValue>,
    pub limit: u32,
    pub offset: u64,
}

impl Default for ListReleaseInstancesRequest {
    fn default() -> Self {
        Self {
            release_id: None,
            state: None,
            format_family: None,
            limit: PageRequest::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchReleaseGroupsRequest {
    pub text: Option<String>,
    pub primary_artist_name: Option<String>,
    pub limit: u32,
    pub offset: u64,
}

impl Default for SearchReleaseGroupsRequest {
    fn default() -> Self {
        Self {
            text: None,
            primary_artist_name: None,
            limit: PageRequest::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseSummaryResource {
    pub id: String,
    pub release_group_id: String,
    pub title: String,
    pub musicbrainz_release_id: Option<String>,
    pub discogs_release_id: Option<u64>,
    pub edition: ReleaseEditionResource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseDetailResource {
    pub release: ReleaseSummaryResource,
    pub tracks: Vec<TrackResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseGroupResource {
    pub id: String,
    pub title: String,
    pub kind: String,
    pub musicbrainz_release_group_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseEditionResource {
    pub edition_title: Option<String>,
    pub disambiguation: Option<String>,
    pub country: Option<String>,
    pub label: Option<String>,
    pub catalog_number: Option<String>,
    pub release_date: Option<PartialDateResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialDateResource {
    pub year: u16,
    pub month: Option<u8>,
    pub day: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackResource {
    pub id: String,
    pub disc_number: u16,
    pub track_number: u16,
    pub title: String,
    pub musicbrainz_track_id: Option<String>,
    pub duration_ms: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseInstanceResource {
    pub id: String,
    pub import_batch_id: String,
    pub source_id: String,
    pub release_id: Option<String>,
    pub state: ReleaseInstanceStateValue,
    pub technical_variant: TechnicalVariantResource,
    pub provenance: ProvenanceResource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TechnicalVariantResource {
    pub format_family: FormatFamilyValue,
    pub bitrate_mode: BitrateModeValue,
    pub bitrate_kbps: Option<u32>,
    pub sample_rate_hz: Option<u32>,
    pub bit_depth: Option<u8>,
    pub track_count: u16,
    pub total_duration_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceResource {
    pub ingest_origin: IngestOriginValue,
    pub original_source_path: String,
    pub imported_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportPreviewResource {
    pub id: String,
    pub release_instance_id: String,
    pub export_profile: String,
    pub album_title: String,
    pub album_artist: String,
    pub artist_credits: Vec<String>,
    pub edition_visibility: QualifierVisibilityValue,
    pub technical_visibility: QualifierVisibilityValue,
    pub path_components: Vec<String>,
    pub primary_artwork_filename: Option<String>,
    pub compatibility: CompatibilityReportResource,
    pub rendered_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityReportResource {
    pub verified: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseInstanceStateValue {
    Discovered,
    Staged,
    Analyzed,
    Matched,
    NeedsReview,
    RenderingExport,
    Tagging,
    Organizing,
    Imported,
    Verified,
    Quarantined,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormatFamilyValue {
    Flac,
    Mp3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BitrateModeValue {
    Constant,
    Variable,
    Lossless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestOriginValue {
    WatchDirectory,
    ApiPush,
    ManualAdd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualifierVisibilityValue {
    Hidden,
    PathOnly,
    TagsAndPath,
}

pub struct InspectionApi<R> {
    repository: R,
}

impl<R> InspectionApi<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> InspectionApi<R>
where
    R: ReleaseRepository + ReleaseInstanceRepository + ExportRepository,
{
    pub fn list_releases(
        &self,
        request_id: impl Into<String>,
        request: ListReleasesRequest,
    ) -> ApiResult<Vec<ReleaseSummaryResource>> {
        let request_id = request_id.into();
        let release_group_id =
            parse_optional_release_group_id(request.release_group_id.as_deref(), &request_id)?;
        let page = self
            .repository
            .list_releases(&ReleaseListQuery {
                release_group_id,
                text: request.text,
                page: PageRequest::new(request.limit, request.offset),
            })
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success_with_pagination(
            page.items
                .iter()
                .map(ReleaseSummaryResource::from)
                .collect(),
            request_id,
            ApiPaginationMeta::from_page(&page),
        ))
    }

    pub fn get_release(
        &self,
        request_id: impl Into<String>,
        release_id: &str,
    ) -> ApiResult<ReleaseDetailResource> {
        let request_id = request_id.into();
        let release_id = parse_release_id(release_id, &request_id)?;
        let release = self
            .repository
            .get_release(&release_id)
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?
            .ok_or_else(|| {
                not_found_envelope("release", release_id.as_uuid().to_string(), &request_id)
            })?;
        let tracks = self
            .repository
            .list_tracks_for_release(&release_id)
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            ReleaseDetailResource {
                release: ReleaseSummaryResource::from(&release),
                tracks: tracks.iter().map(TrackResource::from).collect(),
            },
            request_id,
        ))
    }

    pub fn list_release_instances(
        &self,
        request_id: impl Into<String>,
        request: ListReleaseInstancesRequest,
    ) -> ApiResult<Vec<ReleaseInstanceResource>> {
        let request_id = request_id.into();
        let release_id = parse_optional_release_id(request.release_id.as_deref(), &request_id)?;
        let page = self
            .repository
            .list_release_instances(&ReleaseInstanceListQuery {
                release_id,
                state: request.state.map(Into::into),
                format_family: request.format_family.map(Into::into),
                page: PageRequest::new(request.limit, request.offset),
            })
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success_with_pagination(
            page.items
                .iter()
                .map(ReleaseInstanceResource::from)
                .collect(),
            request_id,
            ApiPaginationMeta::from_page(&page),
        ))
    }

    pub fn get_release_instance(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
    ) -> ApiResult<ReleaseInstanceResource> {
        let request_id = request_id.into();
        let release_instance_id = parse_release_instance_id(release_instance_id, &request_id)?;
        let release_instance = self
            .repository
            .get_release_instance(&release_instance_id)
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?
            .ok_or_else(|| {
                not_found_envelope(
                    "release instance",
                    release_instance_id.as_uuid().to_string(),
                    &request_id,
                )
            })?;
        Ok(ApiEnvelope::success(
            ReleaseInstanceResource::from(&release_instance),
            request_id,
        ))
    }

    pub fn get_export_preview(
        &self,
        request_id: impl Into<String>,
        release_instance_id: &str,
    ) -> ApiResult<ExportPreviewResource> {
        let request_id = request_id.into();
        let release_instance_id = parse_release_instance_id(release_instance_id, &request_id)?;
        let preview = self
            .repository
            .get_latest_exported_metadata(&release_instance_id)
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?
            .ok_or_else(|| {
                not_found_envelope(
                    "export preview",
                    release_instance_id.as_uuid().to_string(),
                    &request_id,
                )
            })?;
        Ok(ApiEnvelope::success(
            ExportPreviewResource::from(&preview),
            request_id,
        ))
    }

    pub fn search_release_groups(
        &self,
        request_id: impl Into<String>,
        request: SearchReleaseGroupsRequest,
    ) -> ApiResult<Vec<ReleaseGroupResource>> {
        let request_id = request_id.into();
        let page = self
            .repository
            .search_release_groups(&ReleaseGroupSearchQuery {
                text: request.text,
                primary_artist_name: request.primary_artist_name,
                page: PageRequest::new(request.limit, request.offset),
            })
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success_with_pagination(
            page.items.iter().map(ReleaseGroupResource::from).collect(),
            request_id,
            ApiPaginationMeta::from_page(&page),
        ))
    }
}

impl From<&Release> for ReleaseSummaryResource {
    fn from(value: &Release) -> Self {
        Self {
            id: value.id.as_uuid().to_string(),
            release_group_id: value.release_group_id.as_uuid().to_string(),
            title: value.title.clone(),
            musicbrainz_release_id: value
                .musicbrainz_release_id
                .as_ref()
                .map(|id| id.as_uuid().to_string()),
            discogs_release_id: value.discogs_release_id.as_ref().map(|id| id.value()),
            edition: ReleaseEditionResource::from(&value.edition),
        }
    }
}

impl From<&ReleaseEdition> for ReleaseEditionResource {
    fn from(value: &ReleaseEdition) -> Self {
        Self {
            edition_title: value.edition_title.clone(),
            disambiguation: value.disambiguation.clone(),
            country: value.country.clone(),
            label: value.label.clone(),
            catalog_number: value.catalog_number.clone(),
            release_date: value.release_date.as_ref().map(PartialDateResource::from),
        }
    }
}

impl From<&PartialDate> for PartialDateResource {
    fn from(value: &PartialDate) -> Self {
        Self {
            year: value.year,
            month: value.month,
            day: value.day,
        }
    }
}

impl From<&Track> for TrackResource {
    fn from(value: &Track) -> Self {
        Self {
            id: value.id.as_uuid().to_string(),
            disc_number: value.position.disc_number,
            track_number: value.position.track_number,
            title: value.title.clone(),
            musicbrainz_track_id: value
                .musicbrainz_track_id
                .as_ref()
                .map(|id| id.as_uuid().to_string()),
            duration_ms: value.duration_ms,
        }
    }
}

impl From<&ReleaseGroup> for ReleaseGroupResource {
    fn from(value: &ReleaseGroup) -> Self {
        Self {
            id: value.id.as_uuid().to_string(),
            title: value.title.clone(),
            kind: release_group_kind_name(&value.kind).to_string(),
            musicbrainz_release_group_id: value
                .musicbrainz_release_group_id
                .as_ref()
                .map(|id| id.as_uuid().to_string()),
        }
    }
}

impl From<&ReleaseInstance> for ReleaseInstanceResource {
    fn from(value: &ReleaseInstance) -> Self {
        Self {
            id: value.id.as_uuid().to_string(),
            import_batch_id: value.import_batch_id.as_uuid().to_string(),
            source_id: value.source_id.as_uuid().to_string(),
            release_id: value.release_id.as_ref().map(|id| id.as_uuid().to_string()),
            state: value.state.clone().into(),
            technical_variant: TechnicalVariantResource {
                format_family: value.technical_variant.format_family.clone().into(),
                bitrate_mode: value.technical_variant.bitrate_mode.clone().into(),
                bitrate_kbps: value.technical_variant.bitrate_kbps,
                sample_rate_hz: value.technical_variant.sample_rate_hz,
                bit_depth: value.technical_variant.bit_depth,
                track_count: value.technical_variant.track_count,
                total_duration_seconds: value.technical_variant.total_duration_seconds,
            },
            provenance: ProvenanceResource {
                ingest_origin: value.provenance.ingest_origin.clone().into(),
                original_source_path: value.provenance.original_source_path.clone(),
                imported_at_unix_seconds: value.provenance.imported_at_unix_seconds,
            },
        }
    }
}

impl From<&ExportedMetadataSnapshot> for ExportPreviewResource {
    fn from(value: &ExportedMetadataSnapshot) -> Self {
        Self {
            id: value.id.as_uuid().to_string(),
            release_instance_id: value.release_instance_id.as_uuid().to_string(),
            export_profile: value.export_profile.clone(),
            album_title: value.album_title.clone(),
            album_artist: value.album_artist.clone(),
            artist_credits: value.artist_credits.clone(),
            edition_visibility: value.edition_visibility.clone().into(),
            technical_visibility: value.technical_visibility.clone().into(),
            path_components: value.path_components.clone(),
            primary_artwork_filename: value.primary_artwork_filename.clone(),
            compatibility: CompatibilityReportResource::from(&value.compatibility),
            rendered_at_unix_seconds: value.rendered_at_unix_seconds,
        }
    }
}

impl From<&CompatibilityReport> for CompatibilityReportResource {
    fn from(value: &CompatibilityReport) -> Self {
        Self {
            verified: value.verified,
            warnings: value.warnings.clone(),
        }
    }
}

impl From<ReleaseInstanceState> for ReleaseInstanceStateValue {
    fn from(value: ReleaseInstanceState) -> Self {
        match value {
            ReleaseInstanceState::Discovered => Self::Discovered,
            ReleaseInstanceState::Staged => Self::Staged,
            ReleaseInstanceState::Analyzed => Self::Analyzed,
            ReleaseInstanceState::Matched => Self::Matched,
            ReleaseInstanceState::NeedsReview => Self::NeedsReview,
            ReleaseInstanceState::RenderingExport => Self::RenderingExport,
            ReleaseInstanceState::Tagging => Self::Tagging,
            ReleaseInstanceState::Organizing => Self::Organizing,
            ReleaseInstanceState::Imported => Self::Imported,
            ReleaseInstanceState::Verified => Self::Verified,
            ReleaseInstanceState::Quarantined => Self::Quarantined,
            ReleaseInstanceState::Failed => Self::Failed,
        }
    }
}

impl From<ReleaseInstanceStateValue> for ReleaseInstanceState {
    fn from(value: ReleaseInstanceStateValue) -> Self {
        match value {
            ReleaseInstanceStateValue::Discovered => Self::Discovered,
            ReleaseInstanceStateValue::Staged => Self::Staged,
            ReleaseInstanceStateValue::Analyzed => Self::Analyzed,
            ReleaseInstanceStateValue::Matched => Self::Matched,
            ReleaseInstanceStateValue::NeedsReview => Self::NeedsReview,
            ReleaseInstanceStateValue::RenderingExport => Self::RenderingExport,
            ReleaseInstanceStateValue::Tagging => Self::Tagging,
            ReleaseInstanceStateValue::Organizing => Self::Organizing,
            ReleaseInstanceStateValue::Imported => Self::Imported,
            ReleaseInstanceStateValue::Verified => Self::Verified,
            ReleaseInstanceStateValue::Quarantined => Self::Quarantined,
            ReleaseInstanceStateValue::Failed => Self::Failed,
        }
    }
}

impl From<FormatFamily> for FormatFamilyValue {
    fn from(value: FormatFamily) -> Self {
        match value {
            FormatFamily::Flac => Self::Flac,
            FormatFamily::Mp3 => Self::Mp3,
        }
    }
}

impl From<FormatFamilyValue> for FormatFamily {
    fn from(value: FormatFamilyValue) -> Self {
        match value {
            FormatFamilyValue::Flac => Self::Flac,
            FormatFamilyValue::Mp3 => Self::Mp3,
        }
    }
}

impl From<BitrateMode> for BitrateModeValue {
    fn from(value: BitrateMode) -> Self {
        match value {
            BitrateMode::Constant => Self::Constant,
            BitrateMode::Variable => Self::Variable,
            BitrateMode::Lossless => Self::Lossless,
        }
    }
}

impl From<IngestOrigin> for IngestOriginValue {
    fn from(value: IngestOrigin) -> Self {
        match value {
            IngestOrigin::WatchDirectory => Self::WatchDirectory,
            IngestOrigin::ApiPush => Self::ApiPush,
            IngestOrigin::ManualAdd => Self::ManualAdd,
        }
    }
}

impl From<QualifierVisibility> for QualifierVisibilityValue {
    fn from(value: QualifierVisibility) -> Self {
        match value {
            QualifierVisibility::Hidden => Self::Hidden,
            QualifierVisibility::PathOnly => Self::PathOnly,
            QualifierVisibility::TagsAndPath => Self::TagsAndPath,
        }
    }
}

fn release_group_kind_name(value: &ReleaseGroupKind) -> &str {
    match value {
        ReleaseGroupKind::Album => "album",
        ReleaseGroupKind::Ep => "ep",
        ReleaseGroupKind::Single => "single",
        ReleaseGroupKind::Live => "live",
        ReleaseGroupKind::Compilation => "compilation",
        ReleaseGroupKind::Soundtrack => "soundtrack",
        ReleaseGroupKind::Other(_) => "other",
    }
}

fn parse_release_id(release_id: &str, request_id: &str) -> Result<ReleaseId, Box<ApiEnvelope<()>>> {
    ReleaseId::parse_str(release_id)
        .map_err(|_| invalid_id_envelope("release", release_id, request_id))
}

fn parse_optional_release_id(
    release_id: Option<&str>,
    request_id: &str,
) -> Result<Option<ReleaseId>, Box<ApiEnvelope<()>>> {
    release_id
        .map(|value| parse_release_id(value, request_id))
        .transpose()
}

fn parse_optional_release_group_id(
    release_group_id: Option<&str>,
    request_id: &str,
) -> Result<Option<crate::support::ids::ReleaseGroupId>, Box<ApiEnvelope<()>>> {
    release_group_id
        .map(|value| {
            crate::support::ids::ReleaseGroupId::parse_str(value)
                .map_err(|_| invalid_id_envelope("release group", value, request_id))
        })
        .transpose()
}

fn parse_release_instance_id(
    release_instance_id: &str,
    request_id: &str,
) -> Result<ReleaseInstanceId, Box<ApiEnvelope<()>>> {
    ReleaseInstanceId::parse_str(release_instance_id)
        .map_err(|_| invalid_id_envelope("release instance", release_instance_id, request_id))
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

fn not_found_envelope(kind: &str, id: String, request_id: &str) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            ApiErrorCode::NotFound,
            format!("{kind} {id} was not found"),
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::support::ids::{ArtistId, ReleaseGroupId, TrackId};
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn list_releases_returns_paginated_resources() {
        let repository = InMemoryInspectionRepository::seeded();
        let api = InspectionApi::new(repository);

        let envelope = api
            .list_releases(
                "req_releases",
                ListReleasesRequest {
                    limit: 1,
                    offset: 0,
                    ..ListReleasesRequest::default()
                },
            )
            .expect("release list should succeed");

        assert_eq!(envelope.data.expect("data should exist").len(), 1);
        assert_eq!(
            envelope
                .meta
                .pagination
                .expect("pagination should exist")
                .next_offset,
            Some(1)
        );
    }

    #[test]
    fn get_release_returns_tracks() {
        let repository = InMemoryInspectionRepository::seeded();
        let api = InspectionApi::new(repository.clone());

        let envelope = api
            .get_release("req_release", &repository.release.id.as_uuid().to_string())
            .expect("release detail should succeed");

        let detail = envelope.data.expect("data should exist");
        assert_eq!(detail.release.title, "Kid A");
        assert_eq!(detail.tracks.len(), 1);
        assert_eq!(detail.tracks[0].title, "Everything in Its Right Place");
    }

    #[test]
    fn list_release_instances_filters_by_state() {
        let repository = InMemoryInspectionRepository::seeded();
        let api = InspectionApi::new(repository);

        let envelope = api
            .list_release_instances(
                "req_instances",
                ListReleaseInstancesRequest {
                    state: Some(ReleaseInstanceStateValue::Matched),
                    ..ListReleaseInstancesRequest::default()
                },
            )
            .expect("instance list should succeed");

        let items = envelope.data.expect("data should exist");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].state, ReleaseInstanceStateValue::Matched);
    }

    #[test]
    fn get_export_preview_returns_latest_snapshot() {
        let repository = InMemoryInspectionRepository::seeded();
        let api = InspectionApi::new(repository.clone());

        let envelope = api
            .get_export_preview(
                "req_preview",
                &repository.release_instance.id.as_uuid().to_string(),
            )
            .expect("preview should succeed");

        let preview = envelope.data.expect("data should exist");
        assert_eq!(preview.album_title, "Kid A [2000]");
        assert_eq!(preview.path_components.len(), 2);
    }

    #[test]
    fn search_release_groups_uses_text_filters() {
        let repository = InMemoryInspectionRepository::seeded();
        let api = InspectionApi::new(repository);

        let envelope = api
            .search_release_groups(
                "req_groups",
                SearchReleaseGroupsRequest {
                    text: Some("Kid".to_string()),
                    primary_artist_name: None,
                    ..SearchReleaseGroupsRequest::default()
                },
            )
            .expect("search should succeed");

        let groups = envelope.data.expect("data should exist");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].title, "Kid A");
    }

    #[derive(Clone)]
    struct InMemoryInspectionRepository {
        release: Release,
        release_instance: ReleaseInstance,
        tracks: Arc<Mutex<Vec<Track>>>,
        releases: Arc<Mutex<Vec<Release>>>,
        release_groups: Arc<Mutex<Vec<ReleaseGroup>>>,
        release_instances: Arc<Mutex<Vec<ReleaseInstance>>>,
        exports: Arc<Mutex<Vec<ExportedMetadataSnapshot>>>,
    }

    impl InMemoryInspectionRepository {
        fn seeded() -> Self {
            let artist_id = ArtistId::new();
            let release_group = ReleaseGroup {
                id: ReleaseGroupId::new(),
                primary_artist_id: artist_id.clone(),
                title: "Kid A".to_string(),
                kind: ReleaseGroupKind::Album,
                musicbrainz_release_group_id: None,
            };
            let release = Release {
                id: ReleaseId::new(),
                release_group_id: release_group.id.clone(),
                primary_artist_id: artist_id,
                title: "Kid A".to_string(),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: ReleaseEdition {
                    release_date: Some(PartialDate {
                        year: 2000,
                        month: Some(10),
                        day: Some(2),
                    }),
                    ..ReleaseEdition::default()
                },
            };
            let release_instance = ReleaseInstance {
                id: ReleaseInstanceId::new(),
                import_batch_id: crate::support::ids::ImportBatchId::new(),
                source_id: crate::support::ids::SourceId::new(),
                release_id: Some(release.id.clone()),
                state: ReleaseInstanceState::Matched,
                technical_variant: crate::domain::release_instance::TechnicalVariant {
                    format_family: FormatFamily::Mp3,
                    bitrate_mode: BitrateMode::Variable,
                    bitrate_kbps: Some(320),
                    sample_rate_hz: Some(44_100),
                    bit_depth: None,
                    track_count: 1,
                    total_duration_seconds: 250,
                },
                provenance: crate::domain::release_instance::ProvenanceSnapshot {
                    ingest_origin: IngestOrigin::ManualAdd,
                    original_source_path: "/imports/kid-a".to_string(),
                    imported_at_unix_seconds: 100,
                    gazelle_reference: None,
                },
            };
            let tracks = vec![Track {
                id: TrackId::new(),
                release_id: release.id.clone(),
                position: crate::domain::track::TrackPosition {
                    disc_number: 1,
                    track_number: 1,
                },
                title: "Everything in Its Right Place".to_string(),
                musicbrainz_track_id: None,
                duration_ms: Some(250_000),
            }];
            let exports = vec![ExportedMetadataSnapshot {
                id: crate::support::ids::ExportedMetadataSnapshotId::new(),
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
                rendered_at_unix_seconds: 200,
            }];

            Self {
                release: release.clone(),
                release_instance: release_instance.clone(),
                tracks: Arc::new(Mutex::new(tracks)),
                releases: Arc::new(Mutex::new(vec![
                    release.clone(),
                    Release {
                        id: ReleaseId::new(),
                        release_group_id: release_group.id.clone(),
                        primary_artist_id: release.primary_artist_id.clone(),
                        title: "Kid A Mnesia".to_string(),
                        musicbrainz_release_id: None,
                        discogs_release_id: None,
                        edition: ReleaseEdition::default(),
                    },
                ])),
                release_groups: Arc::new(Mutex::new(vec![release_group])),
                release_instances: Arc::new(Mutex::new(vec![release_instance])),
                exports: Arc::new(Mutex::new(exports)),
            }
        }
    }

    impl ReleaseRepository for InMemoryInspectionRepository {
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
            Ok(self
                .release_groups
                .lock()
                .expect("release groups should lock")
                .iter()
                .find(|group| group.id == *id)
                .cloned())
        }

        fn find_release_group_by_musicbrainz_id(
            &self,
            _musicbrainz_release_group_id: &str,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(None)
        }

        fn get_release(&self, id: &ReleaseId) -> Result<Option<Release>, RepositoryError> {
            Ok(self
                .releases
                .lock()
                .expect("releases should lock")
                .iter()
                .find(|release| release.id == *id)
                .cloned())
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
            let items = self
                .release_groups
                .lock()
                .expect("release groups should lock")
                .iter()
                .filter(|group| {
                    query
                        .text
                        .as_ref()
                        .is_none_or(|text| group.title.contains(text))
                })
                .cloned()
                .collect::<Vec<_>>();
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }

        fn list_releases(
            &self,
            query: &ReleaseListQuery,
        ) -> Result<Page<Release>, RepositoryError> {
            let mut items = self
                .releases
                .lock()
                .expect("releases should lock")
                .iter()
                .filter(|release| {
                    query
                        .release_group_id
                        .as_ref()
                        .is_none_or(|group_id| &release.release_group_id == group_id)
                })
                .filter(|release| {
                    query
                        .text
                        .as_ref()
                        .is_none_or(|text| release.title.contains(text))
                })
                .cloned()
                .collect::<Vec<_>>();
            let total = items.len() as u64;
            items = items
                .into_iter()
                .skip(query.page.offset as usize)
                .take(query.page.limit as usize)
                .collect();
            Ok(Page {
                total,
                items,
                request: query.page,
            })
        }

        fn list_tracks_for_release(
            &self,
            release_id: &ReleaseId,
        ) -> Result<Vec<Track>, RepositoryError> {
            Ok(self
                .tracks
                .lock()
                .expect("tracks should lock")
                .iter()
                .filter(|track| &track.release_id == release_id)
                .cloned()
                .collect())
        }
    }

    impl ReleaseInstanceRepository for InMemoryInspectionRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .iter()
                .find(|instance| instance.id == *id)
                .cloned())
        }

        fn list_release_instances(
            &self,
            query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            let mut items =
                self.release_instances
                    .lock()
                    .expect("release instances should lock")
                    .iter()
                    .filter(|instance| {
                        query.release_id.as_ref().is_none_or(|release_id| {
                            instance.release_id.as_ref() == Some(release_id)
                        })
                    })
                    .filter(|instance| {
                        query
                            .state
                            .as_ref()
                            .is_none_or(|state| &instance.state == state)
                    })
                    .filter(|instance| {
                        query.format_family.as_ref().is_none_or(|family| {
                            &instance.technical_variant.format_family == family
                        })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
            let total = items.len() as u64;
            items = items
                .into_iter()
                .skip(query.page.offset as usize)
                .take(query.page.limit as usize)
                .collect();
            Ok(Page {
                total,
                items,
                request: query.page,
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
            _release_instance_id: &ReleaseInstanceId,
            _page: &PageRequest,
        ) -> Result<Page<crate::domain::candidate_match::CandidateMatch>, RepositoryError> {
            Ok(Page {
                total: 0,
                items: Vec::new(),
                request: PageRequest::default(),
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

    impl ExportRepository for InMemoryInspectionRepository {
        fn get_latest_exported_metadata(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(self
                .exports
                .lock()
                .expect("exports should lock")
                .iter()
                .find(|export| &export.release_instance_id == release_instance_id)
                .cloned())
        }

        fn list_exported_metadata(
            &self,
            _query: &crate::application::repository::ExportedMetadataListQuery,
        ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(Page {
                total: 0,
                items: Vec::new(),
                request: PageRequest::default(),
            })
        }

        fn get_exported_metadata(
            &self,
            _id: &crate::support::ids::ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(None)
        }
    }
}
