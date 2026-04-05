use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::PathBuf;

use crate::application::repository::{
    ImportBatchRepository, IngestEvidenceRepository, IssueCommandRepository, IssueListQuery,
    IssueRepository, ManualOverrideCommandRepository, ManualOverrideListQuery,
    ManualOverrideRepository, MetadataSnapshotCommandRepository, ReleaseCommandRepository,
    ReleaseInstanceCommandRepository, ReleaseInstanceRepository, ReleaseRepository,
    RepositoryError, RepositoryErrorKind, SourceRepository, StagingManifestRepository,
};
use crate::domain::artist::Artist;
use crate::domain::candidate_match::{
    CandidateMatch, CandidateProvider, CandidateScore, CandidateSubject, EvidenceKind,
    EvidenceNote, ProviderProvenance,
};
use crate::domain::import_batch::ImportBatch;
use crate::domain::ingest_evidence::{
    IngestEvidenceRecord, IngestEvidenceSubject, ObservedValueKind,
};
use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
use crate::domain::manual_override::{ManualOverride, OverrideField, OverrideSubject};
use crate::domain::metadata_snapshot::{
    MetadataSnapshot, MetadataSnapshotSource, MetadataSubject, SnapshotFormat,
};
use crate::domain::release::{PartialDate, Release, ReleaseEdition};
use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
use crate::domain::release_instance::{
    BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
    ReleaseInstanceState, TechnicalVariant,
};
use crate::domain::source::{Source, SourceKind};
use crate::domain::staging_manifest::{StagedReleaseGroup, StagingManifest};
use crate::support::ids::{CandidateMatchId, ImportBatchId, ReleaseInstanceId};
use crate::support::pagination::PageRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingServiceError {
    pub kind: MatchingServiceErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchingServiceErrorKind {
    NotFound,
    Conflict,
    Storage,
    Provider,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchMatchProbeReport {
    pub batch_id: ImportBatchId,
    pub groups: Vec<GroupMatchProbe>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMatchProbe {
    pub release_instance_id: Option<ReleaseInstanceId>,
    pub group_key: String,
    pub evidence: MatchEvidenceSummary,
    pub release_query: String,
    pub release_group_query: String,
    pub release_candidates: Vec<MusicBrainzReleaseCandidate>,
    pub release_group_candidates: Vec<MusicBrainzReleaseGroupCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MatchEvidenceSummary {
    pub primary_artist: Option<String>,
    pub release_title: Option<String>,
    pub release_year: Option<String>,
    pub label_hints: Vec<String>,
    pub catalog_hints: Vec<String>,
    pub track_count: usize,
    pub disc_count: Option<usize>,
    pub directory_hint: Option<String>,
    pub filename_hints: Vec<String>,
    pub source_descriptors: Vec<String>,
    pub tracker_identifiers: Vec<String>,
    pub evidence_conflicts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PersistedBatchMatchReport {
    pub batch_id: ImportBatchId,
    pub groups: Vec<PersistedGroupMatch>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PersistedGroupMatch {
    pub release_instance: ReleaseInstance,
    pub group_key: String,
    pub evidence: MatchEvidenceSummary,
    pub release_query: String,
    pub release_group_query: String,
    pub persisted_candidates: Vec<CandidateMatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzReleaseCandidate {
    pub id: String,
    pub title: String,
    pub score: u16,
    pub artist_names: Vec<String>,
    pub release_group_id: Option<String>,
    pub release_group_title: Option<String>,
    pub country: Option<String>,
    pub date: Option<String>,
    pub track_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzReleaseGroupCandidate {
    pub id: String,
    pub title: String,
    pub score: u16,
    pub artist_names: Vec<String>,
    pub primary_type: Option<String>,
    pub first_release_date: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiscogsReleaseQuery {
    pub text: Option<String>,
    pub artist: Option<String>,
    pub title: Option<String>,
    pub year: Option<String>,
    pub label: Option<String>,
    pub catalog_number: Option<String>,
    pub format_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscogsReleaseCandidate {
    pub id: String,
    pub title: String,
    pub artist: Option<String>,
    pub year: Option<String>,
    pub country: Option<String>,
    pub label: Option<String>,
    pub catalog_number: Option<String>,
    pub format_descriptors: Vec<String>,
    pub raw_payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscogsFieldDifference {
    pub field: String,
    pub local_value: Option<String>,
    pub provider_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscogsEnrichmentReport {
    pub release_instance: ReleaseInstance,
    pub query: DiscogsReleaseQuery,
    pub persisted_candidates: Vec<CandidateMatch>,
    pub metadata_snapshot: MetadataSnapshot,
    pub field_differences: Vec<DiscogsFieldDifference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzReleaseDetail {
    pub id: String,
    pub title: String,
    pub country: Option<String>,
    pub date: Option<String>,
    pub artist_credit: Vec<MusicBrainzArtistCredit>,
    pub release_group: Option<MusicBrainzReleaseGroupRef>,
    pub label_info: Vec<MusicBrainzLabelInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzArtistCredit {
    pub artist_id: String,
    pub artist_name: String,
    pub artist_sort_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzReleaseGroupRef {
    pub id: String,
    pub title: String,
    pub primary_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzLabelInfo {
    pub catalog_number: Option<String>,
    pub label_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedBatchMatchReport {
    pub batch_id: ImportBatchId,
    pub groups: Vec<MaterializedReleaseMatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedReleaseMatch {
    pub release_instance: ReleaseInstance,
    pub release: Release,
    pub release_group: ReleaseGroup,
    pub artist: Artist,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectedCandidateMatchReport {
    pub release_instance: ReleaseInstance,
    pub release: Release,
    pub release_group: ReleaseGroup,
    pub artist: Artist,
    pub candidate: CandidateMatch,
}

pub trait MusicBrainzMetadataProvider {
    fn search_releases(
        &self,
        query: &str,
        limit: u8,
    ) -> impl Future<Output = Result<Vec<MusicBrainzReleaseCandidate>, String>> + Send;

    fn search_release_groups(
        &self,
        query: &str,
        limit: u8,
    ) -> impl Future<Output = Result<Vec<MusicBrainzReleaseGroupCandidate>, String>> + Send;

    fn lookup_release(
        &self,
        release_id: &str,
    ) -> impl Future<Output = Result<MusicBrainzReleaseDetail, String>> + Send;
}

pub trait DiscogsMetadataProvider {
    fn search_releases(
        &self,
        query: &DiscogsReleaseQuery,
        limit: u8,
    ) -> impl Future<Output = Result<Vec<DiscogsReleaseCandidate>, String>> + Send;
}

pub struct ReleaseMatchingService<R, P> {
    repository: R,
    provider: P,
}

impl<R, P> ReleaseMatchingService<R, P> {
    pub fn new(repository: R, provider: P) -> Self {
        Self {
            repository,
            provider,
        }
    }
}

impl<R, P> ReleaseMatchingService<R, P>
where
    R: StagingManifestRepository + IngestEvidenceRepository,
    P: MusicBrainzMetadataProvider,
{
    pub async fn probe_batch_matches(
        &self,
        batch_id: &ImportBatchId,
    ) -> Result<BatchMatchProbeReport, MatchingServiceError> {
        let manifest = latest_manifest(
            self.repository
                .list_staging_manifests_for_batch(batch_id)
                .map_err(map_repository_error)?,
            batch_id,
        )?;
        let evidence = self
            .repository
            .list_ingest_evidence_for_batch(batch_id)
            .map_err(map_repository_error)?;
        let evidence_by_path = evidence_by_discovered_path(&evidence);
        let evidence_by_group = evidence_by_group_key(&evidence);

        let mut groups = Vec::new();
        for group in &manifest.grouping.groups {
            let summary = summarize_group_evidence(group, &evidence_by_path, &evidence_by_group);
            let release_query = build_release_query(&summary);
            let release_group_query = build_release_group_query(&summary);
            let release_candidates =
                MusicBrainzMetadataProvider::search_releases(&self.provider, &release_query, 10)
                    .await
                    .map_err(map_provider_error)?;
            let release_group_candidates = self
                .provider
                .search_release_groups(&release_group_query, 10)
                .await
                .map_err(map_provider_error)?;
            groups.push(GroupMatchProbe {
                release_instance_id: None,
                group_key: group.key.clone(),
                evidence: summary,
                release_query,
                release_group_query,
                release_candidates,
                release_group_candidates,
            });
        }

        Ok(BatchMatchProbeReport {
            batch_id: batch_id.clone(),
            groups,
        })
    }
}

impl<R, P> ReleaseMatchingService<R, P>
where
    R: ImportBatchRepository
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
    P: MusicBrainzMetadataProvider + DiscogsMetadataProvider,
{
    pub async fn score_and_persist_batch_matches(
        &self,
        batch_id: &ImportBatchId,
        persisted_at_unix_seconds: i64,
    ) -> Result<PersistedBatchMatchReport, MatchingServiceError> {
        let batch = self
            .repository
            .get_import_batch(batch_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| MatchingServiceError {
                kind: MatchingServiceErrorKind::NotFound,
                message: format!("no import batch found for {}", batch_id.as_uuid()),
            })?;
        let source = self
            .repository
            .get_source(&batch.source_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| MatchingServiceError {
                kind: MatchingServiceErrorKind::NotFound,
                message: format!("no source found for batch {}", batch_id.as_uuid()),
            })?;
        let manifest = latest_manifest(
            self.repository
                .list_staging_manifests_for_batch(batch_id)
                .map_err(map_repository_error)?,
            batch_id,
        )?;
        let evidence = self
            .repository
            .list_ingest_evidence_for_batch(batch_id)
            .map_err(map_repository_error)?;
        let evidence_by_path = evidence_by_discovered_path(&evidence);
        let evidence_by_group = evidence_by_group_key(&evidence);
        let existing_instances = self
            .repository
            .list_release_instances_for_batch(batch_id)
            .map_err(map_repository_error)?;

        let mut groups = Vec::new();
        for group in &manifest.grouping.groups {
            let summary = summarize_group_evidence(group, &evidence_by_path, &evidence_by_group);
            let release_query = build_release_query(&summary);
            let release_group_query = build_release_group_query(&summary);
            let release_candidates =
                MusicBrainzMetadataProvider::search_releases(&self.provider, &release_query, 10)
                    .await
                    .map_err(map_provider_error)?;
            let release_group_candidates = self
                .provider
                .search_release_groups(&release_group_query, 10)
                .await
                .map_err(map_provider_error)?;
            let release_instance = ensure_provisional_release_instance(
                &self.repository,
                &batch,
                &source,
                group,
                &summary,
                &evidence_by_path,
                &existing_instances,
            )?;
            let persisted_candidates = score_group_candidates(
                &release_instance.id,
                &summary,
                &release_query,
                &release_group_query,
                release_candidates,
                release_group_candidates,
                persisted_at_unix_seconds,
            );
            self.repository
                .replace_candidate_matches(&release_instance.id, &persisted_candidates)
                .map_err(map_repository_error)?;

            let mut updated_instance = release_instance.clone();
            apply_match_outcome(
                &self.repository,
                &mut updated_instance,
                &persisted_candidates,
                persisted_at_unix_seconds,
            )?;

            groups.push(PersistedGroupMatch {
                release_instance: updated_instance,
                group_key: group.key.clone(),
                evidence: summary,
                release_query,
                release_group_query,
                persisted_candidates,
            });
        }

        Ok(PersistedBatchMatchReport {
            batch_id: batch_id.clone(),
            groups,
        })
    }

    pub async fn materialize_batch_matches(
        &self,
        batch_id: &ImportBatchId,
    ) -> Result<MaterializedBatchMatchReport, MatchingServiceError> {
        let release_instances = self
            .repository
            .list_release_instances_for_batch(batch_id)
            .map_err(map_repository_error)?;
        let mut groups = Vec::new();

        for release_instance in release_instances {
            if release_instance.state != ReleaseInstanceState::Analyzed {
                continue;
            }

            let candidates = self
                .repository
                .list_candidate_matches(&release_instance.id, &PageRequest::new(50, 0))
                .map_err(map_repository_error)?;
            if review_issue_type(&candidates.items).is_some() {
                continue;
            }

            let Some(best_release_id) = best_musicbrainz_release_id(&candidates.items) else {
                continue;
            };
            let detail = self
                .provider
                .lookup_release(&best_release_id)
                .await
                .map_err(map_provider_error)?;
            let artist = ensure_artist(&self.repository, &detail)?;
            let release_group = ensure_release_group(&self.repository, &artist, &detail)?;
            let release = ensure_release(&self.repository, &artist, &release_group, &detail)?;

            let mut updated_instance = release_instance.clone();
            updated_instance.release_id = Some(release.id.clone());
            updated_instance.state = ReleaseInstanceState::Matched;
            self.repository
                .update_release_instance(&updated_instance)
                .map_err(map_repository_error)?;

            groups.push(MaterializedReleaseMatch {
                release_instance: updated_instance,
                release,
                release_group,
                artist,
            });
        }

        Ok(MaterializedBatchMatchReport {
            batch_id: batch_id.clone(),
            groups,
        })
    }

    pub fn apply_manual_release_override(
        &self,
        release_instance_id: &ReleaseInstanceId,
        release_id: &crate::support::ids::ReleaseId,
        created_by: &str,
        note: Option<String>,
        created_at_unix_seconds: i64,
    ) -> Result<ReleaseInstance, MatchingServiceError> {
        let mut release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| MatchingServiceError {
                kind: MatchingServiceErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        self.repository
            .get_release(release_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| MatchingServiceError {
                kind: MatchingServiceErrorKind::NotFound,
                message: format!("no release found for {}", release_id.as_uuid()),
            })?;

        let override_record = ManualOverride {
            id: crate::support::ids::ManualOverrideId::new(),
            subject: OverrideSubject::ReleaseInstance(release_instance.id.clone()),
            field: OverrideField::ReleaseMatch,
            value: release_id.as_uuid().to_string(),
            note,
            created_by: created_by.to_string(),
            created_at_unix_seconds,
        };
        self.repository
            .create_manual_override(&override_record)
            .map_err(map_repository_error)?;

        release_instance.release_id = Some(release_id.clone());
        release_instance.state = ReleaseInstanceState::Matched;
        self.repository
            .update_release_instance(&release_instance)
            .map_err(map_repository_error)?;
        resolve_review_issues_for_subject(
            &self.repository,
            &IssueSubject::ReleaseInstance(release_instance.id.clone()),
            created_at_unix_seconds,
        )?;
        Ok(release_instance)
    }

    pub async fn select_candidate_match(
        &self,
        release_instance_id: &ReleaseInstanceId,
        candidate_id: &CandidateMatchId,
        created_by: &str,
        note: Option<String>,
        created_at_unix_seconds: i64,
    ) -> Result<SelectedCandidateMatchReport, MatchingServiceError> {
        let candidate = self
            .repository
            .get_candidate_match(candidate_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| MatchingServiceError {
                kind: MatchingServiceErrorKind::NotFound,
                message: format!("no candidate match found for {}", candidate_id.as_uuid()),
            })?;
        if candidate.release_instance_id != *release_instance_id {
            return Err(MatchingServiceError {
                kind: MatchingServiceErrorKind::Conflict,
                message: format!(
                    "candidate {} does not belong to release instance {}",
                    candidate_id.as_uuid(),
                    release_instance_id.as_uuid()
                ),
            });
        }

        let provider_release_id = match (&candidate.provider, &candidate.subject) {
            (CandidateProvider::MusicBrainz, CandidateSubject::Release { provider_id }) => {
                provider_id.clone()
            }
            _ => {
                return Err(MatchingServiceError {
                    kind: MatchingServiceErrorKind::Conflict,
                    message: "only MusicBrainz release candidates can be selected directly"
                        .to_string(),
                });
            }
        };

        let detail = self
            .provider
            .lookup_release(&provider_release_id)
            .await
            .map_err(map_provider_error)?;
        let artist = ensure_artist(&self.repository, &detail)?;
        let release_group = ensure_release_group(&self.repository, &artist, &detail)?;
        let release = ensure_release(&self.repository, &artist, &release_group, &detail)?;
        let release_instance = self.apply_manual_release_override(
            release_instance_id,
            &release.id,
            created_by,
            note,
            created_at_unix_seconds,
        )?;

        Ok(SelectedCandidateMatchReport {
            release_instance,
            release,
            release_group,
            artist,
            candidate,
        })
    }

    pub async fn enrich_release_instance_with_discogs(
        &self,
        release_instance_id: &ReleaseInstanceId,
        persisted_at_unix_seconds: i64,
    ) -> Result<DiscogsEnrichmentReport, MatchingServiceError> {
        let release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| MatchingServiceError {
                kind: MatchingServiceErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        let manifest = latest_manifest(
            self.repository
                .list_staging_manifests_for_batch(&release_instance.import_batch_id)
                .map_err(map_repository_error)?,
            &release_instance.import_batch_id,
        )?;
        let evidence = self
            .repository
            .list_ingest_evidence_for_batch(&release_instance.import_batch_id)
            .map_err(map_repository_error)?;
        let evidence_by_path = evidence_by_discovered_path(&evidence);
        let evidence_by_group = evidence_by_group_key(&evidence);
        let group = manifest
            .grouping
            .groups
            .iter()
            .find(|group| {
                representative_group_path(group) == release_instance.provenance.original_source_path
            })
            .ok_or_else(|| MatchingServiceError {
                kind: MatchingServiceErrorKind::NotFound,
                message: format!(
                    "no staged group found for release instance {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        let summary = summarize_group_evidence(group, &evidence_by_path, &evidence_by_group);
        let query = build_discogs_query(&summary);
        let results = DiscogsMetadataProvider::search_releases(&self.provider, &query, 10)
            .await
            .map_err(map_provider_error)?;
        let persisted_candidates = score_discogs_candidates(
            release_instance_id,
            &summary,
            &query,
            results.clone(),
            persisted_at_unix_seconds,
        );
        self.repository
            .replace_candidate_matches_for_provider(
                release_instance_id,
                &CandidateProvider::Discogs,
                &persisted_candidates,
            )
            .map_err(map_repository_error)?;
        let metadata_snapshot = MetadataSnapshot {
            id: crate::support::ids::MetadataSnapshotId::new(),
            subject: MetadataSubject::ReleaseInstance(release_instance_id.clone()),
            source: MetadataSnapshotSource::DiscogsPayload,
            format: SnapshotFormat::Json,
            payload: serde_json::to_string(
                &results
                    .iter()
                    .map(|item| item.raw_payload.clone())
                    .collect::<Vec<_>>(),
            )
            .map_err(|error| MatchingServiceError {
                kind: MatchingServiceErrorKind::Storage,
                message: format!("failed to serialize Discogs payloads: {error}"),
            })?,
            captured_at_unix_seconds: persisted_at_unix_seconds,
        };
        self.repository
            .create_metadata_snapshots(std::slice::from_ref(&metadata_snapshot))
            .map_err(map_repository_error)?;

        let field_differences = results
            .first()
            .map(|candidate| discogs_field_differences(&summary, candidate))
            .unwrap_or_default();

        Ok(DiscogsEnrichmentReport {
            release_instance,
            query,
            persisted_candidates,
            metadata_snapshot,
            field_differences,
        })
    }
}

fn latest_manifest(
    manifests: Vec<StagingManifest>,
    batch_id: &ImportBatchId,
) -> Result<StagingManifest, MatchingServiceError> {
    manifests
        .into_iter()
        .next()
        .ok_or_else(|| MatchingServiceError {
            kind: MatchingServiceErrorKind::NotFound,
            message: format!("no staging manifest found for batch {}", batch_id.as_uuid()),
        })
}

fn evidence_by_discovered_path(
    evidence: &[IngestEvidenceRecord],
) -> HashMap<PathBuf, &IngestEvidenceRecord> {
    evidence
        .iter()
        .filter_map(|record| match &record.subject {
            IngestEvidenceSubject::DiscoveredPath(path) => Some((path.clone(), record)),
            IngestEvidenceSubject::GroupedReleaseInput { .. } => None,
        })
        .collect()
}

fn evidence_by_group_key(
    evidence: &[IngestEvidenceRecord],
) -> HashMap<String, Vec<&IngestEvidenceRecord>> {
    let mut grouped = HashMap::<String, Vec<&IngestEvidenceRecord>>::new();
    for record in evidence {
        if let IngestEvidenceSubject::GroupedReleaseInput { group_key } = &record.subject {
            grouped.entry(group_key.clone()).or_default().push(record);
        }
    }
    grouped
}

fn summarize_group_evidence(
    group: &StagedReleaseGroup,
    evidence_by_path: &HashMap<PathBuf, &IngestEvidenceRecord>,
    evidence_by_group: &HashMap<String, Vec<&IngestEvidenceRecord>>,
) -> MatchEvidenceSummary {
    let mut embedded_artists = Vec::new();
    let mut embedded_titles = Vec::new();
    let mut embedded_years = Vec::new();
    let mut disc_numbers = BTreeSet::new();
    let mut filename_hints = BTreeSet::new();

    for file_path in &group.file_paths {
        if let Some(stem) = file_path.file_stem().and_then(|value| value.to_str()) {
            filename_hints.insert(stem.to_string());
        }
        if let Some(record) = evidence_by_path.get(file_path) {
            for observation in &record.observations {
                match observation.kind {
                    ObservedValueKind::Artist => embedded_artists.push(observation.value.clone()),
                    ObservedValueKind::ReleaseTitle => {
                        embedded_titles.push(observation.value.clone())
                    }
                    ObservedValueKind::ReleaseYear => {
                        embedded_years.push(observation.value.clone())
                    }
                    ObservedValueKind::DiscNumber => {
                        if let Ok(value) = observation.value.parse::<usize>() {
                            disc_numbers.insert(value);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let mut yaml_artists = Vec::new();
    let mut yaml_titles = Vec::new();
    let mut yaml_years = Vec::new();
    let mut label_hints = BTreeSet::new();
    let mut catalog_hints = BTreeSet::new();
    let mut source_descriptors = BTreeSet::new();
    let mut tracker_identifiers = BTreeSet::new();
    if let Some(records) = evidence_by_group.get(&group.key) {
        for record in records {
            for observation in &record.observations {
                match observation.kind {
                    ObservedValueKind::Artist => yaml_artists.push(observation.value.clone()),
                    ObservedValueKind::ReleaseTitle => yaml_titles.push(observation.value.clone()),
                    ObservedValueKind::ReleaseYear => yaml_years.push(observation.value.clone()),
                    ObservedValueKind::Label => {
                        label_hints.insert(observation.value.clone());
                    }
                    ObservedValueKind::CatalogNumber => {
                        catalog_hints.insert(observation.value.clone());
                    }
                    ObservedValueKind::SourceDescriptor | ObservedValueKind::MediaDescriptor => {
                        source_descriptors.insert(observation.value.clone());
                    }
                    ObservedValueKind::TrackerIdentifier => {
                        tracker_identifiers.insert(observation.value.clone());
                    }
                    _ => {}
                }
            }
        }
    }

    let primary_artist = choose_preferred_value(&embedded_artists, &yaml_artists);
    let release_title = choose_preferred_value(&embedded_titles, &yaml_titles);
    let release_year = choose_preferred_value(&embedded_years, &yaml_years);
    let mut evidence_conflicts = Vec::new();
    push_conflict(
        &mut evidence_conflicts,
        "artist",
        primary_artist.as_deref(),
        majority_value(&yaml_artists).as_deref(),
    );
    push_conflict(
        &mut evidence_conflicts,
        "release title",
        release_title.as_deref(),
        majority_value(&yaml_titles).as_deref(),
    );
    push_conflict(
        &mut evidence_conflicts,
        "release year",
        release_year.as_deref(),
        majority_value(&yaml_years).as_deref(),
    );

    MatchEvidenceSummary {
        primary_artist,
        release_title,
        release_year,
        label_hints: label_hints.into_iter().collect(),
        catalog_hints: catalog_hints.into_iter().collect(),
        track_count: group.file_paths.len(),
        disc_count: if disc_numbers.is_empty() {
            None
        } else {
            Some(disc_numbers.len())
        },
        directory_hint: group_common_parent_name(group),
        filename_hints: filename_hints.into_iter().collect(),
        source_descriptors: source_descriptors.into_iter().collect(),
        tracker_identifiers: tracker_identifiers.into_iter().collect(),
        evidence_conflicts,
    }
}

fn build_release_query(summary: &MatchEvidenceSummary) -> String {
    let mut clauses = Vec::new();
    if let Some(title) = &summary.release_title {
        clauses.push(format!("\"{}\"", escape_query_value(title)));
    }
    if let Some(artist) = &summary.primary_artist {
        clauses.push(format!("artist:\"{}\"", escape_query_value(artist)));
    }
    if let Some(year) = &summary.release_year {
        clauses.push(format!("date:{year}"));
    }
    if summary.track_count > 0 {
        clauses.push(format!("tracks:{}", summary.track_count));
    }
    if clauses.is_empty() {
        summary
            .directory_hint
            .as_ref()
            .map(|value| format!("\"{}\"", escape_query_value(value)))
            .unwrap_or_else(|| "*".to_string())
    } else {
        clauses.join(" AND ")
    }
}

fn build_release_group_query(summary: &MatchEvidenceSummary) -> String {
    let mut clauses = Vec::new();
    if let Some(title) = &summary.release_title {
        clauses.push(format!("\"{}\"", escape_query_value(title)));
    }
    if let Some(artist) = &summary.primary_artist {
        clauses.push(format!("artist:\"{}\"", escape_query_value(artist)));
    }
    if let Some(year) = &summary.release_year {
        clauses.push(format!("firstreleasedate:{year}"));
    }
    if clauses.is_empty() {
        summary
            .directory_hint
            .as_ref()
            .map(|value| format!("\"{}\"", escape_query_value(value)))
            .unwrap_or_else(|| "*".to_string())
    } else {
        clauses.join(" AND ")
    }
}

fn choose_preferred_value(primary: &[String], supporting: &[String]) -> Option<String> {
    majority_value(primary).or_else(|| majority_value(supporting))
}

fn majority_value(values: &[String]) -> Option<String> {
    let mut counts = BTreeMap::<String, usize>::new();
    for value in values {
        *counts.entry(value.clone()).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        .map(|item| item.0)
}

fn push_conflict(
    conflicts: &mut Vec<String>,
    label: &str,
    preferred: Option<&str>,
    supporting: Option<&str>,
) {
    if let (Some(preferred), Some(supporting)) = (preferred, supporting)
        && preferred != supporting
    {
        conflicts.push(format!(
            "{label} evidence differed; retained embedded-tag preference '{}' over '{}'",
            preferred, supporting
        ));
    }
}

fn group_common_parent_name(group: &StagedReleaseGroup) -> Option<String> {
    let first_parent = group.file_paths.first()?.parent()?.to_path_buf();
    if group
        .file_paths
        .iter()
        .all(|path| path.parent() == Some(first_parent.as_path()))
    {
        first_parent
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string)
    } else {
        None
    }
}

fn escape_query_value(value: &str) -> String {
    value.replace('"', "\\\"")
}

fn ensure_provisional_release_instance<R>(
    repository: &R,
    batch: &ImportBatch,
    source: &Source,
    group: &StagedReleaseGroup,
    summary: &MatchEvidenceSummary,
    evidence_by_path: &HashMap<PathBuf, &IngestEvidenceRecord>,
    existing_instances: &[ReleaseInstance],
) -> Result<ReleaseInstance, MatchingServiceError>
where
    R: ReleaseInstanceCommandRepository,
{
    let original_source_path = representative_group_path(group);
    if let Some(existing) = existing_instances
        .iter()
        .find(|item| item.provenance.original_source_path == original_source_path)
        .cloned()
    {
        return Ok(existing);
    }

    let release_instance = ReleaseInstance {
        id: ReleaseInstanceId::new(),
        import_batch_id: batch.id.clone(),
        source_id: source.id.clone(),
        release_id: None,
        state: ReleaseInstanceState::Analyzed,
        technical_variant: infer_technical_variant(group, evidence_by_path),
        provenance: ProvenanceSnapshot {
            ingest_origin: source_kind_to_ingest_origin(&source.kind),
            original_source_path,
            imported_at_unix_seconds: batch.created_at_unix_seconds,
            gazelle_reference: gazelle_reference(summary),
        },
    };
    repository
        .create_release_instance(&release_instance)
        .map_err(map_repository_error)?;
    Ok(release_instance)
}

fn representative_group_path(group: &StagedReleaseGroup) -> String {
    group_common_parent_path(group)
        .or_else(|| {
            group
                .file_paths
                .iter()
                .min()
                .map(|path| path.to_string_lossy().to_string())
        })
        .unwrap_or_default()
}

fn group_common_parent_path(group: &StagedReleaseGroup) -> Option<String> {
    let first_parent = group.file_paths.first()?.parent()?.to_path_buf();
    if group
        .file_paths
        .iter()
        .all(|path| path.parent() == Some(first_parent.as_path()))
    {
        Some(first_parent.to_string_lossy().to_string())
    } else {
        None
    }
}

fn infer_technical_variant(
    group: &StagedReleaseGroup,
    evidence_by_path: &HashMap<PathBuf, &IngestEvidenceRecord>,
) -> TechnicalVariant {
    let mut flac_votes = 0usize;
    let mut mp3_votes = 0usize;
    let mut disc_numbers = BTreeSet::new();
    let mut total_duration_seconds = 0u32;

    for file_path in &group.file_paths {
        match file_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref()
        {
            Some("flac") => flac_votes += 1,
            Some("mp3") => mp3_votes += 1,
            _ => {}
        }

        if let Some(record) = evidence_by_path.get(file_path) {
            for observation in &record.observations {
                match observation.kind {
                    ObservedValueKind::FormatFamily => match observation.value.as_str() {
                        "flac" => flac_votes += 1,
                        "mp3" => mp3_votes += 1,
                        _ => {}
                    },
                    ObservedValueKind::DiscNumber => {
                        if let Ok(value) = observation.value.parse::<usize>() {
                            disc_numbers.insert(value);
                        }
                    }
                    ObservedValueKind::DurationMs => {
                        if let Ok(value) = observation.value.parse::<u32>() {
                            total_duration_seconds += value / 1000;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let format_family = if flac_votes >= mp3_votes {
        FormatFamily::Flac
    } else {
        FormatFamily::Mp3
    };

    TechnicalVariant {
        bitrate_mode: match format_family {
            FormatFamily::Flac => BitrateMode::Lossless,
            FormatFamily::Mp3 => BitrateMode::Variable,
        },
        format_family,
        bitrate_kbps: None,
        sample_rate_hz: None,
        bit_depth: None,
        track_count: group.file_paths.len() as u16,
        total_duration_seconds,
    }
}

fn source_kind_to_ingest_origin(value: &SourceKind) -> IngestOrigin {
    match value {
        SourceKind::WatchDirectory => IngestOrigin::WatchDirectory,
        SourceKind::ApiClient | SourceKind::Gazelle => IngestOrigin::ApiPush,
        SourceKind::ManualAdd => IngestOrigin::ManualAdd,
    }
}

fn gazelle_reference(
    summary: &MatchEvidenceSummary,
) -> Option<crate::domain::release_instance::GazelleReference> {
    summary.tracker_identifiers.first().map(|identifier| {
        let (tracker, torrent_id) = identifier
            .split_once(':')
            .map(|(tracker, value)| (tracker.to_string(), Some(value.to_string())))
            .unwrap_or_else(|| ("gazelle".to_string(), Some(identifier.clone())));
        crate::domain::release_instance::GazelleReference {
            tracker,
            torrent_id,
            release_group_id: None,
        }
    })
}

fn score_group_candidates(
    release_instance_id: &ReleaseInstanceId,
    summary: &MatchEvidenceSummary,
    release_query: &str,
    release_group_query: &str,
    release_candidates: Vec<MusicBrainzReleaseCandidate>,
    release_group_candidates: Vec<MusicBrainzReleaseGroupCandidate>,
    persisted_at_unix_seconds: i64,
) -> Vec<CandidateMatch> {
    let mut candidates = release_candidates
        .into_iter()
        .map(|candidate| {
            score_release_candidate(
                release_instance_id,
                summary,
                release_query,
                candidate,
                persisted_at_unix_seconds,
            )
        })
        .chain(release_group_candidates.into_iter().map(|candidate| {
            score_release_group_candidate(
                release_instance_id,
                summary,
                release_group_query,
                candidate,
                persisted_at_unix_seconds,
            )
        }))
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        right
            .normalized_score
            .value()
            .total_cmp(&left.normalized_score.value())
    });

    let top_candidate_subject = candidates
        .first()
        .map(|candidate| candidate.subject.clone());
    let top_score = candidates
        .first()
        .map(|candidate| candidate.normalized_score.value())
        .unwrap_or(0.0);
    for candidate in &mut candidates {
        if top_score >= 0.80
            && (top_score - candidate.normalized_score.value()).abs() <= 0.05
            && Some(candidate.subject.clone()) != top_candidate_subject
        {
            candidate.unresolved_ambiguities.push(
                "another high-confidence candidate remained within review distance".to_string(),
            );
        }
    }

    candidates
}

fn score_release_candidate(
    release_instance_id: &ReleaseInstanceId,
    summary: &MatchEvidenceSummary,
    query: &str,
    candidate: MusicBrainzReleaseCandidate,
    persisted_at_unix_seconds: i64,
) -> CandidateMatch {
    let mut evidence_matches = Vec::new();
    let mut mismatches = Vec::new();
    let mut score = provider_rank_score(candidate.score, 0.20);

    compare_artist(
        summary.primary_artist.as_deref(),
        &candidate.artist_names,
        &mut evidence_matches,
        &mut mismatches,
        &mut score,
    );
    compare_release_title(
        summary.release_title.as_deref(),
        Some(candidate.title.as_str()),
        &mut evidence_matches,
        &mut mismatches,
        &mut score,
    );
    compare_track_count(
        summary.track_count,
        candidate.track_count,
        &mut evidence_matches,
        &mut mismatches,
        &mut score,
    );
    compare_release_year(
        summary.release_year.as_deref(),
        candidate.date.as_deref(),
        &mut evidence_matches,
        &mut mismatches,
        &mut score,
    );
    compare_release_group_title(
        summary.release_title.as_deref(),
        candidate.release_group_title.as_deref(),
        &mut evidence_matches,
        &mut score,
    );

    CandidateMatch {
        id: CandidateMatchId::new(),
        release_instance_id: release_instance_id.clone(),
        provider: CandidateProvider::MusicBrainz,
        subject: CandidateSubject::Release {
            provider_id: candidate.id,
        },
        normalized_score: CandidateScore::new(score.clamp(0.0, 1.0)),
        evidence_matches,
        mismatches,
        unresolved_ambiguities: summary.evidence_conflicts.clone(),
        provider_provenance: ProviderProvenance {
            provider_name: "musicbrainz".to_string(),
            query: query.to_string(),
            fetched_at_unix_seconds: persisted_at_unix_seconds,
        },
    }
}

fn score_release_group_candidate(
    release_instance_id: &ReleaseInstanceId,
    summary: &MatchEvidenceSummary,
    query: &str,
    candidate: MusicBrainzReleaseGroupCandidate,
    persisted_at_unix_seconds: i64,
) -> CandidateMatch {
    let mut evidence_matches = Vec::new();
    let mut mismatches = Vec::new();
    let mut score = provider_rank_score(candidate.score, 0.15);

    compare_artist(
        summary.primary_artist.as_deref(),
        &candidate.artist_names,
        &mut evidence_matches,
        &mut mismatches,
        &mut score,
    );
    compare_release_title(
        summary.release_title.as_deref(),
        Some(candidate.title.as_str()),
        &mut evidence_matches,
        &mut mismatches,
        &mut score,
    );
    compare_release_year(
        summary.release_year.as_deref(),
        candidate.first_release_date.as_deref(),
        &mut evidence_matches,
        &mut mismatches,
        &mut score,
    );

    CandidateMatch {
        id: CandidateMatchId::new(),
        release_instance_id: release_instance_id.clone(),
        provider: CandidateProvider::MusicBrainz,
        subject: CandidateSubject::ReleaseGroup {
            provider_id: candidate.id,
        },
        normalized_score: CandidateScore::new(score.clamp(0.0, 1.0)),
        evidence_matches,
        mismatches,
        unresolved_ambiguities: summary.evidence_conflicts.clone(),
        provider_provenance: ProviderProvenance {
            provider_name: "musicbrainz".to_string(),
            query: query.to_string(),
            fetched_at_unix_seconds: persisted_at_unix_seconds,
        },
    }
}

fn provider_rank_score(score: u16, weight: f32) -> f32 {
    ((score as f32 / 100.0) * weight).clamp(0.0, weight)
}

fn compare_artist(
    expected: Option<&str>,
    candidates: &[String],
    matches: &mut Vec<EvidenceNote>,
    mismatches: &mut Vec<EvidenceNote>,
    score: &mut f32,
) {
    if let Some(expected) = expected {
        if candidates
            .iter()
            .any(|value| normalize_text(value) == normalize_text(expected))
        {
            *score += 0.25;
            matches.push(note(EvidenceKind::ArtistMatch, "artist names aligned"));
        } else {
            mismatches.push(note(
                EvidenceKind::ArtistMatch,
                format!("expected artist '{expected}' did not align"),
            ));
        }
    }
}

fn compare_release_title(
    expected: Option<&str>,
    observed: Option<&str>,
    matches: &mut Vec<EvidenceNote>,
    mismatches: &mut Vec<EvidenceNote>,
    score: &mut f32,
) {
    if let Some(expected) = expected {
        if observed
            .map(|value| normalize_text(value) == normalize_text(expected))
            .unwrap_or(false)
        {
            *score += 0.30;
            matches.push(note(EvidenceKind::AlbumTitleMatch, "release title aligned"));
        } else {
            mismatches.push(note(
                EvidenceKind::AlbumTitleMatch,
                format!("expected title '{expected}' did not align"),
            ));
        }
    }
}

fn compare_track_count(
    expected: usize,
    observed: Option<u32>,
    matches: &mut Vec<EvidenceNote>,
    mismatches: &mut Vec<EvidenceNote>,
    score: &mut f32,
) {
    if expected == 0 {
        return;
    }
    if observed == Some(expected as u32) {
        *score += 0.15;
        matches.push(note(
            EvidenceKind::TrackCountMatch,
            format!("track count matched at {expected}"),
        ));
    } else if let Some(observed) = observed {
        mismatches.push(note(
            EvidenceKind::TrackCountMatch,
            format!("expected {expected} tracks, found {observed}"),
        ));
    }
}

fn compare_release_year(
    expected: Option<&str>,
    observed: Option<&str>,
    matches: &mut Vec<EvidenceNote>,
    mismatches: &mut Vec<EvidenceNote>,
    score: &mut f32,
) {
    let expected_year = expected.and_then(parse_year);
    let observed_year = observed.and_then(parse_year);
    match (expected_year, observed_year) {
        (Some(expected_year), Some(observed_year)) if expected_year == observed_year => {
            *score += 0.10;
            matches.push(note(
                EvidenceKind::DateProximity,
                format!("release year matched at {expected_year}"),
            ));
        }
        (Some(expected_year), Some(observed_year))
            if expected_year.abs_diff(observed_year) == 1 =>
        {
            *score += 0.05;
            matches.push(note(
                EvidenceKind::DateProximity,
                format!("release year was within one year ({observed_year})"),
            ));
        }
        (Some(expected_year), Some(observed_year)) => mismatches.push(note(
            EvidenceKind::DateProximity,
            format!("expected year {expected_year}, found {observed_year}"),
        )),
        _ => {}
    }
}

fn compare_release_group_title(
    expected: Option<&str>,
    observed: Option<&str>,
    matches: &mut Vec<EvidenceNote>,
    score: &mut f32,
) {
    if expected.is_some()
        && observed
            .map(|value| normalize_text(value) == normalize_text(expected.unwrap_or_default()))
            .unwrap_or(false)
    {
        *score += 0.05;
        matches.push(note(
            EvidenceKind::AlbumTitleMatch,
            "release-group title aligned",
        ));
    }
}

fn note(kind: EvidenceKind, detail: impl Into<String>) -> EvidenceNote {
    EvidenceNote {
        kind,
        detail: detail.into(),
    }
}

fn normalize_text(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|value| value.is_alphanumeric())
        .collect()
}

fn parse_year(value: &str) -> Option<u16> {
    value.get(0..4)?.parse().ok()
}

fn needs_review(candidates: &[CandidateMatch]) -> bool {
    let Some(best) = candidates.first() else {
        return true;
    };

    if !matches!(best.subject, CandidateSubject::Release { .. }) {
        return true;
    }

    if best.normalized_score.value() < 0.80 || !best.unresolved_ambiguities.is_empty() {
        return true;
    }

    candidates
        .iter()
        .skip(1)
        .any(|candidate| best.normalized_score.value() - candidate.normalized_score.value() <= 0.05)
}

fn review_issue_type(candidates: &[CandidateMatch]) -> Option<IssueType> {
    let best = candidates.first()?;
    let competing_release = candidates.iter().skip(1).any(|candidate| {
        matches!(candidate.subject, CandidateSubject::Release { .. })
            && best.normalized_score.value() - candidate.normalized_score.value() <= 0.05
    });

    if !matches!(best.subject, CandidateSubject::Release { .. })
        || best.normalized_score.value() < 0.80
    {
        return Some(IssueType::UnmatchedRelease);
    }

    if !best.unresolved_ambiguities.is_empty() || competing_release {
        return Some(IssueType::AmbiguousReleaseMatch);
    }

    None
}

fn synchronize_review_issues<R>(
    repository: &R,
    release_instance: &ReleaseInstance,
    candidates: &[CandidateMatch],
    changed_at_unix_seconds: i64,
) -> Result<(), MatchingServiceError>
where
    R: IssueRepository + IssueCommandRepository,
{
    let desired_issue = review_issue_type(candidates);
    let subject = IssueSubject::ReleaseInstance(release_instance.id.clone());

    for issue_type in [
        IssueType::UnmatchedRelease,
        IssueType::AmbiguousReleaseMatch,
    ] {
        let existing = repository
            .list_issues(&IssueListQuery {
                state: None,
                issue_type: Some(issue_type.clone()),
                subject: Some(subject.clone()),
                page: PageRequest::new(20, 0),
            })
            .map_err(map_repository_error)?;

        for mut issue in existing.items {
            if issue.state != IssueState::Open {
                continue;
            }

            if desired_issue.as_ref() == Some(&issue_type) {
                let (summary, details) = build_review_issue_content(release_instance, candidates);
                if issue.summary != summary || issue.details != Some(details.clone()) {
                    issue.summary = summary;
                    issue.details = Some(details);
                    repository
                        .update_issue(&issue)
                        .map_err(map_repository_error)?;
                }
            } else {
                issue
                    .resolve(changed_at_unix_seconds)
                    .map_err(map_issue_lifecycle_error)?;
                repository
                    .update_issue(&issue)
                    .map_err(map_repository_error)?;
            }
        }
    }

    if let Some(issue_type) = desired_issue {
        let existing_open = repository
            .list_issues(&IssueListQuery {
                state: Some(IssueState::Open),
                issue_type: Some(issue_type.clone()),
                subject: Some(subject.clone()),
                page: PageRequest::new(1, 0),
            })
            .map_err(map_repository_error)?;
        if existing_open.items.is_empty() {
            let (summary, details) = build_review_issue_content(release_instance, candidates);
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

    Ok(())
}

fn apply_match_outcome<R>(
    repository: &R,
    release_instance: &mut ReleaseInstance,
    candidates: &[CandidateMatch],
    changed_at_unix_seconds: i64,
) -> Result<(), MatchingServiceError>
where
    R: IssueCommandRepository
        + IssueRepository
        + ManualOverrideRepository
        + ReleaseInstanceCommandRepository,
{
    if let Some(release_id) = latest_manual_release_override(repository, &release_instance.id)? {
        release_instance.release_id = Some(release_id);
        release_instance.state = ReleaseInstanceState::Matched;
        repository
            .update_release_instance(release_instance)
            .map_err(map_repository_error)?;
        resolve_review_issues_for_subject(
            repository,
            &IssueSubject::ReleaseInstance(release_instance.id.clone()),
            changed_at_unix_seconds,
        )?;
        return Ok(());
    }

    release_instance.state = if needs_review(candidates) {
        ReleaseInstanceState::NeedsReview
    } else {
        ReleaseInstanceState::Analyzed
    };
    repository
        .update_release_instance(release_instance)
        .map_err(map_repository_error)?;
    synchronize_review_issues(
        repository,
        release_instance,
        candidates,
        changed_at_unix_seconds,
    )
}

fn latest_manual_release_override<R>(
    repository: &R,
    release_instance_id: &ReleaseInstanceId,
) -> Result<Option<crate::support::ids::ReleaseId>, MatchingServiceError>
where
    R: ManualOverrideRepository,
{
    let overrides = repository
        .list_manual_overrides(&ManualOverrideListQuery {
            subject: Some(OverrideSubject::ReleaseInstance(
                release_instance_id.clone(),
            )),
            field: Some(OverrideField::ReleaseMatch),
            page: PageRequest::new(1, 0),
        })
        .map_err(map_repository_error)?;
    let Some(latest) = overrides.items.first() else {
        return Ok(None);
    };
    Ok(crate::support::ids::ReleaseId::parse_str(&latest.value).ok())
}

fn resolve_review_issues_for_subject<R>(
    repository: &R,
    subject: &IssueSubject,
    changed_at_unix_seconds: i64,
) -> Result<(), MatchingServiceError>
where
    R: IssueCommandRepository + IssueRepository,
{
    for issue_type in [
        IssueType::UnmatchedRelease,
        IssueType::AmbiguousReleaseMatch,
    ] {
        let existing = repository
            .list_issues(&IssueListQuery {
                state: Some(IssueState::Open),
                issue_type: Some(issue_type),
                subject: Some(subject.clone()),
                page: PageRequest::new(20, 0),
            })
            .map_err(map_repository_error)?;
        for mut issue in existing.items {
            issue
                .resolve(changed_at_unix_seconds)
                .map_err(map_issue_lifecycle_error)?;
            repository
                .update_issue(&issue)
                .map_err(map_repository_error)?;
        }
    }
    Ok(())
}

fn build_review_issue_content(
    release_instance: &ReleaseInstance,
    candidates: &[CandidateMatch],
) -> (String, String) {
    let path = release_instance.provenance.original_source_path.clone();
    match review_issue_type(candidates) {
        Some(IssueType::UnmatchedRelease) => {
            let best_score = candidates
                .first()
                .map(|candidate| format!("{:.2}", candidate.normalized_score.value()))
                .unwrap_or_else(|| "none".to_string());
            (
                format!("Release match unresolved for {path}"),
                format!(
                    "No canonical MusicBrainz release cleared the review threshold. Best score: {best_score}."
                ),
            )
        }
        Some(IssueType::AmbiguousReleaseMatch) => {
            let candidate_count = candidates
                .iter()
                .filter(|candidate| matches!(candidate.subject, CandidateSubject::Release { .. }))
                .count();
            let ambiguity_count = candidates
                .first()
                .map(|candidate| candidate.unresolved_ambiguities.len())
                .unwrap_or_default();
            (
                format!("Release match is ambiguous for {path}"),
                format!(
                    "Found {candidate_count} competing release candidates with {ambiguity_count} unresolved ambiguity notes."
                ),
            )
        }
        None => (
            format!("Release review cleared for {path}"),
            "Matching no longer requires operator review.".to_string(),
        ),
        _ => unreachable!("review issues are limited to unmatched and ambiguous releases"),
    }
}

fn best_musicbrainz_release_id(candidates: &[CandidateMatch]) -> Option<String> {
    candidates.iter().find_map(|candidate| {
        (candidate.provider == CandidateProvider::MusicBrainz).then_some(())?;
        match &candidate.subject {
            CandidateSubject::Release { provider_id } => Some(provider_id.clone()),
            CandidateSubject::ReleaseGroup { .. } => None,
        }
    })
}

fn ensure_artist<R>(
    repository: &R,
    detail: &MusicBrainzReleaseDetail,
) -> Result<Artist, MatchingServiceError>
where
    R: ReleaseCommandRepository + ReleaseRepository,
{
    let primary_credit = detail
        .artist_credit
        .first()
        .ok_or_else(|| MatchingServiceError {
            kind: MatchingServiceErrorKind::Provider,
            message: format!("release {} did not include artist credits", detail.id),
        })?;
    if let Some(existing) = repository
        .find_artist_by_musicbrainz_id(&primary_credit.artist_id)
        .map_err(map_repository_error)?
    {
        return Ok(existing);
    }

    let artist = Artist {
        id: crate::support::ids::ArtistId::new(),
        name: primary_credit.artist_name.clone(),
        sort_name: Some(primary_credit.artist_sort_name.clone()),
        musicbrainz_artist_id: crate::support::ids::MusicBrainzArtistId::parse_str(
            &primary_credit.artist_id,
        )
        .ok(),
    };
    repository
        .create_artist(&artist)
        .map_err(map_repository_error)?;
    Ok(artist)
}

fn ensure_release_group<R>(
    repository: &R,
    artist: &Artist,
    detail: &MusicBrainzReleaseDetail,
) -> Result<ReleaseGroup, MatchingServiceError>
where
    R: ReleaseCommandRepository + ReleaseRepository,
{
    let Some(group_ref) = detail.release_group.as_ref() else {
        return Err(MatchingServiceError {
            kind: MatchingServiceErrorKind::Provider,
            message: format!("release {} did not include a release group", detail.id),
        });
    };
    if let Some(existing) = repository
        .find_release_group_by_musicbrainz_id(&group_ref.id)
        .map_err(map_repository_error)?
    {
        return Ok(existing);
    }

    let release_group =
        ReleaseGroup {
            id: crate::support::ids::ReleaseGroupId::new(),
            primary_artist_id: artist.id.clone(),
            title: group_ref.title.clone(),
            kind: map_release_group_kind(group_ref.primary_type.as_deref()),
            musicbrainz_release_group_id:
                crate::support::ids::MusicBrainzReleaseGroupId::parse_str(&group_ref.id).ok(),
        };
    repository
        .create_release_group(&release_group)
        .map_err(map_repository_error)?;
    Ok(release_group)
}

fn ensure_release<R>(
    repository: &R,
    artist: &Artist,
    release_group: &ReleaseGroup,
    detail: &MusicBrainzReleaseDetail,
) -> Result<Release, MatchingServiceError>
where
    R: ReleaseCommandRepository + ReleaseRepository,
{
    if let Some(existing) = repository
        .find_release_by_musicbrainz_id(&detail.id)
        .map_err(map_repository_error)?
    {
        return Ok(existing);
    }

    let release = Release {
        id: crate::support::ids::ReleaseId::new(),
        release_group_id: release_group.id.clone(),
        primary_artist_id: artist.id.clone(),
        title: detail.title.clone(),
        musicbrainz_release_id: crate::support::ids::MusicBrainzReleaseId::parse_str(&detail.id)
            .ok(),
        discogs_release_id: None,
        edition: ReleaseEdition {
            edition_title: None,
            disambiguation: None,
            country: detail.country.clone(),
            label: detail
                .label_info
                .iter()
                .find_map(|label| label.label_name.clone()),
            catalog_number: detail
                .label_info
                .iter()
                .find_map(|label| label.catalog_number.clone()),
            release_date: parse_partial_date(detail.date.as_deref()),
        },
    };
    repository
        .create_release(&release)
        .map_err(map_repository_error)?;
    Ok(release)
}

fn map_release_group_kind(primary_type: Option<&str>) -> ReleaseGroupKind {
    match primary_type.map(normalize_text).as_deref() {
        Some("album") => ReleaseGroupKind::Album,
        Some("ep") => ReleaseGroupKind::Ep,
        Some("single") => ReleaseGroupKind::Single,
        Some("live") => ReleaseGroupKind::Live,
        Some("compilation") => ReleaseGroupKind::Compilation,
        Some("soundtrack") => ReleaseGroupKind::Soundtrack,
        Some(other) => ReleaseGroupKind::Other(other.to_string()),
        None => ReleaseGroupKind::Album,
    }
}

fn parse_partial_date(value: Option<&str>) -> Option<PartialDate> {
    let value = value?;
    let mut parts = value.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next().and_then(|part| part.parse().ok());
    let day = parts.next().and_then(|part| part.parse().ok());
    Some(PartialDate { year, month, day })
}

fn build_discogs_query(summary: &MatchEvidenceSummary) -> DiscogsReleaseQuery {
    DiscogsReleaseQuery {
        text: summary
            .release_title
            .clone()
            .or_else(|| summary.directory_hint.clone()),
        artist: summary.primary_artist.clone(),
        title: summary.release_title.clone(),
        year: summary.release_year.clone(),
        label: summary.label_hints.first().cloned(),
        catalog_number: summary.catalog_hints.first().cloned(),
        format_hint: summary.source_descriptors.first().cloned(),
    }
}

fn score_discogs_candidates(
    release_instance_id: &ReleaseInstanceId,
    summary: &MatchEvidenceSummary,
    query: &DiscogsReleaseQuery,
    candidates: Vec<DiscogsReleaseCandidate>,
    persisted_at_unix_seconds: i64,
) -> Vec<CandidateMatch> {
    let mut matches = candidates
        .into_iter()
        .map(|candidate| {
            let mut evidence_matches = Vec::new();
            let mut mismatches = Vec::new();
            let mut score = 0.10;

            compare_artist(
                summary.primary_artist.as_deref(),
                &candidate.artist.iter().cloned().collect::<Vec<_>>(),
                &mut evidence_matches,
                &mut mismatches,
                &mut score,
            );
            compare_release_title(
                summary.release_title.as_deref(),
                Some(candidate.title.as_str()),
                &mut evidence_matches,
                &mut mismatches,
                &mut score,
            );
            compare_release_year(
                summary.release_year.as_deref(),
                candidate.year.as_deref(),
                &mut evidence_matches,
                &mut mismatches,
                &mut score,
            );
            compare_label_catalog(
                &summary.label_hints,
                &summary.catalog_hints,
                candidate.label.as_deref(),
                candidate.catalog_number.as_deref(),
                &mut evidence_matches,
                &mut mismatches,
                &mut score,
            );
            compare_format_descriptors(
                &summary.source_descriptors,
                &candidate.format_descriptors,
                &mut evidence_matches,
                &mut score,
            );

            CandidateMatch {
                id: CandidateMatchId::new(),
                release_instance_id: release_instance_id.clone(),
                provider: CandidateProvider::Discogs,
                subject: CandidateSubject::Release {
                    provider_id: candidate.id,
                },
                normalized_score: CandidateScore::new(score.clamp(0.0, 1.0)),
                evidence_matches,
                mismatches,
                unresolved_ambiguities: Vec::new(),
                provider_provenance: ProviderProvenance {
                    provider_name: "discogs".to_string(),
                    query: format!("{query:?}"),
                    fetched_at_unix_seconds: persisted_at_unix_seconds,
                },
            }
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .normalized_score
            .value()
            .total_cmp(&left.normalized_score.value())
    });
    matches
}

fn compare_label_catalog(
    expected_labels: &[String],
    expected_catalogs: &[String],
    provider_label: Option<&str>,
    provider_catalog: Option<&str>,
    matches: &mut Vec<EvidenceNote>,
    mismatches: &mut Vec<EvidenceNote>,
    score: &mut f32,
) {
    if provider_label
        .map(|value| {
            expected_labels
                .iter()
                .any(|item| normalize_text(item) == normalize_text(value))
        })
        .unwrap_or(false)
    {
        *score += 0.20;
        matches.push(note(
            EvidenceKind::LabelCatalogAlignment,
            "label aligned with local evidence",
        ));
    } else if !expected_labels.is_empty() && provider_label.is_some() {
        mismatches.push(note(
            EvidenceKind::LabelCatalogAlignment,
            "label differed from local evidence",
        ));
    }

    if provider_catalog
        .map(|value| {
            expected_catalogs
                .iter()
                .any(|item| normalize_text(item) == normalize_text(value))
        })
        .unwrap_or(false)
    {
        *score += 0.25;
        matches.push(note(
            EvidenceKind::LabelCatalogAlignment,
            "catalog number aligned with local evidence",
        ));
    } else if !expected_catalogs.is_empty() && provider_catalog.is_some() {
        mismatches.push(note(
            EvidenceKind::LabelCatalogAlignment,
            "catalog number differed from local evidence",
        ));
    }
}

fn compare_format_descriptors(
    expected_descriptors: &[String],
    provider_descriptors: &[String],
    matches: &mut Vec<EvidenceNote>,
    score: &mut f32,
) {
    if expected_descriptors.iter().any(|expected| {
        provider_descriptors
            .iter()
            .any(|provider| normalize_text(provider).contains(&normalize_text(expected)))
    }) {
        *score += 0.10;
        matches.push(note(
            EvidenceKind::GazelleConsistency,
            "format or source descriptors aligned",
        ));
    }
}

fn discogs_field_differences(
    summary: &MatchEvidenceSummary,
    candidate: &DiscogsReleaseCandidate,
) -> Vec<DiscogsFieldDifference> {
    vec![
        DiscogsFieldDifference {
            field: "label".to_string(),
            local_value: summary.label_hints.first().cloned(),
            provider_value: candidate.label.clone(),
        },
        DiscogsFieldDifference {
            field: "catalog_number".to_string(),
            local_value: summary.catalog_hints.first().cloned(),
            provider_value: candidate.catalog_number.clone(),
        },
        DiscogsFieldDifference {
            field: "year".to_string(),
            local_value: summary.release_year.clone(),
            provider_value: candidate.year.clone(),
        },
        DiscogsFieldDifference {
            field: "country".to_string(),
            local_value: None,
            provider_value: candidate.country.clone(),
        },
    ]
}

fn map_repository_error(error: RepositoryError) -> MatchingServiceError {
    MatchingServiceError {
        kind: match error.kind {
            RepositoryErrorKind::NotFound => MatchingServiceErrorKind::NotFound,
            RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                MatchingServiceErrorKind::Conflict
            }
            RepositoryErrorKind::Storage => MatchingServiceErrorKind::Storage,
        },
        message: error.message,
    }
}

fn map_provider_error(message: String) -> MatchingServiceError {
    MatchingServiceError {
        kind: MatchingServiceErrorKind::Provider,
        message,
    }
}

fn map_issue_lifecycle_error(
    error: crate::domain::issue::IssueLifecycleError,
) -> MatchingServiceError {
    MatchingServiceError {
        kind: MatchingServiceErrorKind::Conflict,
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
    use super::*;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use crate::application::repository::{
        ImportBatchRepository, IssueCommandRepository, IssueListQuery, IssueRepository,
        ManualOverrideCommandRepository, ManualOverrideListQuery, ManualOverrideRepository,
        MetadataSnapshotCommandRepository, ReleaseCommandRepository,
        ReleaseInstanceCommandRepository, ReleaseInstanceRepository, ReleaseRepository,
        SourceRepository,
    };
    use crate::domain::artist::Artist;
    use crate::domain::candidate_match::CandidateMatch;
    use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
    use crate::domain::ingest_evidence::{
        IngestEvidenceRecord, IngestEvidenceSource, IngestEvidenceSubject, ObservedValue,
    };
    use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
    use crate::domain::manual_override::{ManualOverride, OverrideField};
    use crate::domain::release_instance::{ReleaseInstance, ReleaseInstanceState};
    use crate::domain::source::{Source, SourceKind, SourceLocator};
    use crate::domain::staging_manifest::{
        AuxiliaryFile, GroupingDecision, GroupingStrategy, StagingManifestSource,
    };
    use crate::support::ids::{
        CandidateMatchId, ImportBatchId, IngestEvidenceId, ReleaseInstanceId, SourceId,
        StagingManifestId,
    };
    use crate::support::pagination::{Page, PageRequest};

    #[test]
    fn evidence_summary_prefers_embedded_tags_over_yaml_conflicts() {
        let group = StagedReleaseGroup {
            key: "kid-a".to_string(),
            file_paths: vec![PathBuf::from("/incoming/Kid A/01 Everything.mp3")],
            auxiliary_paths: vec![PathBuf::from("/incoming/Kid A/release.yaml")],
        };
        let file_path = group.file_paths[0].clone();
        let path_records = [embedded_record(
            &ImportBatchId::new(),
            &file_path,
            vec![
                observed(ObservedValueKind::Artist, "Radiohead"),
                observed(ObservedValueKind::ReleaseTitle, "Kid A"),
                observed(ObservedValueKind::ReleaseYear, "2000"),
                observed(ObservedValueKind::DiscNumber, "1"),
            ],
        )];
        let path_evidence = evidence_by_discovered_path(&path_records);
        let group_records = [yaml_record(
            &ImportBatchId::new(),
            "kid-a",
            vec![
                observed(ObservedValueKind::Artist, "Radio Head"),
                observed(ObservedValueKind::ReleaseTitle, "Kid A (Promo)"),
                observed(ObservedValueKind::ReleaseYear, "2001"),
                observed(ObservedValueKind::TrackerIdentifier, "1234"),
            ],
        )];
        let group_evidence = evidence_by_group_key(&group_records);

        let summary = summarize_group_evidence(&group, &path_evidence, &group_evidence);

        assert_eq!(summary.primary_artist.as_deref(), Some("Radiohead"));
        assert_eq!(summary.release_title.as_deref(), Some("Kid A"));
        assert_eq!(summary.release_year.as_deref(), Some("2000"));
        assert_eq!(summary.track_count, 1);
        assert_eq!(summary.directory_hint.as_deref(), Some("Kid A"));
        assert_eq!(summary.tracker_identifiers, vec!["1234".to_string()]);
        assert_eq!(summary.evidence_conflicts.len(), 3);
    }

    #[test]
    fn query_building_uses_conservative_strongest_evidence() {
        let summary = MatchEvidenceSummary {
            primary_artist: Some("Radiohead".to_string()),
            release_title: Some("Kid A".to_string()),
            release_year: Some("2000".to_string()),
            label_hints: Vec::new(),
            catalog_hints: Vec::new(),
            track_count: 10,
            disc_count: Some(1),
            directory_hint: Some("Kid A".to_string()),
            filename_hints: Vec::new(),
            source_descriptors: Vec::new(),
            tracker_identifiers: Vec::new(),
            evidence_conflicts: Vec::new(),
        };

        assert_eq!(
            build_release_query(&summary),
            "\"Kid A\" AND artist:\"Radiohead\" AND date:2000 AND tracks:10"
        );
        assert_eq!(
            build_release_group_query(&summary),
            "\"Kid A\" AND artist:\"Radiohead\" AND firstreleasedate:2000"
        );
    }

    #[tokio::test]
    async fn matching_service_fetches_provider_candidates_per_group() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let provider = FakeMusicBrainzProvider::default();
        let service = ReleaseMatchingService::new(repository, provider.clone());

        let report = service
            .probe_batch_matches(&batch_id)
            .await
            .expect("matching probe should succeed");

        assert_eq!(report.groups.len(), 1);
        assert_eq!(report.groups[0].release_candidates.len(), 1);
        assert_eq!(report.groups[0].release_group_candidates.len(), 1);
        assert!(
            provider
                .queries()
                .iter()
                .any(|query| query.contains("artist:\"Radiohead\""))
        );
    }

    #[tokio::test]
    async fn scoring_persists_candidates_and_marks_reviewable_groups() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let provider = FakeMusicBrainzProvider::with_release_candidates(vec![
            MusicBrainzReleaseCandidate {
                id: "release-strong".to_string(),
                title: "Kid A".to_string(),
                score: 96,
                artist_names: vec!["Radiohead".to_string()],
                release_group_id: Some("group-1".to_string()),
                release_group_title: Some("Kid A".to_string()),
                country: Some("GB".to_string()),
                date: Some("2000-10-02".to_string()),
                track_count: Some(1),
            },
            MusicBrainzReleaseCandidate {
                id: "release-near".to_string(),
                title: "Kid A".to_string(),
                score: 94,
                artist_names: vec!["Radiohead".to_string()],
                release_group_id: Some("group-2".to_string()),
                release_group_title: Some("Kid A".to_string()),
                country: Some("GB".to_string()),
                date: Some("2000-10-03".to_string()),
                track_count: Some(1),
            },
        ]);
        let service = ReleaseMatchingService::new(repository.clone(), provider);

        let report = service
            .score_and_persist_batch_matches(&batch_id, 77)
            .await
            .expect("candidate scoring should succeed");

        assert_eq!(report.groups.len(), 1);
        assert_eq!(
            report.groups[0].release_instance.state,
            ReleaseInstanceState::NeedsReview
        );
        assert_eq!(report.groups[0].persisted_candidates.len(), 3);
        assert!(
            report.groups[0]
                .persisted_candidates
                .iter()
                .all(|candidate| candidate.release_instance_id
                    == report.groups[0].release_instance.id)
        );

        let stored = repository
            .stored_candidates(&report.groups[0].release_instance.id)
            .expect("stored candidates should exist");
        assert_eq!(stored.len(), 3);
        assert!(stored.iter().any(|candidate| {
            matches!(
                candidate.subject,
                CandidateSubject::Release {
                    ref provider_id
                } if provider_id == "release-strong"
            )
        }));
        let issues = repository.issues_for(
            IssueSubject::ReleaseInstance(report.groups[0].release_instance.id.clone()),
            IssueType::AmbiguousReleaseMatch,
        );
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].state, IssueState::Open);
    }

    #[tokio::test]
    async fn scoring_opens_unmatched_issue_for_weak_candidates() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let provider =
            FakeMusicBrainzProvider::with_release_candidates(vec![MusicBrainzReleaseCandidate {
                id: "release-weak".to_string(),
                title: "Kid A Tribute".to_string(),
                score: 40,
                artist_names: vec!["Various Artists".to_string()],
                release_group_id: Some("group-weak".to_string()),
                release_group_title: Some("Kid A Tribute".to_string()),
                country: Some("US".to_string()),
                date: Some("2001-01-01".to_string()),
                track_count: Some(14),
            }]);
        let service = ReleaseMatchingService::new(repository.clone(), provider);

        let report = service
            .score_and_persist_batch_matches(&batch_id, 90)
            .await
            .expect("candidate scoring should succeed");

        let issues = repository.issues_for(
            IssueSubject::ReleaseInstance(report.groups[0].release_instance.id.clone()),
            IssueType::UnmatchedRelease,
        );
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].state, IssueState::Open);
        assert!(issues[0].summary.contains("unresolved"));
    }

    #[tokio::test]
    async fn rescoring_resolves_open_review_issue_once_confident() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let initial_provider = FakeMusicBrainzProvider::with_release_candidates(vec![
            MusicBrainzReleaseCandidate {
                id: "release-strong".to_string(),
                title: "Kid A".to_string(),
                score: 96,
                artist_names: vec!["Radiohead".to_string()],
                release_group_id: Some("group-1".to_string()),
                release_group_title: Some("Kid A".to_string()),
                country: Some("GB".to_string()),
                date: Some("2000-10-02".to_string()),
                track_count: Some(1),
            },
            MusicBrainzReleaseCandidate {
                id: "release-near".to_string(),
                title: "Kid A".to_string(),
                score: 94,
                artist_names: vec!["Radiohead".to_string()],
                release_group_id: Some("group-2".to_string()),
                release_group_title: Some("Kid A".to_string()),
                country: Some("GB".to_string()),
                date: Some("2000-10-03".to_string()),
                track_count: Some(1),
            },
        ]);
        ReleaseMatchingService::new(repository.clone(), initial_provider)
            .score_and_persist_batch_matches(&batch_id, 100)
            .await
            .expect("initial ambiguous scoring should succeed");

        let strong_provider = FakeMusicBrainzProvider::default();
        let report = ReleaseMatchingService::new(repository.clone(), strong_provider)
            .score_and_persist_batch_matches(&batch_id, 120)
            .await
            .expect("rescoring should succeed");

        let ambiguous_issues = repository.issues_for(
            IssueSubject::ReleaseInstance(report.groups[0].release_instance.id.clone()),
            IssueType::AmbiguousReleaseMatch,
        );
        assert_eq!(ambiguous_issues.len(), 1);
        assert_eq!(ambiguous_issues[0].state, IssueState::Resolved);
    }

    #[tokio::test]
    async fn materialization_creates_canonical_release_rows() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let provider = FakeMusicBrainzProvider::default();
        let service = ReleaseMatchingService::new(repository.clone(), provider);

        service
            .score_and_persist_batch_matches(&batch_id, 140)
            .await
            .expect("candidate scoring should succeed");
        let report = service
            .materialize_batch_matches(&batch_id)
            .await
            .expect("materialization should succeed");

        assert_eq!(report.groups.len(), 1);
        assert_eq!(report.groups[0].release.title, "Kid A");
        assert_eq!(report.groups[0].release_group.title, "Kid A");
        assert_eq!(report.groups[0].artist.name, "Radiohead");
        assert_eq!(
            report.groups[0].release_instance.state,
            ReleaseInstanceState::Matched
        );
        assert_eq!(repository.stored_artists().len(), 1);
        assert_eq!(repository.stored_release_groups().len(), 1);
        assert_eq!(repository.stored_releases().len(), 1);
    }

    #[tokio::test]
    async fn materialization_reuses_existing_release_identity() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let provider = FakeMusicBrainzProvider::default();
        let service = ReleaseMatchingService::new(repository.clone(), provider.clone());

        service
            .score_and_persist_batch_matches(&batch_id, 150)
            .await
            .expect("candidate scoring should succeed");
        let first = service
            .materialize_batch_matches(&batch_id)
            .await
            .expect("first materialization should succeed");
        let second = ReleaseMatchingService::new(repository.clone(), provider)
            .materialize_batch_matches(&batch_id)
            .await
            .expect("second materialization should succeed");

        assert_eq!(first.groups.len(), 1);
        assert!(second.groups.is_empty());
        assert_eq!(repository.stored_releases().len(), 1);
    }

    #[tokio::test]
    async fn manual_release_override_resolves_unmatched_and_survives_rescoring() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let weak_provider =
            FakeMusicBrainzProvider::with_release_candidates(vec![MusicBrainzReleaseCandidate {
                id: "release-weak".to_string(),
                title: "Kid A Tribute".to_string(),
                score: 40,
                artist_names: vec!["Various Artists".to_string()],
                release_group_id: Some("group-weak".to_string()),
                release_group_title: Some("Kid A Tribute".to_string()),
                country: Some("US".to_string()),
                date: Some("2001-01-01".to_string()),
                track_count: Some(14),
            }]);
        let service = ReleaseMatchingService::new(repository.clone(), weak_provider.clone());
        let release = seed_manual_release(&repository);

        let scored = service
            .score_and_persist_batch_matches(&batch_id, 200)
            .await
            .expect("scoring should succeed");
        let release_instance_id = scored.groups[0].release_instance.id.clone();
        let overridden = service
            .apply_manual_release_override(
                &release_instance_id,
                &release.id,
                "operator",
                Some("confirmed manually".to_string()),
                210,
            )
            .expect("manual override should apply");
        assert_eq!(overridden.state, ReleaseInstanceState::Matched);
        assert_eq!(overridden.release_id, Some(release.id.clone()));
        assert!(
            repository
                .stored_manual_overrides()
                .iter()
                .any(|item| item.field == OverrideField::ReleaseMatch)
        );

        let rescored = ReleaseMatchingService::new(repository.clone(), weak_provider)
            .score_and_persist_batch_matches(&batch_id, 220)
            .await
            .expect("rescoring should succeed");
        assert_eq!(
            rescored.groups[0].release_instance.state,
            ReleaseInstanceState::Matched
        );
        assert_eq!(
            rescored.groups[0].release_instance.release_id,
            Some(release.id)
        );
        assert!(
            repository
                .issues_for(
                    IssueSubject::ReleaseInstance(release_instance_id),
                    IssueType::UnmatchedRelease,
                )
                .iter()
                .all(|issue| issue.state != IssueState::Open)
        );
    }

    #[tokio::test]
    async fn manual_release_override_resolves_ambiguous_and_survives_rescoring() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let ambiguous_provider = FakeMusicBrainzProvider::with_release_candidates(vec![
            MusicBrainzReleaseCandidate {
                id: "release-strong".to_string(),
                title: "Kid A".to_string(),
                score: 96,
                artist_names: vec!["Radiohead".to_string()],
                release_group_id: Some("group-1".to_string()),
                release_group_title: Some("Kid A".to_string()),
                country: Some("GB".to_string()),
                date: Some("2000-10-02".to_string()),
                track_count: Some(1),
            },
            MusicBrainzReleaseCandidate {
                id: "release-near".to_string(),
                title: "Kid A".to_string(),
                score: 94,
                artist_names: vec!["Radiohead".to_string()],
                release_group_id: Some("group-2".to_string()),
                release_group_title: Some("Kid A".to_string()),
                country: Some("GB".to_string()),
                date: Some("2000-10-03".to_string()),
                track_count: Some(1),
            },
        ]);
        let service = ReleaseMatchingService::new(repository.clone(), ambiguous_provider.clone());
        let release = seed_manual_release(&repository);

        let scored = service
            .score_and_persist_batch_matches(&batch_id, 230)
            .await
            .expect("scoring should succeed");
        let release_instance_id = scored.groups[0].release_instance.id.clone();
        service
            .apply_manual_release_override(&release_instance_id, &release.id, "operator", None, 240)
            .expect("manual override should apply");

        let rescored = ReleaseMatchingService::new(repository.clone(), ambiguous_provider)
            .score_and_persist_batch_matches(&batch_id, 250)
            .await
            .expect("rescoring should succeed");
        assert_eq!(
            rescored.groups[0].release_instance.state,
            ReleaseInstanceState::Matched
        );
        assert_eq!(
            rescored.groups[0].release_instance.release_id,
            Some(release.id)
        );
        assert!(
            repository
                .issues_for(
                    IssueSubject::ReleaseInstance(release_instance_id),
                    IssueType::AmbiguousReleaseMatch,
                )
                .iter()
                .all(|issue| issue.state != IssueState::Open)
        );
    }

    #[tokio::test]
    async fn discogs_enrichment_persists_payloads_without_overriding_identity() {
        let batch_id = ImportBatchId::new();
        let repository = InMemoryMatchingRepository::with_batch(batch_id.clone());
        let provider = FakeMusicBrainzProvider::default();
        let service = ReleaseMatchingService::new(repository.clone(), provider);
        let scored = service
            .score_and_persist_batch_matches(&batch_id, 50)
            .await
            .expect("candidate scoring should succeed");

        let report = service
            .enrich_release_instance_with_discogs(&scored.groups[0].release_instance.id, 60)
            .await
            .expect("discogs enrichment should succeed");

        assert_eq!(report.release_instance.release_id, None);
        assert_eq!(
            report.persisted_candidates[0].provider,
            CandidateProvider::Discogs
        );
        assert_eq!(
            report.metadata_snapshot.source,
            MetadataSnapshotSource::DiscogsPayload
        );
        assert!(
            repository
                .stored_metadata_snapshots(&scored.groups[0].release_instance.id)
                .iter()
                .any(|snapshot| snapshot.source == MetadataSnapshotSource::DiscogsPayload)
        );
    }

    #[derive(Clone)]
    struct FakeMusicBrainzProvider {
        queries: Arc<Mutex<Vec<String>>>,
        release_candidates: Arc<Vec<MusicBrainzReleaseCandidate>>,
    }

    impl Default for FakeMusicBrainzProvider {
        fn default() -> Self {
            Self {
                queries: Arc::new(Mutex::new(Vec::new())),
                release_candidates: Arc::new(vec![MusicBrainzReleaseCandidate {
                    id: "release-1".to_string(),
                    title: "Kid A".to_string(),
                    score: 100,
                    artist_names: vec!["Radiohead".to_string()],
                    release_group_id: Some("group-1".to_string()),
                    release_group_title: Some("Kid A".to_string()),
                    country: Some("GB".to_string()),
                    date: Some("2000-10-02".to_string()),
                    track_count: Some(10),
                }]),
            }
        }
    }

    impl FakeMusicBrainzProvider {
        fn with_release_candidates(release_candidates: Vec<MusicBrainzReleaseCandidate>) -> Self {
            Self {
                release_candidates: Arc::new(release_candidates),
                ..Self::default()
            }
        }

        fn queries(&self) -> Vec<String> {
            self.queries.lock().expect("queries should lock").clone()
        }
    }

    impl MusicBrainzMetadataProvider for FakeMusicBrainzProvider {
        fn search_releases(
            &self,
            query: &str,
            _limit: u8,
        ) -> impl Future<Output = Result<Vec<MusicBrainzReleaseCandidate>, String>> + Send {
            self.queries
                .lock()
                .expect("queries should lock")
                .push(query.to_string());
            let items = (*self.release_candidates).clone();
            async move { Ok(items) }
        }

        fn search_release_groups(
            &self,
            query: &str,
            _limit: u8,
        ) -> impl Future<Output = Result<Vec<MusicBrainzReleaseGroupCandidate>, String>> + Send
        {
            self.queries
                .lock()
                .expect("queries should lock")
                .push(query.to_string());
            let items = vec![MusicBrainzReleaseGroupCandidate {
                id: "group-1".to_string(),
                title: "Kid A".to_string(),
                score: 97,
                artist_names: vec!["Radiohead".to_string()],
                primary_type: Some("Album".to_string()),
                first_release_date: Some("2000".to_string()),
            }];
            async move { Ok(items) }
        }

        fn lookup_release(
            &self,
            release_id: &str,
        ) -> impl Future<Output = Result<MusicBrainzReleaseDetail, String>> + Send {
            let detail = MusicBrainzReleaseDetail {
                id: release_id.to_string(),
                title: "Kid A".to_string(),
                country: Some("GB".to_string()),
                date: Some("2000-10-02".to_string()),
                artist_credit: vec![MusicBrainzArtistCredit {
                    artist_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string(),
                    artist_name: "Radiohead".to_string(),
                    artist_sort_name: "Radiohead".to_string(),
                }],
                release_group: Some(MusicBrainzReleaseGroupRef {
                    id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb".to_string(),
                    title: "Kid A".to_string(),
                    primary_type: Some("Album".to_string()),
                }),
                label_info: vec![MusicBrainzLabelInfo {
                    catalog_number: Some("XLLP782".to_string()),
                    label_name: Some("XL Recordings".to_string()),
                }],
            };
            async move { Ok(detail) }
        }
    }

    impl DiscogsMetadataProvider for FakeMusicBrainzProvider {
        fn search_releases(
            &self,
            _query: &DiscogsReleaseQuery,
            _limit: u8,
        ) -> impl Future<Output = Result<Vec<DiscogsReleaseCandidate>, String>> + Send {
            let items = vec![DiscogsReleaseCandidate {
                id: "discogs-1".to_string(),
                title: "Kid A".to_string(),
                artist: Some("Radiohead".to_string()),
                year: Some("2000".to_string()),
                country: Some("UK".to_string()),
                label: Some("XL Recordings".to_string()),
                catalog_number: Some("XLLP782".to_string()),
                format_descriptors: vec!["CD".to_string(), "Album".to_string()],
                raw_payload: "{\"id\":1}".to_string(),
            }];
            async move { Ok(items) }
        }
    }

    #[derive(Clone)]
    struct InMemoryMatchingRepository {
        batch: Arc<ImportBatch>,
        source: Arc<Source>,
        manifests: Arc<Vec<StagingManifest>>,
        evidence: Arc<Vec<IngestEvidenceRecord>>,
        artists: Arc<Mutex<Vec<Artist>>>,
        release_groups: Arc<Mutex<Vec<ReleaseGroup>>>,
        releases: Arc<Mutex<Vec<Release>>>,
        release_instances: Arc<Mutex<Vec<ReleaseInstance>>>,
        candidate_matches: Arc<Mutex<HashMap<ReleaseInstanceId, Vec<CandidateMatch>>>>,
        metadata_snapshots: Arc<Mutex<Vec<MetadataSnapshot>>>,
        issues: Arc<Mutex<Vec<Issue>>>,
        manual_overrides: Arc<Mutex<Vec<ManualOverride>>>,
    }

    impl InMemoryMatchingRepository {
        fn with_batch(batch_id: ImportBatchId) -> Self {
            let source = Source {
                id: SourceId::new(),
                kind: SourceKind::ManualAdd,
                display_name: "manual".to_string(),
                locator: SourceLocator::ManualEntry {
                    submitted_path: PathBuf::from("/incoming/Kid A"),
                },
                external_reference: None,
            };
            let batch = ImportBatch {
                id: batch_id.clone(),
                source_id: source.id.clone(),
                mode: ImportMode::Copy,
                status: ImportBatchStatus::Grouped,
                requested_by: BatchRequester::Operator {
                    name: "operator".to_string(),
                },
                created_at_unix_seconds: 1,
                received_paths: vec![PathBuf::from("/incoming/Kid A")],
            };
            let group = StagedReleaseGroup {
                key: "kid-a".to_string(),
                file_paths: vec![PathBuf::from("/incoming/Kid A/01 Everything.mp3")],
                auxiliary_paths: vec![PathBuf::from("/incoming/Kid A/release.yaml")],
            };
            let manifest = StagingManifest {
                id: StagingManifestId::new(),
                batch_id: batch_id.clone(),
                source: StagingManifestSource {
                    kind: source.kind.clone(),
                    source_path: PathBuf::from("/incoming/Kid A"),
                },
                discovered_files: Vec::new(),
                auxiliary_files: vec![AuxiliaryFile {
                    path: PathBuf::from("/incoming/Kid A/release.yaml"),
                    role: crate::domain::staging_manifest::AuxiliaryFileRole::GazelleYaml,
                }],
                grouping: GroupingDecision {
                    strategy: GroupingStrategy::CommonParentDirectory,
                    groups: vec![group.clone()],
                    notes: Vec::new(),
                },
                captured_at_unix_seconds: 1,
            };
            let evidence = vec![
                embedded_record(
                    &batch_id,
                    &group.file_paths[0],
                    vec![
                        observed(ObservedValueKind::Artist, "Radiohead"),
                        observed(ObservedValueKind::ReleaseTitle, "Kid A"),
                        observed(ObservedValueKind::ReleaseYear, "2000"),
                    ],
                ),
                yaml_record(
                    &batch_id,
                    &group.key,
                    vec![
                        observed(ObservedValueKind::Artist, "Radiohead"),
                        observed(ObservedValueKind::ReleaseTitle, "Kid A"),
                    ],
                ),
            ];
            Self {
                batch: Arc::new(batch),
                source: Arc::new(source),
                manifests: Arc::new(vec![manifest]),
                evidence: Arc::new(evidence),
                artists: Arc::new(Mutex::new(Vec::new())),
                release_groups: Arc::new(Mutex::new(Vec::new())),
                releases: Arc::new(Mutex::new(Vec::new())),
                release_instances: Arc::new(Mutex::new(Vec::new())),
                candidate_matches: Arc::new(Mutex::new(HashMap::new())),
                metadata_snapshots: Arc::new(Mutex::new(Vec::new())),
                issues: Arc::new(Mutex::new(Vec::new())),
                manual_overrides: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn stored_candidates(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Option<Vec<CandidateMatch>> {
            self.candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .get(release_instance_id)
                .cloned()
        }

        fn stored_metadata_snapshots(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Vec<MetadataSnapshot> {
            self.metadata_snapshots
                .lock()
                .expect("metadata snapshots should lock")
                .iter()
                .filter(|snapshot| {
                    matches!(
                        snapshot.subject,
                        MetadataSubject::ReleaseInstance(ref id) if id == release_instance_id
                    )
                })
                .cloned()
                .collect()
        }

        fn issues_for(&self, subject: IssueSubject, issue_type: IssueType) -> Vec<Issue> {
            self.issues
                .lock()
                .expect("issues should lock")
                .iter()
                .filter(|issue| issue.subject == subject && issue.issue_type == issue_type)
                .cloned()
                .collect()
        }

        fn stored_artists(&self) -> Vec<Artist> {
            self.artists.lock().expect("artists should lock").clone()
        }

        fn stored_release_groups(&self) -> Vec<ReleaseGroup> {
            self.release_groups
                .lock()
                .expect("release groups should lock")
                .clone()
        }

        fn stored_releases(&self) -> Vec<Release> {
            self.releases.lock().expect("releases should lock").clone()
        }

        fn stored_manual_overrides(&self) -> Vec<ManualOverride> {
            self.manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .clone()
        }
    }

    impl ImportBatchRepository for InMemoryMatchingRepository {
        fn get_import_batch(
            &self,
            id: &ImportBatchId,
        ) -> Result<Option<ImportBatch>, RepositoryError> {
            Ok((self.batch.id == *id).then(|| (*self.batch).clone()))
        }

        fn list_import_batches(
            &self,
            _query: &crate::application::repository::ImportBatchListQuery,
        ) -> Result<crate::support::pagination::Page<ImportBatch>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }
    }

    impl ReleaseRepository for InMemoryMatchingRepository {
        fn find_artist_by_musicbrainz_id(
            &self,
            musicbrainz_artist_id: &str,
        ) -> Result<Option<Artist>, RepositoryError> {
            Ok(self
                .artists
                .lock()
                .expect("artists should lock")
                .iter()
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
            musicbrainz_release_group_id: &str,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(self
                .release_groups
                .lock()
                .expect("release groups should lock")
                .iter()
                .find(|group| {
                    group
                        .musicbrainz_release_group_id
                        .as_ref()
                        .is_some_and(|id| id.as_uuid().to_string() == musicbrainz_release_group_id)
                })
                .cloned())
        }

        fn get_release(
            &self,
            id: &crate::support::ids::ReleaseId,
        ) -> Result<Option<Release>, RepositoryError> {
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
            musicbrainz_release_id: &str,
        ) -> Result<Option<Release>, RepositoryError> {
            Ok(self
                .releases
                .lock()
                .expect("releases should lock")
                .iter()
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
            _query: &crate::application::repository::ReleaseGroupSearchQuery,
        ) -> Result<Page<ReleaseGroup>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }

        fn list_releases(
            &self,
            _query: &crate::application::repository::ReleaseListQuery,
        ) -> Result<Page<Release>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }

        fn list_tracks_for_release(
            &self,
            _release_id: &crate::support::ids::ReleaseId,
        ) -> Result<Vec<crate::domain::track::Track>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }
    }

    impl ReleaseCommandRepository for InMemoryMatchingRepository {
        fn create_artist(&self, artist: &Artist) -> Result<(), RepositoryError> {
            self.artists
                .lock()
                .expect("artists should lock")
                .push(artist.clone());
            Ok(())
        }

        fn create_release_group(
            &self,
            release_group: &ReleaseGroup,
        ) -> Result<(), RepositoryError> {
            self.release_groups
                .lock()
                .expect("release groups should lock")
                .push(release_group.clone());
            Ok(())
        }

        fn create_release(&self, release: &Release) -> Result<(), RepositoryError> {
            self.releases
                .lock()
                .expect("releases should lock")
                .push(release.clone());
            Ok(())
        }
    }

    impl SourceRepository for InMemoryMatchingRepository {
        fn get_source(&self, id: &SourceId) -> Result<Option<Source>, RepositoryError> {
            Ok((self.source.id == *id).then(|| (*self.source).clone()))
        }

        fn find_source_by_locator(
            &self,
            _locator: &SourceLocator,
        ) -> Result<Option<Source>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }
    }

    impl StagingManifestRepository for InMemoryMatchingRepository {
        fn list_staging_manifests_for_batch(
            &self,
            batch_id: &ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(self
                .manifests
                .iter()
                .filter(|manifest| manifest.batch_id == *batch_id)
                .cloned()
                .collect())
        }
    }

    impl IngestEvidenceRepository for InMemoryMatchingRepository {
        fn list_ingest_evidence_for_batch(
            &self,
            batch_id: &ImportBatchId,
        ) -> Result<Vec<IngestEvidenceRecord>, RepositoryError> {
            Ok(self
                .evidence
                .iter()
                .filter(|record| record.batch_id == *batch_id)
                .cloned()
                .collect())
        }
    }

    impl ReleaseInstanceRepository for InMemoryMatchingRepository {
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
            _query: &crate::application::repository::ReleaseInstanceListQuery,
        ) -> Result<crate::support::pagination::Page<ReleaseInstance>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }

        fn list_release_instances_for_batch(
            &self,
            import_batch_id: &ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .iter()
                .filter(|instance| instance.import_batch_id == *import_batch_id)
                .cloned()
                .collect())
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
                .get(release_instance_id)
                .cloned()
                .unwrap_or_default();
            let total = items.len() as u64;
            let offset = (page.offset as usize).min(items.len());
            let paged = items
                .into_iter()
                .skip(offset)
                .take(page.limit as usize)
                .collect();
            Ok(Page {
                items: paged,
                request: *page,
                total,
            })
        }

        fn get_candidate_match(
            &self,
            _id: &CandidateMatchId,
        ) -> Result<Option<CandidateMatch>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }

        fn list_track_instances_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<crate::domain::track_instance::TrackInstance>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _role: Option<crate::domain::file::FileRole>,
        ) -> Result<Vec<crate::domain::file::FileRecord>, RepositoryError> {
            unimplemented!("not needed in matching tests")
        }
    }

    impl ReleaseInstanceCommandRepository for InMemoryMatchingRepository {
        fn create_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .push(release_instance.clone());
            Ok(())
        }

        fn update_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            let mut instances = self
                .release_instances
                .lock()
                .expect("release instances should lock");
            let existing = instances
                .iter_mut()
                .find(|instance| instance.id == release_instance.id)
                .expect("release instance should exist");
            *existing = release_instance.clone();
            Ok(())
        }

        fn replace_candidate_matches(
            &self,
            release_instance_id: &ReleaseInstanceId,
            matches: &[CandidateMatch],
        ) -> Result<(), RepositoryError> {
            self.candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .insert(release_instance_id.clone(), matches.to_vec());
            Ok(())
        }

        fn replace_candidate_matches_for_provider(
            &self,
            release_instance_id: &ReleaseInstanceId,
            provider: &CandidateProvider,
            matches: &[CandidateMatch],
        ) -> Result<(), RepositoryError> {
            let mut all_matches = self
                .candidate_matches
                .lock()
                .expect("candidate matches should lock");
            let existing = all_matches.entry(release_instance_id.clone()).or_default();
            existing.retain(|candidate| &candidate.provider != provider);
            existing.extend_from_slice(matches);
            existing.sort_by(|left, right| {
                right
                    .normalized_score
                    .value()
                    .total_cmp(&left.normalized_score.value())
            });
            Ok(())
        }

        fn replace_track_instances_and_files(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _track_instances: &[crate::domain::track_instance::TrackInstance],
            _files: &[crate::domain::file::FileRecord],
        ) -> Result<(), RepositoryError> {
            unimplemented!("not needed in matching tests")
        }
    }

    impl MetadataSnapshotCommandRepository for InMemoryMatchingRepository {
        fn create_metadata_snapshots(
            &self,
            snapshots: &[MetadataSnapshot],
        ) -> Result<(), RepositoryError> {
            self.metadata_snapshots
                .lock()
                .expect("metadata snapshots should lock")
                .extend_from_slice(snapshots);
            Ok(())
        }
    }

    impl IssueRepository for InMemoryMatchingRepository {
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
            let mut items = self
                .issues
                .lock()
                .expect("issues should lock")
                .iter()
                .filter(|issue| {
                    query
                        .state
                        .as_ref()
                        .is_none_or(|state| &issue.state == state)
                        && query
                            .issue_type
                            .as_ref()
                            .is_none_or(|issue_type| &issue.issue_type == issue_type)
                        && query
                            .subject
                            .as_ref()
                            .is_none_or(|subject| &issue.subject == subject)
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .created_at_unix_seconds
                    .cmp(&left.created_at_unix_seconds)
            });
            let total = items.len() as u64;
            let offset = (query.page.offset as usize).min(items.len());
            let paged = items
                .into_iter()
                .skip(offset)
                .take(query.page.limit as usize)
                .collect();
            Ok(Page {
                items: paged,
                request: query.page,
                total,
            })
        }
    }

    impl IssueCommandRepository for InMemoryMatchingRepository {
        fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("issues should lock")
                .push(issue.clone());
            Ok(())
        }

        fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            let mut issues = self.issues.lock().expect("issues should lock");
            let existing = issues
                .iter_mut()
                .find(|existing| existing.id == issue.id)
                .expect("issue should exist");
            *existing = issue.clone();
            Ok(())
        }
    }

    impl ManualOverrideRepository for InMemoryMatchingRepository {
        fn get_manual_override(
            &self,
            id: &crate::support::ids::ManualOverrideId,
        ) -> Result<Option<ManualOverride>, RepositoryError> {
            Ok(self
                .manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .iter()
                .find(|override_record| override_record.id == *id)
                .cloned())
        }

        fn list_manual_overrides(
            &self,
            query: &ManualOverrideListQuery,
        ) -> Result<Page<ManualOverride>, RepositoryError> {
            let mut items = self
                .manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .iter()
                .filter(|override_record| {
                    query
                        .subject
                        .as_ref()
                        .is_none_or(|subject| &override_record.subject == subject)
                        && query
                            .field
                            .as_ref()
                            .is_none_or(|field| &override_record.field == field)
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .created_at_unix_seconds
                    .cmp(&left.created_at_unix_seconds)
            });
            let total = items.len() as u64;
            let offset = (query.page.offset as usize).min(items.len());
            let paged = items
                .into_iter()
                .skip(offset)
                .take(query.page.limit as usize)
                .collect();
            Ok(Page {
                items: paged,
                request: query.page,
                total,
            })
        }
    }

    impl ManualOverrideCommandRepository for InMemoryMatchingRepository {
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

    fn observed(kind: ObservedValueKind, value: &str) -> ObservedValue {
        ObservedValue {
            kind,
            value: value.to_string(),
        }
    }

    fn embedded_record(
        batch_id: &ImportBatchId,
        path: &Path,
        observations: Vec<ObservedValue>,
    ) -> IngestEvidenceRecord {
        IngestEvidenceRecord {
            id: IngestEvidenceId::new(),
            batch_id: batch_id.clone(),
            subject: IngestEvidenceSubject::DiscoveredPath(path.to_path_buf()),
            source: IngestEvidenceSource::EmbeddedTags,
            observations,
            structured_payload: None,
            captured_at_unix_seconds: 1,
        }
    }

    fn yaml_record(
        batch_id: &ImportBatchId,
        group_key: &str,
        observations: Vec<ObservedValue>,
    ) -> IngestEvidenceRecord {
        IngestEvidenceRecord {
            id: IngestEvidenceId::new(),
            batch_id: batch_id.clone(),
            subject: IngestEvidenceSubject::GroupedReleaseInput {
                group_key: group_key.to_string(),
            },
            source: IngestEvidenceSource::GazelleYaml,
            observations,
            structured_payload: Some("release_name: Kid A".to_string()),
            captured_at_unix_seconds: 1,
        }
    }

    fn seed_manual_release(repository: &InMemoryMatchingRepository) -> Release {
        let artist = Artist {
            id: crate::support::ids::ArtistId::new(),
            name: "Radiohead".to_string(),
            sort_name: Some("Radiohead".to_string()),
            musicbrainz_artist_id: None,
        };
        repository
            .create_artist(&artist)
            .expect("artist should persist");
        let release_group = ReleaseGroup {
            id: crate::support::ids::ReleaseGroupId::new(),
            primary_artist_id: artist.id.clone(),
            title: "Kid A".to_string(),
            kind: ReleaseGroupKind::Album,
            musicbrainz_release_group_id: None,
        };
        repository
            .create_release_group(&release_group)
            .expect("release group should persist");
        let release = Release {
            id: crate::support::ids::ReleaseId::new(),
            release_group_id: release_group.id,
            primary_artist_id: artist.id,
            title: "Kid A".to_string(),
            musicbrainz_release_id: None,
            discogs_release_id: None,
            edition: ReleaseEdition::default(),
        };
        repository
            .create_release(&release)
            .expect("release should persist");
        release
    }
}
