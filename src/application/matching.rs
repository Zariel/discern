use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::PathBuf;

use crate::application::repository::{
    IngestEvidenceRepository, RepositoryError, RepositoryErrorKind, StagingManifestRepository,
};
use crate::domain::ingest_evidence::{
    IngestEvidenceRecord, IngestEvidenceSubject, ObservedValueKind,
};
use crate::domain::staging_manifest::{StagedReleaseGroup, StagingManifest};
use crate::support::ids::ImportBatchId;

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
    pub track_count: usize,
    pub disc_count: Option<usize>,
    pub directory_hint: Option<String>,
    pub filename_hints: Vec<String>,
    pub source_descriptors: Vec<String>,
    pub tracker_identifiers: Vec<String>,
    pub evidence_conflicts: Vec<String>,
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
            let release_candidates = self
                .provider
                .search_releases(&release_query, 10)
                .await
                .map_err(map_provider_error)?;
            let release_group_candidates = self
                .provider
                .search_release_groups(&release_group_query, 10)
                .await
                .map_err(map_provider_error)?;
            groups.push(GroupMatchProbe {
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
    let mut source_descriptors = BTreeSet::new();
    let mut tracker_identifiers = BTreeSet::new();
    if let Some(records) = evidence_by_group.get(&group.key) {
        for record in records {
            for observation in &record.observations {
                match observation.kind {
                    ObservedValueKind::Artist => yaml_artists.push(observation.value.clone()),
                    ObservedValueKind::ReleaseTitle => yaml_titles.push(observation.value.clone()),
                    ObservedValueKind::ReleaseYear => yaml_years.push(observation.value.clone()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use crate::domain::ingest_evidence::{
        IngestEvidenceRecord, IngestEvidenceSource, IngestEvidenceSubject, ObservedValue,
    };
    use crate::domain::source::SourceKind;
    use crate::domain::staging_manifest::{
        AuxiliaryFile, GroupingDecision, GroupingStrategy, StagingManifestSource,
    };
    use crate::support::ids::{ImportBatchId, IngestEvidenceId, StagingManifestId};

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

    #[derive(Clone)]
    struct FakeMusicBrainzProvider {
        queries: Arc<Mutex<Vec<String>>>,
    }

    impl Default for FakeMusicBrainzProvider {
        fn default() -> Self {
            Self {
                queries: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl FakeMusicBrainzProvider {
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
            let items = vec![MusicBrainzReleaseCandidate {
                id: "release-1".to_string(),
                title: "Kid A".to_string(),
                score: 100,
                artist_names: vec!["Radiohead".to_string()],
                release_group_id: Some("group-1".to_string()),
                release_group_title: Some("Kid A".to_string()),
                country: Some("GB".to_string()),
                date: Some("2000-10-02".to_string()),
                track_count: Some(10),
            }];
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
    }

    #[derive(Clone)]
    struct InMemoryMatchingRepository {
        manifests: Arc<Vec<StagingManifest>>,
        evidence: Arc<Vec<IngestEvidenceRecord>>,
    }

    impl InMemoryMatchingRepository {
        fn with_batch(batch_id: ImportBatchId) -> Self {
            let group = StagedReleaseGroup {
                key: "kid-a".to_string(),
                file_paths: vec![PathBuf::from("/incoming/Kid A/01 Everything.mp3")],
                auxiliary_paths: vec![PathBuf::from("/incoming/Kid A/release.yaml")],
            };
            let manifest = StagingManifest {
                id: StagingManifestId::new(),
                batch_id: batch_id.clone(),
                source: StagingManifestSource {
                    kind: SourceKind::ManualAdd,
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
                manifests: Arc::new(vec![manifest]),
                evidence: Arc::new(evidence),
            }
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
}
