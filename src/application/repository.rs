use crate::domain::candidate_match::CandidateMatch;
use crate::domain::exported_metadata_snapshot::ExportedMetadataSnapshot;
use crate::domain::import_batch::ImportBatch;
use crate::domain::ingest_evidence::IngestEvidenceRecord;
use crate::domain::issue::{Issue, IssueState, IssueType};
use crate::domain::job::{Job, JobStatus, JobType};
use crate::domain::metadata_snapshot::MetadataSnapshot;
use crate::domain::release::Release;
use crate::domain::release_group::ReleaseGroup;
use crate::domain::release_instance::{FormatFamily, ReleaseInstance, ReleaseInstanceState};
use crate::domain::source::{Source, SourceLocator};
use crate::domain::staging_manifest::StagingManifest;
use crate::support::ids::{
    CandidateMatchId, ExportedMetadataSnapshotId, ImportBatchId, IssueId, JobId, ReleaseGroupId,
    ReleaseId, ReleaseInstanceId,
};
use crate::support::pagination::{Page, PageRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryError {
    pub kind: RepositoryErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepositoryErrorKind {
    NotFound,
    Conflict,
    InvalidQuery,
    Storage,
}

pub trait ReleaseRepository {
    fn get_release_group(
        &self,
        id: &ReleaseGroupId,
    ) -> Result<Option<ReleaseGroup>, RepositoryError>;

    fn get_release(&self, id: &ReleaseId) -> Result<Option<Release>, RepositoryError>;

    fn find_release_by_musicbrainz_id(
        &self,
        musicbrainz_release_id: &str,
    ) -> Result<Option<Release>, RepositoryError>;

    fn search_release_groups(
        &self,
        query: &ReleaseGroupSearchQuery,
    ) -> Result<Page<ReleaseGroup>, RepositoryError>;

    fn list_releases(&self, query: &ReleaseListQuery) -> Result<Page<Release>, RepositoryError>;
}

pub trait ReleaseInstanceRepository {
    fn get_release_instance(
        &self,
        id: &ReleaseInstanceId,
    ) -> Result<Option<ReleaseInstance>, RepositoryError>;

    fn list_release_instances(
        &self,
        query: &ReleaseInstanceListQuery,
    ) -> Result<Page<ReleaseInstance>, RepositoryError>;

    fn list_candidate_matches(
        &self,
        release_instance_id: &ReleaseInstanceId,
        page: &PageRequest,
    ) -> Result<Page<CandidateMatch>, RepositoryError>;

    fn get_candidate_match(
        &self,
        id: &CandidateMatchId,
    ) -> Result<Option<CandidateMatch>, RepositoryError>;
}

pub trait ImportBatchRepository {
    fn get_import_batch(&self, id: &ImportBatchId) -> Result<Option<ImportBatch>, RepositoryError>;

    fn list_import_batches(
        &self,
        query: &ImportBatchListQuery,
    ) -> Result<Page<ImportBatch>, RepositoryError>;
}

pub trait SourceRepository {
    fn get_source(
        &self,
        id: &crate::support::ids::SourceId,
    ) -> Result<Option<Source>, RepositoryError>;

    fn find_source_by_locator(
        &self,
        locator: &SourceLocator,
    ) -> Result<Option<Source>, RepositoryError>;
}

pub trait SourceCommandRepository {
    fn create_source(&self, source: &Source) -> Result<(), RepositoryError>;
}

pub trait ImportBatchCommandRepository {
    fn create_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError>;

    fn update_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError>;

    fn list_active_import_batches_for_source(
        &self,
        source_id: &crate::support::ids::SourceId,
    ) -> Result<Vec<ImportBatch>, RepositoryError>;
}

pub trait StagingManifestRepository {
    fn list_staging_manifests_for_batch(
        &self,
        batch_id: &ImportBatchId,
    ) -> Result<Vec<StagingManifest>, RepositoryError>;
}

pub trait StagingManifestCommandRepository {
    fn create_staging_manifest(&self, manifest: &StagingManifest) -> Result<(), RepositoryError>;
}

pub trait IngestEvidenceRepository {
    fn list_ingest_evidence_for_batch(
        &self,
        batch_id: &ImportBatchId,
    ) -> Result<Vec<IngestEvidenceRecord>, RepositoryError>;
}

pub trait IngestEvidenceCommandRepository {
    fn create_ingest_evidence_records(
        &self,
        records: &[IngestEvidenceRecord],
    ) -> Result<(), RepositoryError>;
}

pub trait MetadataSnapshotRepository {
    fn list_metadata_snapshots_for_batch(
        &self,
        batch_id: &ImportBatchId,
    ) -> Result<Vec<MetadataSnapshot>, RepositoryError>;
}

pub trait MetadataSnapshotCommandRepository {
    fn create_metadata_snapshots(
        &self,
        snapshots: &[MetadataSnapshot],
    ) -> Result<(), RepositoryError>;
}

pub trait IssueRepository {
    fn get_issue(&self, id: &IssueId) -> Result<Option<Issue>, RepositoryError>;

    fn list_issues(&self, query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError>;
}

pub trait IssueCommandRepository {
    fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError>;

    fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError>;
}

pub trait JobRepository {
    fn get_job(&self, id: &JobId) -> Result<Option<Job>, RepositoryError>;

    fn list_jobs(&self, query: &JobListQuery) -> Result<Page<Job>, RepositoryError>;
}

pub trait JobCommandRepository {
    fn create_job(&self, job: &Job) -> Result<(), RepositoryError>;

    fn update_job(&self, job: &Job) -> Result<(), RepositoryError>;

    fn list_recoverable_jobs(&self) -> Result<Vec<Job>, RepositoryError>;
}

pub trait ExportRepository {
    fn get_latest_exported_metadata(
        &self,
        release_instance_id: &ReleaseInstanceId,
    ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError>;

    fn list_exported_metadata(
        &self,
        query: &ExportedMetadataListQuery,
    ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError>;

    fn get_exported_metadata(
        &self,
        id: &ExportedMetadataSnapshotId,
    ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReleaseGroupSearchQuery {
    pub text: Option<String>,
    pub primary_artist_name: Option<String>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReleaseListQuery {
    pub release_group_id: Option<ReleaseGroupId>,
    pub text: Option<String>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReleaseInstanceListQuery {
    pub release_id: Option<ReleaseId>,
    pub state: Option<ReleaseInstanceState>,
    pub format_family: Option<FormatFamily>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ImportBatchListQuery {
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IssueListQuery {
    pub state: Option<IssueState>,
    pub issue_type: Option<IssueType>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobListQuery {
    pub status: Option<JobStatus>,
    pub job_type: Option<JobType>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExportedMetadataListQuery {
    pub release_instance_id: Option<ReleaseInstanceId>,
    pub album_title: Option<String>,
    pub page: PageRequest,
}
