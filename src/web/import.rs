use crate::api::ingest::{
    CreateImportBatchFromPathRequest, ImportBatchResource, ImportSubmissionResource, IngestApi,
    ListImportBatchesRequest, WatchRescanResource,
};
use crate::application::config::ValidatedRuntimeConfig;
use crate::application::repository::{
    ImportBatchCommandRepository, ImportBatchRepository, JobCommandRepository, JobRepository,
    SourceCommandRepository, SourceRepository,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ManualImportBatchesFilters {
    pub limit: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualImportScreen {
    pub filters: ManualImportBatchesFilters,
    pub recent_batches: Vec<ImportBatchResource>,
    pub total_batches: u64,
}

pub struct ManualImportScreenLoader<R> {
    repository: R,
    config: ValidatedRuntimeConfig,
}

impl<R> ManualImportScreenLoader<R> {
    pub fn new(repository: R, config: ValidatedRuntimeConfig) -> Self {
        Self { repository, config }
    }
}

impl<R> ManualImportScreenLoader<R>
where
    R: Clone
        + SourceRepository
        + SourceCommandRepository
        + ImportBatchCommandRepository
        + ImportBatchRepository
        + JobCommandRepository
        + JobRepository,
{
    pub fn load(
        &self,
        request_id: impl Into<String>,
        filters: ManualImportBatchesFilters,
    ) -> Result<ManualImportScreen, String> {
        let envelope = IngestApi::new(self.repository.clone(), self.config.clone())
            .list_import_batches(
                request_id,
                ListImportBatchesRequest {
                    limit: normalize_limit(filters.limit),
                    offset: filters.offset,
                },
            )
            .map_err(|envelope| error_message(*envelope))?;
        let recent_batches = envelope.data.unwrap_or_default();
        Ok(ManualImportScreen {
            filters,
            total_batches: envelope
                .meta
                .pagination
                .map(|value| value.total)
                .unwrap_or(recent_batches.len() as u64),
            recent_batches,
        })
    }

    pub fn submit_manual_path(
        &self,
        request_id: impl Into<String>,
        request: CreateImportBatchFromPathRequest,
    ) -> Result<ImportSubmissionResource, String> {
        IngestApi::new(self.repository.clone(), self.config.clone())
            .create_import_batch_from_path(request_id, request)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "manual import submission response was empty".to_string())
    }

    pub fn rescan_watcher(
        &self,
        request_id: impl Into<String>,
        request: crate::api::ingest::RescanWatcherRequest,
    ) -> Result<WatchRescanResource, String>
    where
        R: crate::application::repository::IssueCommandRepository
            + crate::application::repository::IssueRepository
            + crate::application::repository::ReleaseInstanceCommandRepository
            + crate::application::repository::ReleaseInstanceRepository
            + crate::application::repository::StagingManifestCommandRepository
            + crate::application::repository::StagingManifestRepository,
    {
        IngestApi::new(self.repository.clone(), self.config.clone())
            .rescan_watcher(request_id, request)
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "watcher rescan response was empty".to_string())
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
