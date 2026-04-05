use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::WorkerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerPoolKind {
    FileIo,
    ProviderRequests,
    DbWrites,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPoolLimits {
    pub file_io: usize,
    pub provider_requests: usize,
    pub db_writes: usize,
}

impl From<&WorkerConfig> for WorkerPoolLimits {
    fn from(config: &WorkerConfig) -> Self {
        Self {
            file_io: config.file_io_concurrency,
            provider_requests: config.provider_request_concurrency,
            db_writes: config.db_write_concurrency,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerPool {
    kind: WorkerPoolKind,
    semaphore: Arc<Semaphore>,
    limit: usize,
}

impl WorkerPool {
    pub fn new(kind: WorkerPoolKind, limit: usize) -> Self {
        Self {
            kind,
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    pub fn kind(&self) -> WorkerPoolKind {
        self.kind
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub async fn acquire(&self) -> WorkerPermit {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("worker semaphore should not close");
        WorkerPermit {
            kind: self.kind,
            permit,
        }
    }

    pub fn try_acquire(&self) -> Option<WorkerPermit> {
        let permit = self.semaphore.clone().try_acquire_owned().ok()?;
        Some(WorkerPermit {
            kind: self.kind,
            permit,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WorkerPools {
    pub file_io: WorkerPool,
    pub provider_requests: WorkerPool,
    pub db_writes: WorkerPool,
}

impl WorkerPools {
    pub fn from_config(config: &WorkerConfig) -> Self {
        Self {
            file_io: WorkerPool::new(WorkerPoolKind::FileIo, config.file_io_concurrency),
            provider_requests: WorkerPool::new(
                WorkerPoolKind::ProviderRequests,
                config.provider_request_concurrency,
            ),
            db_writes: WorkerPool::new(WorkerPoolKind::DbWrites, config.db_write_concurrency),
        }
    }

    pub fn limits(&self) -> WorkerPoolLimits {
        WorkerPoolLimits {
            file_io: self.file_io.limit(),
            provider_requests: self.provider_requests.limit(),
            db_writes: self.db_writes.limit(),
        }
    }

    pub fn pool(&self, kind: WorkerPoolKind) -> &WorkerPool {
        match kind {
            WorkerPoolKind::FileIo => &self.file_io,
            WorkerPoolKind::ProviderRequests => &self.provider_requests,
            WorkerPoolKind::DbWrites => &self.db_writes,
        }
    }
}

#[derive(Debug)]
pub struct WorkerPermit {
    kind: WorkerPoolKind,
    permit: OwnedSemaphorePermit,
}

impl WorkerPermit {
    pub fn kind(&self) -> WorkerPoolKind {
        self.kind
    }

    pub fn permit_count(&self) -> usize {
        self.permit.num_permits()
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, timeout};

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn worker_pools_apply_independent_limits() {
        let config = WorkerConfig {
            max_concurrent_jobs: 4,
            file_io_concurrency: 2,
            provider_request_concurrency: 3,
            db_write_concurrency: 1,
        };

        let pools = WorkerPools::from_config(&config);
        let file_io_a = pools.file_io.acquire().await;
        let file_io_b = pools.file_io.acquire().await;
        let provider_a = pools.provider_requests.acquire().await;
        let db_write = pools.db_writes.acquire().await;

        assert_eq!(file_io_a.kind(), WorkerPoolKind::FileIo);
        assert_eq!(provider_a.kind(), WorkerPoolKind::ProviderRequests);
        assert_eq!(db_write.kind(), WorkerPoolKind::DbWrites);
        assert_eq!(db_write.permit_count(), 1);
        assert_eq!(pools.file_io.available_permits(), 0);
        assert_eq!(pools.provider_requests.available_permits(), 2);
        assert_eq!(pools.db_writes.available_permits(), 0);
        assert!(pools.file_io.try_acquire().is_none());
        assert!(pools.db_writes.try_acquire().is_none());

        drop(file_io_a);
        drop(file_io_b);

        let reacquired = timeout(Duration::from_millis(20), pools.file_io.acquire())
            .await
            .expect("file I/O permit should become available");
        assert_eq!(reacquired.kind(), WorkerPoolKind::FileIo);
    }
}
