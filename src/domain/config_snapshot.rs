use crate::support::ids::{ConfigSnapshotId, ReleaseInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSnapshot {
    pub id: ConfigSnapshotId,
    pub release_instance_id: Option<ReleaseInstanceId>,
    pub fingerprint: String,
    pub content: String,
    pub captured_at_unix_seconds: i64,
}
