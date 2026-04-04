use uuid::Uuid;

macro_rules! strong_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(Uuid);

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }
    };
}

strong_id!(ArtistId);
strong_id!(ReleaseGroupId);
strong_id!(ReleaseId);
strong_id!(ReleaseInstanceId);
strong_id!(TrackId);
strong_id!(TrackInstanceId);
strong_id!(FileId);
strong_id!(SourceId);
strong_id!(ImportBatchId);
strong_id!(MetadataSnapshotId);
strong_id!(ExportedMetadataSnapshotId);
strong_id!(IssueId);
strong_id!(JobId);
strong_id!(ManualOverrideId);
strong_id!(ReleaseArtworkId);
strong_id!(ConfigSnapshotId);

strong_id!(MusicBrainzArtistId);
strong_id!(MusicBrainzReleaseGroupId);
strong_id!(MusicBrainzReleaseId);
strong_id!(MusicBrainzTrackId);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiscogsReleaseId(u64);

impl DiscogsReleaseId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}
