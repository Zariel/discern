use crate::application::repository::{
    ManualOverrideCommandRepository, ReleaseInstanceRepository, ReleaseRepository, RepositoryError,
    RepositoryErrorKind,
};
use crate::domain::manual_override::{ManualOverride, OverrideField, OverrideSubject};
use crate::support::ids::{ReleaseId, ReleaseInstanceId, TrackInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualMetadataServiceError {
    pub kind: ManualMetadataServiceErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualMetadataServiceErrorKind {
    NotFound,
    Conflict,
    Storage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverrideInput {
    pub field: OverrideField,
    pub value: String,
}

pub struct ManualMetadataService<R> {
    repository: R,
}

impl<R> ManualMetadataService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> ManualMetadataService<R>
where
    R: ManualOverrideCommandRepository + ReleaseRepository + ReleaseInstanceRepository,
{
    pub fn apply_release_overrides(
        &self,
        release_id: &ReleaseId,
        overrides: Vec<OverrideInput>,
        created_by: &str,
        note: Option<String>,
        created_at_unix_seconds: i64,
    ) -> Result<Vec<ManualOverride>, ManualMetadataServiceError> {
        self.repository
            .get_release(release_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| ManualMetadataServiceError {
                kind: ManualMetadataServiceErrorKind::NotFound,
                message: format!("release {} was not found", release_id.as_uuid()),
            })?;
        validate_overrides(
            &overrides,
            &[
                OverrideField::Title,
                OverrideField::AlbumArtist,
                OverrideField::ArtistCredit,
                OverrideField::ReleaseDate,
                OverrideField::EditionQualifier,
            ],
        )?;
        create_overrides(
            &self.repository,
            OverrideSubject::Release(release_id.clone()),
            overrides,
            created_by,
            note,
            created_at_unix_seconds,
        )
    }

    pub fn apply_release_instance_overrides(
        &self,
        release_instance_id: &ReleaseInstanceId,
        overrides: Vec<OverrideInput>,
        created_by: &str,
        note: Option<String>,
        created_at_unix_seconds: i64,
    ) -> Result<Vec<ManualOverride>, ManualMetadataServiceError> {
        self.repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| ManualMetadataServiceError {
                kind: ManualMetadataServiceErrorKind::NotFound,
                message: format!(
                    "release instance {} was not found",
                    release_instance_id.as_uuid()
                ),
            })?;
        validate_overrides(&overrides, &[OverrideField::ArtworkSelection])?;
        create_overrides(
            &self.repository,
            OverrideSubject::ReleaseInstance(release_instance_id.clone()),
            overrides,
            created_by,
            note,
            created_at_unix_seconds,
        )
    }

    pub fn apply_track_overrides(
        &self,
        release_instance_id: &ReleaseInstanceId,
        track_instance_id: &TrackInstanceId,
        overrides: Vec<OverrideInput>,
        created_by: &str,
        note: Option<String>,
        created_at_unix_seconds: i64,
    ) -> Result<Vec<ManualOverride>, ManualMetadataServiceError> {
        let track = self
            .repository
            .list_track_instances_for_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .iter()
            .find(|track| track.id == *track_instance_id)
            .cloned();
        let Some(track) = track else {
            return Err(ManualMetadataServiceError {
                kind: ManualMetadataServiceErrorKind::NotFound,
                message: format!(
                    "track instance {} was not found for release instance {}",
                    track_instance_id.as_uuid(),
                    release_instance_id.as_uuid()
                ),
            });
        };
        validate_overrides(&overrides, &[OverrideField::TrackTitle])?;
        create_overrides(
            &self.repository,
            OverrideSubject::Track(track.track_id),
            overrides,
            created_by,
            note,
            created_at_unix_seconds,
        )
    }
}

fn create_overrides<R>(
    repository: &R,
    subject: OverrideSubject,
    overrides: Vec<OverrideInput>,
    created_by: &str,
    note: Option<String>,
    created_at_unix_seconds: i64,
) -> Result<Vec<ManualOverride>, ManualMetadataServiceError>
where
    R: ManualOverrideCommandRepository,
{
    if overrides.is_empty() {
        return Err(ManualMetadataServiceError {
            kind: ManualMetadataServiceErrorKind::Conflict,
            message: "at least one override value is required".to_string(),
        });
    }

    let mut created = Vec::with_capacity(overrides.len());
    for override_input in overrides {
        let record = ManualOverride {
            id: crate::support::ids::ManualOverrideId::new(),
            subject: subject.clone(),
            field: override_input.field,
            value: override_input.value,
            note: note.clone(),
            created_by: created_by.to_string(),
            created_at_unix_seconds,
        };
        repository
            .create_manual_override(&record)
            .map_err(map_repository_error)?;
        created.push(record);
    }
    Ok(created)
}

fn validate_overrides(
    overrides: &[OverrideInput],
    allowed: &[OverrideField],
) -> Result<(), ManualMetadataServiceError> {
    if overrides.is_empty() {
        return Err(ManualMetadataServiceError {
            kind: ManualMetadataServiceErrorKind::Conflict,
            message: "at least one override value is required".to_string(),
        });
    }

    for item in overrides {
        if !allowed.iter().any(|field| field == &item.field) {
            return Err(ManualMetadataServiceError {
                kind: ManualMetadataServiceErrorKind::Conflict,
                message: format!(
                    "override field {:?} is not allowed for this subject",
                    item.field
                ),
            });
        }
    }

    Ok(())
}

fn map_repository_error(error: RepositoryError) -> ManualMetadataServiceError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => ManualMetadataServiceErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            ManualMetadataServiceErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => ManualMetadataServiceErrorKind::Storage,
    };
    ManualMetadataServiceError {
        kind,
        message: error.message,
    }
}
