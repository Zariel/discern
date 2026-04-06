pub mod discogs;
pub mod musicbrainz;
pub mod sqlite;

use crate::application::observability::ObservabilityContext;
use crate::config::{DiscogsConfig, MusicBrainzConfig, StorageConfig};

use self::discogs::DiscogsClient;
use self::musicbrainz::MusicBrainzClient;
use self::sqlite::SqliteInfrastructure;

#[derive(Debug, Clone)]
pub struct Infrastructure {
    pub sqlite: SqliteInfrastructure,
    pub musicbrainz: MusicBrainzClient,
    pub discogs: DiscogsClient,
}

impl Infrastructure {
    pub fn from_config(
        storage: &StorageConfig,
        musicbrainz: &MusicBrainzConfig,
        discogs: &DiscogsConfig,
    ) -> Self {
        Self::from_config_with_observability(storage, musicbrainz, discogs, None)
    }

    pub fn from_config_with_observability(
        storage: &StorageConfig,
        musicbrainz: &MusicBrainzConfig,
        discogs: &DiscogsConfig,
        observability: Option<ObservabilityContext>,
    ) -> Self {
        Self {
            sqlite: SqliteInfrastructure::new(storage.sqlite_path.clone()),
            musicbrainz: MusicBrainzClient::from_config_with_observability(
                musicbrainz,
                observability.clone(),
            ),
            discogs: DiscogsClient::from_config_with_observability(discogs, observability),
        }
    }
}
