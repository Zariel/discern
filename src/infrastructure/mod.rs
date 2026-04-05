pub mod discogs;
pub mod musicbrainz;
pub mod sqlite;

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
        Self {
            sqlite: SqliteInfrastructure::new(storage.sqlite_path.clone()),
            musicbrainz: MusicBrainzClient::from_config(musicbrainz),
            discogs: DiscogsClient::from_config(discogs),
        }
    }
}
