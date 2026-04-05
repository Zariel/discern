pub mod musicbrainz;
pub mod sqlite;

use crate::config::{MusicBrainzConfig, StorageConfig};

use self::musicbrainz::MusicBrainzClient;
use self::sqlite::SqliteInfrastructure;

#[derive(Debug, Clone)]
pub struct Infrastructure {
    pub sqlite: SqliteInfrastructure,
    pub musicbrainz: MusicBrainzClient,
}

impl Infrastructure {
    pub fn from_config(storage: &StorageConfig, musicbrainz: &MusicBrainzConfig) -> Self {
        Self {
            sqlite: SqliteInfrastructure::new(storage.sqlite_path.clone()),
            musicbrainz: MusicBrainzClient::from_config(musicbrainz),
        }
    }
}
