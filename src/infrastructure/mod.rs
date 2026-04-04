pub mod sqlite;

use crate::config::StorageConfig;

use self::sqlite::SqliteInfrastructure;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Infrastructure {
    pub sqlite: SqliteInfrastructure,
}

impl Infrastructure {
    pub fn from_config(config: &StorageConfig) -> Self {
        Self {
            sqlite: SqliteInfrastructure::new(config.sqlite_path.clone()),
        }
    }
}
