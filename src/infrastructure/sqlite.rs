use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteInfrastructure {
    pub database_path: PathBuf,
}

impl SqliteInfrastructure {
    pub fn new(database_path: PathBuf) -> Self {
        Self { database_path }
    }
}
