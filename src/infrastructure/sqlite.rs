#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteInfrastructure {
    pub database_path: String,
}

impl SqliteInfrastructure {
    pub fn new(database_path: String) -> Self {
        Self { database_path }
    }
}
