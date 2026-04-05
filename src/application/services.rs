use crate::application::workers::WorkerPools;
use crate::config::WorkerConfig;

#[derive(Debug, Clone)]
pub struct ApplicationContext {
    pub workers: WorkerPools,
}

impl ApplicationContext {
    pub fn new(workers: &WorkerConfig) -> Self {
        Self {
            workers: WorkerPools::from_config(workers),
        }
    }
}

impl Default for ApplicationContext {
    fn default() -> Self {
        Self::new(&WorkerConfig::default())
    }
}
