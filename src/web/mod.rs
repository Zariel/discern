pub mod client;
pub mod inspect;
pub mod operate;
pub mod review;
pub mod shell;

use std::path::PathBuf;

use crate::config::{ApiConfig, WebConfig};

pub use client::{WebApiClient, WebApiPaths};
pub use inspect::{
    LibrarySearchFilters, LibrarySearchScreen, LibrarySearchScreenLoader, ReleaseDetailScreen,
    ReleaseDetailScreenLoader, ReleaseInstanceDetailScreen, ReleaseInstanceDetailScreenLoader,
};
pub use operate::{
    IssueQueueActionRequest, IssueQueueFilters, IssueQueueScreen, IssueQueueScreenLoader,
    IssueQueueSummary, JobsScreen, JobsScreenFilters, JobsScreenLoader, JobsScreenSummary,
};
pub use review::{CandidateReviewFilters, CandidateReviewScreen, CandidateReviewScreenLoader};
pub use shell::{ShellNavGroup, ShellNavItem, ShellRoute, WebShell};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSurface {
    pub mount_path: String,
    pub asset_dir: PathBuf,
    pub shell: WebShell,
    pub api_client: WebApiClient,
}

impl WebSurface {
    pub fn from_config(config: &WebConfig) -> Self {
        Self::from_config_with_api(config, &ApiConfig::default())
    }

    pub fn from_config_with_api(config: &WebConfig, api: &ApiConfig) -> Self {
        Self {
            mount_path: config.mount_path.clone(),
            asset_dir: config.asset_dir.clone(),
            shell: WebShell::default(),
            api_client: WebApiClient::from_api_base_path(&api.base_path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn web_surface_scaffolds_shell_and_api_client() {
        let surface =
            WebSurface::from_config_with_api(&WebConfig::default(), &ApiConfig::default());

        assert_eq!(surface.mount_path, "/");
        assert_eq!(surface.shell.default_route, ShellRoute::IssueQueue);
        assert_eq!(surface.api_client.paths.jobs, "/api/jobs");
        assert!(
            surface
                .shell
                .nav_groups
                .iter()
                .flat_map(|group| group.items.iter())
                .any(|item| item.route == ShellRoute::ExportPreview)
        );
    }
}
