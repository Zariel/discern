#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebShell {
    pub default_route: ShellRoute,
    pub nav_groups: Vec<ShellNavGroup>,
}

impl Default for WebShell {
    fn default() -> Self {
        Self {
            default_route: ShellRoute::IssueQueue,
            nav_groups: vec![
                ShellNavGroup {
                    title: "Operate".to_string(),
                    items: vec![
                        ShellNavItem::new("Issue Queue", ShellRoute::IssueQueue),
                        ShellNavItem::new("Jobs", ShellRoute::Jobs),
                        ShellNavItem::new("Dashboard", ShellRoute::Dashboard),
                    ],
                },
                ShellNavGroup {
                    title: "Inspect".to_string(),
                    items: vec![
                        ShellNavItem::new("Library Search", ShellRoute::LibrarySearch),
                        ShellNavItem::new("Release Detail", ShellRoute::ReleaseDetail),
                        ShellNavItem::new(
                            "Release Instance Detail",
                            ShellRoute::ReleaseInstanceDetail,
                        ),
                        ShellNavItem::new("Export Preview", ShellRoute::ExportPreview),
                    ],
                },
                ShellNavGroup {
                    title: "Resolve".to_string(),
                    items: vec![ShellNavItem::new(
                        "Candidate Review",
                        ShellRoute::CandidateReview,
                    )],
                },
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellNavGroup {
    pub title: String,
    pub items: Vec<ShellNavItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellNavItem {
    pub label: String,
    pub route: ShellRoute,
}

impl ShellNavItem {
    pub fn new(label: impl Into<String>, route: ShellRoute) -> Self {
        Self {
            label: label.into(),
            route,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellRoute {
    Dashboard,
    LibrarySearch,
    ReleaseDetail,
    ReleaseInstanceDetail,
    IssueQueue,
    CandidateReview,
    ExportPreview,
    Jobs,
}

impl ShellRoute {
    pub fn path(self) -> &'static str {
        match self {
            ShellRoute::Dashboard => "/",
            ShellRoute::LibrarySearch => "/library",
            ShellRoute::ReleaseDetail => "/releases/:release_id",
            ShellRoute::ReleaseInstanceDetail => "/release-instances/:release_instance_id",
            ShellRoute::IssueQueue => "/issues",
            ShellRoute::CandidateReview => {
                "/release-instances/:release_instance_id/candidate-matches"
            }
            ShellRoute::ExportPreview => "/release-instances/:release_instance_id/export-preview",
            ShellRoute::Jobs => "/jobs",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_defaults_to_issue_queue_and_dense_navigation() {
        let shell = WebShell::default();

        assert_eq!(shell.default_route, ShellRoute::IssueQueue);
        assert_eq!(shell.nav_groups.len(), 3);
        assert!(
            shell
                .nav_groups
                .iter()
                .flat_map(|group| group.items.iter())
                .any(|item| item.route == ShellRoute::CandidateReview)
        );
    }

    #[test]
    fn shell_routes_cover_operator_workflows() {
        assert_eq!(ShellRoute::Dashboard.path(), "/");
        assert_eq!(ShellRoute::IssueQueue.path(), "/issues");
        assert_eq!(
            ShellRoute::ExportPreview.path(),
            "/release-instances/:release_instance_id/export-preview"
        );
    }
}
