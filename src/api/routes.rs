#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRoute {
    pub method: HttpMethod,
    pub path: String,
    pub long_running: bool,
}

impl ApiRoute {
    fn new(method: HttpMethod, path: String, long_running: bool) -> Self {
        Self {
            method,
            path,
            long_running,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Patch,
}

pub fn core_routes(base_path: &str) -> Vec<ApiRoute> {
    [
        (HttpMethod::Get, "/releases", false),
        (HttpMethod::Get, "/releases/{id}", false),
        (HttpMethod::Patch, "/releases/{id}", false),
        (HttpMethod::Get, "/release-groups", false),
        (HttpMethod::Get, "/release-instances", false),
        (HttpMethod::Get, "/release-instances/{id}", false),
        (
            HttpMethod::Get,
            "/release-instances/{id}/export-preview",
            false,
        ),
        (
            HttpMethod::Post,
            "/release-instances/{id}/resolve-match",
            true,
        ),
        (HttpMethod::Post, "/release-instances/{id}/reprocess", true),
        (HttpMethod::Patch, "/release-instances/{id}", false),
        (
            HttpMethod::Patch,
            "/release-instances/{id}/tracks/{track_instance_id}",
            false,
        ),
        (
            HttpMethod::Get,
            "/release-instances/{id}/candidate-matches",
            false,
        ),
        (
            HttpMethod::Post,
            "/release-instances/{id}/candidate-matches/{candidate_id}/select",
            true,
        ),
        (HttpMethod::Get, "/issues", false),
        (HttpMethod::Get, "/issues/{id}", false),
        (HttpMethod::Post, "/issues/{id}/resolve", true),
        (HttpMethod::Post, "/issues/{id}/suppress", true),
        (HttpMethod::Get, "/jobs", false),
        (HttpMethod::Get, "/jobs/{id}", false),
        (HttpMethod::Post, "/jobs/{id}/retry", true),
        (HttpMethod::Get, "/import-batches", false),
        (HttpMethod::Post, "/import-batches", true),
        (HttpMethod::Post, "/import-batches/from-path", true),
        (HttpMethod::Post, "/watchers/rescan", true),
        (HttpMethod::Get, "/sources", false),
        (HttpMethod::Get, "/config/validation", false),
        (HttpMethod::Get, "/export-profiles", false),
    ]
    .into_iter()
    .map(|(method, suffix, long_running)| {
        ApiRoute::new(method, format!("{base_path}{suffix}"), long_running)
    })
    .collect()
}
