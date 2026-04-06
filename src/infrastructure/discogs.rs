use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::application::matching::{
    DiscogsMetadataProvider, DiscogsReleaseCandidate, DiscogsReleaseQuery,
};
use crate::application::observability::{LogLevel, ObservabilityContext, labels};
use crate::config::DiscogsConfig;

const DEFAULT_BASE_URL: &str = "https://api.discogs.com";
const CACHE_CAPACITY: usize = 128;

#[derive(Debug, Clone)]
pub struct DiscogsClient {
    enabled: bool,
    base_url: String,
    http: reqwest::Client,
    min_interval: Duration,
    state: Arc<Mutex<ClientState>>,
    observability: Option<ObservabilityContext>,
}

#[derive(Debug)]
struct ClientState {
    next_request_at: Instant,
    cache_order: VecDeque<String>,
    cache: HashMap<String, String>,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            next_request_at: Instant::now(),
            cache_order: VecDeque::new(),
            cache: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscogsError {
    pub message: String,
}

impl DiscogsClient {
    pub fn from_config(config: &DiscogsConfig) -> Self {
        Self::from_config_with_observability(config, None)
    }

    pub fn from_config_with_observability(
        config: &DiscogsConfig,
        observability: Option<ObservabilityContext>,
    ) -> Self {
        Self::new(
            DEFAULT_BASE_URL,
            config.enabled,
            config.personal_access_token.clone(),
            config.rate_limit_per_second,
            observability,
        )
        .expect("discogs client should construct")
    }

    pub fn new(
        base_url: impl Into<String>,
        enabled: bool,
        personal_access_token: Option<String>,
        rate_limit_per_second: u16,
        observability: Option<ObservabilityContext>,
    ) -> Result<Self, DiscogsError> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(USER_AGENT, HeaderValue::from_static("discern/0.1"));
        if let Some(token) = personal_access_token {
            let value =
                HeaderValue::from_str(&format!("Discogs token={token}")).map_err(|error| {
                    DiscogsError {
                        message: format!("invalid Discogs auth header: {error}"),
                    }
                })?;
            headers.insert(AUTHORIZATION, value);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|error| DiscogsError {
                message: format!("failed to build Discogs HTTP client: {error}"),
            })?;
        Ok(Self {
            enabled,
            base_url,
            http,
            min_interval: Duration::from_secs_f64(1.0 / f64::from(rate_limit_per_second.max(1))),
            state: Arc::new(Mutex::new(ClientState::default())),
            observability,
        })
    }

    pub async fn search_releases(
        &self,
        query: &DiscogsReleaseQuery,
        limit: u8,
    ) -> Result<Vec<DiscogsReleaseCandidate>, DiscogsError> {
        if !self.enabled {
            self.record_provider_result("disabled", "/database/search");
            return Ok(Vec::new());
        }
        let mut params = vec![
            ("type", "release".to_string()),
            ("per_page", limit.to_string()),
            ("page", "1".to_string()),
        ];
        if let Some(value) = query.text.as_deref() {
            params.push(("q", value.to_string()));
        }
        if let Some(value) = query.artist.as_deref() {
            params.push(("artist", value.to_string()));
        }
        if let Some(value) = query.title.as_deref() {
            params.push(("release_title", value.to_string()));
        }
        if let Some(value) = query.year.as_deref() {
            params.push(("year", value.to_string()));
        }
        if let Some(value) = query.label.as_deref() {
            params.push(("label", value.to_string()));
        }
        if let Some(value) = query.catalog_number.as_deref() {
            params.push(("catno", value.to_string()));
        }
        if let Some(value) = query.format_hint.as_deref() {
            params.push(("format", value.to_string()));
        }

        let body = self.get_json("/database/search", &params).await?;
        let response: SearchResponse =
            serde_json::from_str(&body).map_err(|error| DiscogsError {
                message: format!("failed to decode Discogs search response: {error}"),
            })?;
        Ok(response.results.into_iter().map(Into::into).collect())
    }

    async fn get_json(
        &self,
        path: &str,
        params: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Result<String, DiscogsError> {
        let cache_key = format!(
            "{}?{}",
            path,
            params
                .iter()
                .map(|(key, value)| format!("{}={}", key.as_ref(), value.as_ref()))
                .collect::<Vec<_>>()
                .join("&")
        );
        {
            let state = self.state.lock().await;
            if let Some(body) = state.cache.get(&cache_key) {
                self.record_provider_result("cache_hit", path);
                return Ok(body.clone());
            }
        }
        self.wait_for_turn().await;
        let response = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .query(
                &params
                    .iter()
                    .map(|(key, value)| (key.as_ref(), value.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .send()
            .await
            .map_err(|error| DiscogsError {
                message: format!("Discogs request failed: {error}"),
            })
            .inspect_err(|_| self.record_provider_result("request_error", path))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| DiscogsError {
                message: format!("failed to read Discogs response body: {error}"),
            })
            .inspect_err(|_| self.record_provider_result("request_error", path))?;
        if !status.is_success() {
            self.record_provider_result("http_error", path);
            return Err(DiscogsError {
                message: format!("Discogs request returned {status}: {body}"),
            });
        }
        self.record_provider_result("success", path);
        let mut state = self.state.lock().await;
        state.cache.insert(cache_key.clone(), body.clone());
        state.cache_order.push_back(cache_key);
        while state.cache_order.len() > CACHE_CAPACITY {
            if let Some(oldest) = state.cache_order.pop_front() {
                state.cache.remove(&oldest);
            }
        }
        Ok(body)
    }

    async fn wait_for_turn(&self) {
        let wait = {
            let mut state = self.state.lock().await;
            let now = Instant::now();
            let wait = state.next_request_at.saturating_duration_since(now);
            state.next_request_at = now + wait + self.min_interval;
            wait
        };
        if !wait.is_zero() {
            if let Some(observability) = &self.observability {
                observability.metrics.increment_counter(
                    "metadata_provider_rate_limit_hits_total",
                    labels([("provider", "discogs")]),
                );
                observability.emit(
                    LogLevel::Warn,
                    "provider_rate_limit_wait",
                    [("provider", "discogs"), ("path", "/database/search")],
                );
            }
            sleep(wait).await;
        }
    }

    fn record_provider_result(&self, result: &str, path: &str) {
        if let Some(observability) = &self.observability {
            observability.metrics.increment_counter(
                "metadata_provider_requests_total",
                labels([("provider", "discogs"), ("result", result)]),
            );
            observability.emit(
                if matches!(result, "success" | "cache_hit" | "disabled") {
                    LogLevel::Info
                } else {
                    LogLevel::Warn
                },
                "provider_request",
                [("provider", "discogs"), ("result", result), ("path", path)],
            );
        }
    }
}

impl DiscogsMetadataProvider for DiscogsClient {
    fn search_releases(
        &self,
        query: &DiscogsReleaseQuery,
        limit: u8,
    ) -> impl std::future::Future<Output = Result<Vec<DiscogsReleaseCandidate>, String>> + Send
    {
        let query = query.clone();
        let this = self.clone();
        async move {
            this.search_releases(&query, limit)
                .await
                .map_err(|error| error.message)
        }
    }
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    #[serde(default)]
    results: Vec<SearchResult>,
}

#[derive(Debug, Deserialize)]
struct SearchResult {
    id: u64,
    title: String,
    year: Option<u16>,
    country: Option<String>,
    #[serde(default)]
    label: Vec<String>,
    #[serde(default)]
    format: Vec<String>,
    catno: Option<String>,
}

impl From<SearchResult> for DiscogsReleaseCandidate {
    fn from(value: SearchResult) -> Self {
        let raw_payload = serde_json::to_string(&serde_json::json!({
            "id": value.id,
            "title": value.title,
            "year": value.year,
            "country": value.country,
            "label": value.label,
            "format": value.format,
            "catno": value.catno,
        }))
        .expect("discogs payload should serialize");
        let artist = value
            .title
            .split_once(" - ")
            .map(|(artist, _)| artist.to_string());
        Self {
            id: value.id.to_string(),
            artist,
            title: value
                .title
                .split_once(" - ")
                .map(|(_, title)| title.to_string())
                .unwrap_or(value.title.clone()),
            year: value.year.map(|year| year.to_string()),
            country: value.country,
            label: value.label.first().cloned(),
            catalog_number: value.catno,
            format_descriptors: value.format,
            raw_payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use super::*;

    #[tokio::test]
    async fn discogs_search_shapes_request_and_parses_results() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("request should arrive");
            let mut buffer = [0_u8; 4096];
            let read = stream
                .read(&mut buffer)
                .expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.contains("GET /database/search?type=release"));
            assert!(request.contains("artist=Radiohead"));
            assert!(request.contains("release_title=Kid+A"));
            assert!(request.contains("catno=XLLP782"));
            let body = r#"{"results":[{"id":1,"title":"Radiohead - Kid A","year":2000,"country":"UK","label":["XL Recordings"],"format":["CD","Album"],"catno":"XLLP782"}]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("response should write");
        });

        let client = DiscogsClient::new(
            format!("http://{address}"),
            true,
            Some("token".to_string()),
            2,
            None,
        )
        .expect("client should build");
        let results = client
            .search_releases(
                &DiscogsReleaseQuery {
                    artist: Some("Radiohead".to_string()),
                    title: Some("Kid A".to_string()),
                    catalog_number: Some("XLLP782".to_string()),
                    ..DiscogsReleaseQuery::default()
                },
                5,
            )
            .await
            .expect("search should succeed");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].artist.as_deref(), Some("Radiohead"));
        assert_eq!(results[0].label.as_deref(), Some("XL Recordings"));
        assert_eq!(results[0].catalog_number.as_deref(), Some("XLLP782"));
    }
}
