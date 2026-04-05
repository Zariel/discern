use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::application::matching::{
    MusicBrainzArtistCredit as MatchingArtistCredit, MusicBrainzLabelInfo as MatchingLabelInfo,
    MusicBrainzMetadataProvider, MusicBrainzReleaseCandidate,
    MusicBrainzReleaseDetail as MatchingReleaseDetail, MusicBrainzReleaseGroupCandidate,
    MusicBrainzReleaseGroupRef as MatchingReleaseGroupRef,
};
use crate::config::MusicBrainzConfig;

const DEFAULT_BASE_URL: &str = "https://musicbrainz.org/ws/2";
const RELEASE_LOOKUP_INCLUDES: &str = "artist-credits+labels+recordings+release-groups+media";
const CACHE_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub struct MusicBrainzClient {
    base_url: String,
    http: reqwest::Client,
    min_interval: Duration,
    state: Arc<Mutex<ClientState>>,
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
pub struct MusicBrainzError {
    pub kind: MusicBrainzErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MusicBrainzErrorKind {
    Request,
    RateLimited,
    Decode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzReleaseSearchResult {
    pub id: String,
    pub score: u16,
    pub title: String,
    pub status: Option<String>,
    pub country: Option<String>,
    pub date: Option<String>,
    pub barcode: Option<String>,
    pub packaging: Option<String>,
    pub artist_credit: Vec<MusicBrainzArtistCredit>,
    pub release_group: Option<MusicBrainzReleaseGroupRef>,
    pub label_info: Vec<MusicBrainzLabelInfo>,
    pub track_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzReleaseGroupSearchResult {
    pub id: String,
    pub score: u16,
    pub title: String,
    pub primary_type: Option<String>,
    pub first_release_date: Option<String>,
    pub artist_credit: Vec<MusicBrainzArtistCredit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzReleaseDetail {
    pub id: String,
    pub title: String,
    pub status: Option<String>,
    pub country: Option<String>,
    pub date: Option<String>,
    pub barcode: Option<String>,
    pub packaging: Option<String>,
    pub artist_credit: Vec<MusicBrainzArtistCredit>,
    pub release_group: Option<MusicBrainzReleaseGroupRef>,
    pub label_info: Vec<MusicBrainzLabelInfo>,
    pub media: Vec<MusicBrainzMedium>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzArtistCredit {
    pub name: String,
    pub joinphrase: Option<String>,
    pub artist_id: String,
    pub artist_name: String,
    pub artist_sort_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzReleaseGroupRef {
    pub id: String,
    pub title: String,
    pub primary_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzLabelInfo {
    pub catalog_number: Option<String>,
    pub label_id: Option<String>,
    pub label_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzMedium {
    pub position: Option<u32>,
    pub format: Option<String>,
    pub track_count: u32,
}

impl MusicBrainzClient {
    pub fn from_config(config: &MusicBrainzConfig) -> Self {
        Self::new(
            DEFAULT_BASE_URL,
            config.contact_email.clone(),
            config.rate_limit_per_second,
        )
        .expect("musicbrainz client should construct")
    }

    pub fn new(
        base_url: impl Into<String>,
        contact_email: Option<String>,
        rate_limit_per_second: u16,
    ) -> Result<Self, MusicBrainzError> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&build_user_agent(contact_email.as_deref())).map_err(
                |error| MusicBrainzError {
                    kind: MusicBrainzErrorKind::Request,
                    message: format!("invalid user agent: {error}"),
                },
            )?,
        );
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|error| MusicBrainzError {
                kind: MusicBrainzErrorKind::Request,
                message: format!("failed to build MusicBrainz HTTP client: {error}"),
            })?;
        let min_interval = Duration::from_secs_f64(1.0 / f64::from(rate_limit_per_second.max(1)));

        Ok(Self {
            base_url,
            http,
            min_interval,
            state: Arc::new(Mutex::new(ClientState::default())),
        })
    }

    pub async fn search_releases(
        &self,
        query: &str,
        limit: u8,
    ) -> Result<Vec<MusicBrainzReleaseSearchResult>, MusicBrainzError> {
        let limit_value = limit.to_string();
        let body = self
            .get_json(
                "/release",
                &[
                    ("query", query),
                    ("limit", limit_value.as_str()),
                    ("fmt", "json"),
                ],
            )
            .await?;
        let response: ReleaseSearchResponse =
            serde_json::from_str(&body).map_err(|error| MusicBrainzError {
                kind: MusicBrainzErrorKind::Decode,
                message: format!("failed to decode MusicBrainz release search: {error}"),
            })?;
        Ok(response.releases.into_iter().map(Into::into).collect())
    }

    pub async fn search_release_groups(
        &self,
        query: &str,
        limit: u8,
    ) -> Result<Vec<MusicBrainzReleaseGroupSearchResult>, MusicBrainzError> {
        let limit_value = limit.to_string();
        let body = self
            .get_json(
                "/release-group",
                &[
                    ("query", query),
                    ("limit", limit_value.as_str()),
                    ("fmt", "json"),
                ],
            )
            .await?;
        let response: ReleaseGroupSearchResponse =
            serde_json::from_str(&body).map_err(|error| MusicBrainzError {
                kind: MusicBrainzErrorKind::Decode,
                message: format!("failed to decode MusicBrainz release-group search: {error}"),
            })?;
        Ok(response
            .release_groups
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn lookup_release(
        &self,
        release_id: &str,
    ) -> Result<MusicBrainzReleaseDetail, MusicBrainzError> {
        let path = format!("/release/{release_id}");
        let body = self
            .get_json(&path, &[("inc", RELEASE_LOOKUP_INCLUDES), ("fmt", "json")])
            .await?;
        let response: ReleaseLookupResponse =
            serde_json::from_str(&body).map_err(|error| MusicBrainzError {
                kind: MusicBrainzErrorKind::Decode,
                message: format!("failed to decode MusicBrainz release lookup: {error}"),
            })?;
        Ok(response.into())
    }

    async fn get_json(
        &self,
        path: &str,
        params: &[(&str, &str)],
    ) -> Result<String, MusicBrainzError> {
        let key = cache_key(path, params);
        let wait = {
            let mut state = self.state.lock().await;
            if let Some(cached) = state.cache.get(&key) {
                return Ok(cached.clone());
            }
            let now = Instant::now();
            let wait = state
                .next_request_at
                .checked_duration_since(now)
                .unwrap_or_default();
            state.next_request_at = now + wait + self.min_interval;
            wait
        };

        if !wait.is_zero() {
            sleep(wait).await;
        }

        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .get(&url)
            .query(params)
            .send()
            .await
            .map_err(|error| MusicBrainzError {
                kind: MusicBrainzErrorKind::Request,
                message: format!("MusicBrainz request failed: {error}"),
            })?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(MusicBrainzError {
                kind: MusicBrainzErrorKind::RateLimited,
                message: "MusicBrainz rate limit exceeded".to_string(),
            });
        }
        if !response.status().is_success() {
            return Err(MusicBrainzError {
                kind: MusicBrainzErrorKind::Request,
                message: format!("MusicBrainz returned HTTP {}", response.status()),
            });
        }

        let body = response.text().await.map_err(|error| MusicBrainzError {
            kind: MusicBrainzErrorKind::Request,
            message: format!("failed to read MusicBrainz response body: {error}"),
        })?;

        let mut state = self.state.lock().await;
        if !state.cache.contains_key(&key) {
            state.cache_order.push_back(key.clone());
        }
        state.cache.insert(key, body.clone());
        while state.cache_order.len() > CACHE_CAPACITY {
            if let Some(oldest) = state.cache_order.pop_front() {
                state.cache.remove(&oldest);
            }
        }
        Ok(body)
    }
}

fn cache_key(path: &str, params: &[(&str, &str)]) -> String {
    let mut parts = params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    parts.sort();
    format!("{path}?{}", parts.join("&"))
}

fn build_user_agent(contact_email: Option<&str>) -> String {
    match contact_email {
        Some(email) if !email.trim().is_empty() => format!("discern/0.1.0 ({email})"),
        _ => "discern/0.1.0".to_string(),
    }
}

#[derive(Debug, Deserialize)]
struct ReleaseSearchResponse {
    #[serde(default)]
    releases: Vec<ReleaseSearchItem>,
}

#[derive(Debug, Deserialize)]
struct ReleaseGroupSearchResponse {
    #[serde(rename = "release-groups", default)]
    release_groups: Vec<ReleaseGroupSearchItem>,
}

#[derive(Debug, Deserialize)]
struct ReleaseSearchItem {
    id: String,
    score: String,
    title: String,
    status: Option<String>,
    country: Option<String>,
    date: Option<String>,
    barcode: Option<String>,
    packaging: Option<String>,
    #[serde(rename = "artist-credit", default)]
    artist_credit: Vec<ArtistCreditDto>,
    #[serde(rename = "release-group")]
    release_group: Option<ReleaseGroupRefDto>,
    #[serde(rename = "label-info", default)]
    label_info: Vec<LabelInfoDto>,
    #[serde(rename = "track-count")]
    track_count: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ReleaseGroupSearchItem {
    id: String,
    score: String,
    title: String,
    #[serde(rename = "primary-type")]
    primary_type: Option<String>,
    #[serde(rename = "first-release-date")]
    first_release_date: Option<String>,
    #[serde(rename = "artist-credit", default)]
    artist_credit: Vec<ArtistCreditDto>,
}

#[derive(Debug, Deserialize)]
struct ReleaseLookupResponse {
    id: String,
    title: String,
    status: Option<String>,
    country: Option<String>,
    date: Option<String>,
    barcode: Option<String>,
    packaging: Option<String>,
    #[serde(rename = "artist-credit", default)]
    artist_credit: Vec<ArtistCreditDto>,
    #[serde(rename = "release-group")]
    release_group: Option<ReleaseGroupRefDto>,
    #[serde(rename = "label-info", default)]
    label_info: Vec<LabelInfoDto>,
    #[serde(default)]
    media: Vec<MediumDto>,
}

#[derive(Debug, Deserialize)]
struct ArtistCreditDto {
    name: String,
    joinphrase: Option<String>,
    artist: ArtistDto,
}

#[derive(Debug, Deserialize)]
struct ArtistDto {
    id: String,
    name: String,
    #[serde(rename = "sort-name")]
    sort_name: String,
}

#[derive(Debug, Deserialize)]
struct ReleaseGroupRefDto {
    id: String,
    title: String,
    #[serde(rename = "primary-type")]
    primary_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LabelInfoDto {
    #[serde(rename = "catalog-number")]
    catalog_number: Option<String>,
    label: Option<LabelDto>,
}

#[derive(Debug, Deserialize)]
struct LabelDto {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct MediumDto {
    position: Option<u32>,
    format: Option<String>,
    #[serde(rename = "track-count")]
    track_count: u32,
}

impl From<ArtistCreditDto> for MusicBrainzArtistCredit {
    fn from(value: ArtistCreditDto) -> Self {
        Self {
            name: value.name,
            joinphrase: value.joinphrase,
            artist_id: value.artist.id,
            artist_name: value.artist.name,
            artist_sort_name: value.artist.sort_name,
        }
    }
}

impl From<ReleaseGroupRefDto> for MusicBrainzReleaseGroupRef {
    fn from(value: ReleaseGroupRefDto) -> Self {
        Self {
            id: value.id,
            title: value.title,
            primary_type: value.primary_type,
        }
    }
}

impl From<LabelInfoDto> for MusicBrainzLabelInfo {
    fn from(value: LabelInfoDto) -> Self {
        Self {
            catalog_number: value.catalog_number,
            label_id: value.label.as_ref().map(|label| label.id.clone()),
            label_name: value.label.map(|label| label.name),
        }
    }
}

impl From<MediumDto> for MusicBrainzMedium {
    fn from(value: MediumDto) -> Self {
        Self {
            position: value.position,
            format: value.format,
            track_count: value.track_count,
        }
    }
}

impl From<ReleaseSearchItem> for MusicBrainzReleaseSearchResult {
    fn from(value: ReleaseSearchItem) -> Self {
        Self {
            id: value.id,
            score: value.score.parse().unwrap_or_default(),
            title: value.title,
            status: value.status,
            country: value.country,
            date: value.date,
            barcode: value.barcode,
            packaging: value.packaging,
            artist_credit: value.artist_credit.into_iter().map(Into::into).collect(),
            release_group: value.release_group.map(Into::into),
            label_info: value.label_info.into_iter().map(Into::into).collect(),
            track_count: value.track_count,
        }
    }
}

impl From<ReleaseGroupSearchItem> for MusicBrainzReleaseGroupSearchResult {
    fn from(value: ReleaseGroupSearchItem) -> Self {
        Self {
            id: value.id,
            score: value.score.parse().unwrap_or_default(),
            title: value.title,
            primary_type: value.primary_type,
            first_release_date: value.first_release_date,
            artist_credit: value.artist_credit.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<ReleaseLookupResponse> for MusicBrainzReleaseDetail {
    fn from(value: ReleaseLookupResponse) -> Self {
        Self {
            id: value.id,
            title: value.title,
            status: value.status,
            country: value.country,
            date: value.date,
            barcode: value.barcode,
            packaging: value.packaging,
            artist_credit: value.artist_credit.into_iter().map(Into::into).collect(),
            release_group: value.release_group.map(Into::into),
            label_info: value.label_info.into_iter().map(Into::into).collect(),
            media: value.media.into_iter().map(Into::into).collect(),
        }
    }
}

impl MusicBrainzMetadataProvider for MusicBrainzClient {
    async fn search_releases(
        &self,
        query: &str,
        limit: u8,
    ) -> Result<Vec<MusicBrainzReleaseCandidate>, String> {
        MusicBrainzClient::search_releases(self, query, limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| MusicBrainzReleaseCandidate {
                        id: item.id,
                        title: item.title,
                        score: item.score,
                        artist_names: item
                            .artist_credit
                            .into_iter()
                            .map(|artist| artist.artist_name)
                            .collect(),
                        release_group_id: item.release_group.as_ref().map(|group| group.id.clone()),
                        release_group_title: item.release_group.map(|group| group.title),
                        country: item.country,
                        date: item.date,
                        track_count: item.track_count,
                    })
                    .collect()
            })
            .map_err(|error| error.message)
    }

    async fn search_release_groups(
        &self,
        query: &str,
        limit: u8,
    ) -> Result<Vec<MusicBrainzReleaseGroupCandidate>, String> {
        MusicBrainzClient::search_release_groups(self, query, limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| MusicBrainzReleaseGroupCandidate {
                        id: item.id,
                        title: item.title,
                        score: item.score,
                        artist_names: item
                            .artist_credit
                            .into_iter()
                            .map(|artist| artist.artist_name)
                            .collect(),
                        primary_type: item.primary_type,
                        first_release_date: item.first_release_date,
                    })
                    .collect()
            })
            .map_err(|error| error.message)
    }

    async fn lookup_release(&self, release_id: &str) -> Result<MatchingReleaseDetail, String> {
        MusicBrainzClient::lookup_release(self, release_id)
            .await
            .map(|detail| MatchingReleaseDetail {
                id: detail.id,
                title: detail.title,
                country: detail.country,
                date: detail.date,
                artist_credit: detail
                    .artist_credit
                    .into_iter()
                    .map(|artist| MatchingArtistCredit {
                        artist_id: artist.artist_id,
                        artist_name: artist.artist_name,
                        artist_sort_name: artist.artist_sort_name,
                    })
                    .collect(),
                release_group: detail.release_group.map(|group| MatchingReleaseGroupRef {
                    id: group.id,
                    title: group.title,
                    primary_type: group.primary_type,
                }),
                label_info: detail
                    .label_info
                    .into_iter()
                    .map(|label| MatchingLabelInfo {
                        catalog_number: label.catalog_number,
                        label_name: label.label_name,
                    })
                    .collect(),
            })
            .map_err(|error| error.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[tokio::test]
    async fn release_search_shapes_request_and_parses_results() {
        let server = MockMusicBrainzServer::spawn(vec![MockResponse::json(
            200,
            r#"{"releases":[{"id":"release-1","score":"100","title":"Kid A","status":"Official","country":"GB","date":"2000-10-02","barcode":"123","packaging":"Jewel Case","track-count":10,"artist-credit":[{"name":"Radiohead","artist":{"id":"artist-1","name":"Radiohead","sort-name":"Radiohead"}}],"release-group":{"id":"group-1","title":"Kid A","primary-type":"Album"},"label-info":[{"catalog-number":"XLLP782","label":{"id":"label-1","name":"XL Recordings"}}]}]}"#,
        )]);
        let client =
            MusicBrainzClient::new(server.base_url(), Some("ops@example.com".to_string()), 20)
                .expect("client should construct");

        let releases = client
            .search_releases("release:kid a AND artist:radiohead", 5)
            .await
            .expect("search should succeed");

        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].title, "Kid A");
        assert_eq!(releases[0].artist_credit[0].artist_name, "Radiohead");
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].path.contains("/ws/2/release?"));
        assert!(
            requests[0]
                .path
                .contains("query=release%3Akid+a+AND+artist%3Aradiohead")
        );
        assert!(requests[0].path.contains("limit=5"));
        assert!(requests[0].headers.iter().any(|header| {
            header
                .to_ascii_lowercase()
                .contains("user-agent: discern/0.1.0 (ops@example.com)")
        }));
    }

    #[tokio::test]
    async fn release_group_search_uses_cache_for_identical_requests() {
        let server = MockMusicBrainzServer::spawn(vec![MockResponse::json(
            200,
            r#"{"release-groups":[{"id":"group-1","score":"87","title":"Kid A","primary-type":"Album","first-release-date":"2000","artist-credit":[{"name":"Radiohead","artist":{"id":"artist-1","name":"Radiohead","sort-name":"Radiohead"}}]}]}"#,
        )]);
        let client =
            MusicBrainzClient::new(server.base_url(), None, 20).expect("client should construct");

        let first = client
            .search_release_groups("releasegroup:kid a", 3)
            .await
            .expect("first search should succeed");
        let second = client
            .search_release_groups("releasegroup:kid a", 3)
            .await
            .expect("second search should succeed");

        assert_eq!(first, second);
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn release_lookup_includes_expected_inc_parameters_and_rate_limits() {
        let server = MockMusicBrainzServer::spawn(vec![
            MockResponse::json(
                200,
                r#"{"id":"release-1","title":"Kid A","status":"Official","country":"GB","date":"2000-10-02","barcode":"123","packaging":"Jewel Case","artist-credit":[{"name":"Radiohead","artist":{"id":"artist-1","name":"Radiohead","sort-name":"Radiohead"}}],"release-group":{"id":"group-1","title":"Kid A","primary-type":"Album"},"label-info":[{"catalog-number":"XLLP782","label":{"id":"label-1","name":"XL Recordings"}}],"media":[{"position":1,"format":"CD","track-count":10}]}"#,
            ),
            MockResponse::json(
                200,
                r#"{"id":"release-2","title":"Amnesiac","artist-credit":[],"label-info":[],"media":[]}"#,
            ),
        ]);
        let client =
            MusicBrainzClient::new(server.base_url(), None, 20).expect("client should construct");

        let first = client.lookup_release("release-1");
        let second = client.lookup_release("release-2");
        let (first, second) = tokio::join!(first, second);
        assert_eq!(
            first.expect("first lookup should succeed").media[0].track_count,
            10
        );
        assert_eq!(
            second.expect("second lookup should succeed").title,
            "Amnesiac"
        );

        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .path
                .contains("inc=artist-credits%2Blabels%2Brecordings%2Brelease-groups%2Bmedia")
        );
        let spacing = requests[1]
            .received_at
            .duration_since(requests[0].received_at);
        assert!(
            spacing >= Duration::from_millis(45),
            "spacing was {spacing:?}"
        );
    }

    struct MockMusicBrainzServer {
        base_url: String,
        requests: Arc<std::sync::Mutex<Vec<ObservedRequest>>>,
        join: Option<thread::JoinHandle<()>>,
    }

    #[derive(Clone, Debug)]
    struct ObservedRequest {
        path: String,
        headers: Vec<String>,
        received_at: Instant,
    }

    struct MockResponse {
        status: u16,
        body: String,
    }

    impl MockResponse {
        fn json(status: u16, body: &str) -> Self {
            Self {
                status,
                body: body.to_string(),
            }
        }
    }

    impl MockMusicBrainzServer {
        fn spawn(responses: Vec<MockResponse>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
            let address = listener
                .local_addr()
                .expect("listener address should resolve");
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests_clone = Arc::clone(&requests);
            let join = thread::spawn(move || {
                let mut responses = VecDeque::from(responses);
                for stream in listener.incoming() {
                    let mut stream = match stream {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    let mut buffer = [0_u8; 8192];
                    let read = match stream.read(&mut buffer) {
                        Ok(read) => read,
                        Err(_) => continue,
                    };
                    if read == 0 {
                        continue;
                    }
                    let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                    let mut lines = request.split("\r\n");
                    let request_line = lines.next().unwrap_or_default().to_string();
                    let path = request_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();
                    let headers = lines
                        .take_while(|line| !line.is_empty())
                        .map(str::to_string)
                        .collect::<Vec<_>>();
                    let observed = ObservedRequest {
                        path,
                        headers,
                        received_at: Instant::now(),
                    };
                    requests_clone
                        .lock()
                        .expect("request list should lock")
                        .push(observed);

                    let response = responses.pop_front().unwrap_or_else(|| MockResponse {
                        status: 500,
                        body: "{}".to_string(),
                    });
                    let body = response.body;
                    let payload = format!(
                        "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response.status,
                        body.len(),
                        body,
                    );
                    let _ = stream.write_all(payload.as_bytes());

                    if responses.is_empty() {
                        break;
                    }
                }
            });

            Self {
                base_url: format!("http://{address}/ws/2"),
                requests,
                join: Some(join),
            }
        }

        fn base_url(&self) -> String {
            self.base_url.clone()
        }

        fn requests(&self) -> Vec<ObservedRequest> {
            self.requests
                .lock()
                .expect("request list should lock")
                .clone()
        }
    }

    impl Drop for MockMusicBrainzServer {
        fn drop(&mut self) {
            if let Some(join) = self.join.take() {
                let _ = join.join();
            }
        }
    }
}
