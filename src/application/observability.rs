use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use serde::Serialize;

use crate::application::repository::{
    IssueListQuery, IssueRepository, ReleaseInstanceListQuery, ReleaseInstanceRepository,
};
use crate::domain::issue::{IssueState, IssueType};
use crate::domain::release_instance::ReleaseInstanceState;
use crate::support::pagination::PageRequest;

pub type LabelSet = BTreeMap<String, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StructuredLogEvent {
    pub timestamp_unix_seconds: i64,
    pub level: LogLevel,
    pub event: String,
    pub fields: LabelSet,
}

pub trait StructuredLogSink: Send + Sync {
    fn emit(&self, event: &StructuredLogEvent);
}

#[derive(Debug, Default)]
pub struct StderrStructuredLogSink;

impl StructuredLogSink for StderrStructuredLogSink {
    fn emit(&self, event: &StructuredLogEvent) {
        if let Ok(line) = serde_json::to_string(event) {
            eprintln!("{line}");
        }
    }
}

#[derive(Clone)]
pub struct ObservabilityContext {
    pub metrics: MetricsRegistry,
    log_sink: Arc<dyn StructuredLogSink>,
}

impl fmt::Debug for ObservabilityContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservabilityContext")
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

impl Default for ObservabilityContext {
    fn default() -> Self {
        Self {
            metrics: MetricsRegistry::default(),
            log_sink: Arc::new(StderrStructuredLogSink),
        }
    }
}

impl ObservabilityContext {
    pub fn with_log_sink(log_sink: Arc<dyn StructuredLogSink>) -> Self {
        Self {
            metrics: MetricsRegistry::default(),
            log_sink,
        }
    }

    pub fn emit<I, K, V>(&self, level: LogLevel, event: impl Into<String>, fields: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let event = StructuredLogEvent {
            timestamp_unix_seconds: unix_timestamp_seconds(),
            level,
            event: event.into(),
            fields: fields
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        };
        self.log_sink.emit(&event);
    }

    pub fn sync_issue_gauges<R>(&self, repository: &R)
    where
        R: IssueRepository,
    {
        self.metrics.reset_issue_gauges();
        let mut request = PageRequest::new(PageRequest::MAX_LIMIT, 0);
        loop {
            let page = match repository.list_issues(&IssueListQuery {
                page: request,
                ..IssueListQuery::default()
            }) {
                Ok(page) => page,
                Err(_) => return,
            };

            for issue in &page.items {
                self.metrics.set_issue_count(
                    issue_type_name(&issue.issue_type),
                    issue_state_name(&issue.state),
                    1,
                    true,
                );
            }

            if !page.has_more() {
                break;
            }
            request = PageRequest::new(PageRequest::MAX_LIMIT, request.next_offset());
        }
    }

    pub fn sync_release_instance_state_gauges<R>(&self, repository: &R)
    where
        R: ReleaseInstanceRepository,
    {
        self.metrics.reset_release_instance_state_gauges();
        let mut request = PageRequest::new(PageRequest::MAX_LIMIT, 0);
        loop {
            let page = match repository.list_release_instances(&ReleaseInstanceListQuery {
                page: request,
                ..ReleaseInstanceListQuery::default()
            }) {
                Ok(page) => page,
                Err(_) => return,
            };

            for release_instance in &page.items {
                self.metrics.set_release_instance_state_count(
                    release_instance_state_name(&release_instance.state),
                    1,
                    true,
                );
            }

            if !page.has_more() {
                break;
            }
            request = PageRequest::new(PageRequest::MAX_LIMIT, request.next_offset());
        }
    }
}

#[derive(Clone)]
pub struct MetricsRegistry {
    inner: Arc<Mutex<MetricsInner>>,
}

impl fmt::Debug for MetricsRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MetricsRegistry(..)")
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsInner::new())),
        }
    }
}

impl MetricsRegistry {
    pub fn increment_counter(&self, name: &str, labels: LabelSet) {
        self.add_counter(name, labels, 1.0);
    }

    pub fn add_counter(&self, name: &str, labels: LabelSet, value: f64) {
        let inner = self.inner.lock().expect("metrics registry lock poisoned");
        match name {
            "jobs_total" => {
                inner
                    .jobs_total
                    .get_or_create(&job_status_labels(&labels))
                    .inc_by(counter_value(value));
            }
            "imports_total" => {
                inner
                    .imports_total
                    .get_or_create(&outcome_labels(&labels))
                    .inc_by(counter_value(value));
            }
            "metadata_provider_requests_total" => {
                inner
                    .metadata_provider_requests_total
                    .get_or_create(&provider_result_labels(&labels))
                    .inc_by(counter_value(value));
            }
            "metadata_provider_rate_limit_hits_total" => {
                inner
                    .metadata_provider_rate_limit_hits_total
                    .get_or_create(&provider_labels(&labels))
                    .inc_by(counter_value(value));
            }
            "file_operations_total" => {
                inner
                    .file_operations_total
                    .get_or_create(&file_operation_labels(&labels))
                    .inc_by(counter_value(value));
            }
            "duplicate_detections_total" => {
                inner
                    .duplicate_detections_total
                    .get_or_create(&result_labels(&labels))
                    .inc_by(counter_value(value));
            }
            "compatibility_verification_failures_total" => {
                inner
                    .compatibility_verification_failures_total
                    .get_or_create(&result_labels(&labels))
                    .inc_by(counter_value(value));
            }
            _ => debug_assert!(false, "unknown counter metric {name}"),
        }
    }

    pub fn set_gauge(&self, name: &str, labels: LabelSet, value: f64) {
        let inner = self.inner.lock().expect("metrics registry lock poisoned");
        match name {
            "release_instances_in_state" => {
                inner
                    .release_instances_in_state
                    .get_or_create(&state_labels(&labels))
                    .set(gauge_value(value));
            }
            "issue_count" => {
                inner
                    .issue_count
                    .get_or_create(&issue_state_labels(&labels))
                    .set(gauge_value(value));
            }
            "startup_recovered_jobs" => {
                inner.startup_recovered_jobs.set(gauge_value(value));
            }
            _ => debug_assert!(false, "unknown gauge metric {name}"),
        }
    }

    pub fn observe_duration_seconds(&self, name: &str, labels: LabelSet, value: f64) {
        let inner = self.inner.lock().expect("metrics registry lock poisoned");
        match name {
            "job_duration_seconds" => inner
                .job_duration_seconds
                .get_or_create(&job_duration_labels(&labels))
                .observe(value),
            _ => debug_assert!(false, "unknown histogram metric {name}"),
        }
    }

    pub fn render_prometheus(&self) -> String {
        let inner = self.inner.lock().expect("metrics registry lock poisoned");
        let mut output = String::new();
        encode(&mut output, &inner.registry).expect("prometheus encoding should succeed");
        output
    }

    fn reset_issue_gauges(&self) {
        let inner = self.inner.lock().expect("metrics registry lock poisoned");
        for issue_type in all_issue_types() {
            for state in all_issue_states() {
                inner
                    .issue_count
                    .get_or_create(&IssueStateLabels {
                        issue_type: issue_type.to_string(),
                        state: state.to_string(),
                    })
                    .set(0);
            }
        }
    }

    fn set_issue_count(
        &self,
        issue_type: &'static str,
        state: &'static str,
        value: i64,
        increment: bool,
    ) {
        let inner = self.inner.lock().expect("metrics registry lock poisoned");
        let gauge = inner.issue_count.get_or_create(&IssueStateLabels {
            issue_type: issue_type.to_string(),
            state: state.to_string(),
        });
        if increment {
            gauge.inc_by(value);
        } else {
            gauge.set(value);
        }
    }

    fn reset_release_instance_state_gauges(&self) {
        let inner = self.inner.lock().expect("metrics registry lock poisoned");
        for state in all_release_instance_states() {
            inner
                .release_instances_in_state
                .get_or_create(&StateLabels {
                    state: state.to_string(),
                })
                .set(0);
        }
    }

    fn set_release_instance_state_count(&self, state: &'static str, value: i64, increment: bool) {
        let inner = self.inner.lock().expect("metrics registry lock poisoned");
        let gauge = inner
            .release_instances_in_state
            .get_or_create(&StateLabels {
                state: state.to_string(),
            });
        if increment {
            gauge.inc_by(value);
        } else {
            gauge.set(value);
        }
    }
}

struct MetricsInner {
    registry: Registry,
    jobs_total: Family<JobStatusLabels, Counter>,
    job_duration_seconds: Family<JobDurationLabels, Histogram>,
    imports_total: Family<OutcomeLabels, Counter>,
    release_instances_in_state: Family<StateLabels, Gauge>,
    metadata_provider_requests_total: Family<ProviderResultLabels, Counter>,
    metadata_provider_rate_limit_hits_total: Family<ProviderLabels, Counter>,
    file_operations_total: Family<FileOperationLabels, Counter>,
    duplicate_detections_total: Family<ResultLabels, Counter>,
    issue_count: Family<IssueStateLabels, Gauge>,
    compatibility_verification_failures_total: Family<ResultLabels, Counter>,
    startup_recovered_jobs: Gauge,
}

impl MetricsInner {
    fn new() -> Self {
        let mut registry = Registry::default();

        let jobs_total = Family::<JobStatusLabels, Counter>::default();
        registry.register(
            "jobs_total",
            "Completed pipeline jobs partitioned by job type and status.",
            jobs_total.clone(),
        );

        let job_duration_seconds =
            Family::<JobDurationLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.05, 2.0, 12))
            });
        registry.register(
            "job_duration_seconds",
            "Pipeline job execution time in seconds.",
            job_duration_seconds.clone(),
        );

        let imports_total = Family::<OutcomeLabels, Counter>::default();
        registry.register(
            "imports_total",
            "Ingest batch outcomes emitted by pipeline stages.",
            imports_total.clone(),
        );

        let release_instances_in_state = Family::<StateLabels, Gauge>::default();
        registry.register(
            "release_instances_in_state",
            "Current release-instance counts by lifecycle state.",
            release_instances_in_state.clone(),
        );

        let metadata_provider_requests_total = Family::<ProviderResultLabels, Counter>::default();
        registry.register(
            "metadata_provider_requests_total",
            "Metadata provider requests partitioned by provider and result.",
            metadata_provider_requests_total.clone(),
        );

        let metadata_provider_rate_limit_hits_total = Family::<ProviderLabels, Counter>::default();
        registry.register(
            "metadata_provider_rate_limit_hits_total",
            "Metadata provider waits caused by bounded request rate.",
            metadata_provider_rate_limit_hits_total.clone(),
        );

        let file_operations_total = Family::<FileOperationLabels, Counter>::default();
        registry.register(
            "file_operations_total",
            "Managed library file operations by import mode and result.",
            file_operations_total.clone(),
        );

        let duplicate_detections_total = Family::<ResultLabels, Counter>::default();
        registry.register(
            "duplicate_detections_total",
            "Duplicate detection outcomes emitted during enrichment.",
            duplicate_detections_total.clone(),
        );

        let issue_count = Family::<IssueStateLabels, Gauge>::default();
        registry.register(
            "issue_count",
            "Current issue counts by issue type and state.",
            issue_count.clone(),
        );

        let compatibility_verification_failures_total = Family::<ResultLabels, Counter>::default();
        registry.register(
            "compatibility_verification_failures_total",
            "Compatibility verification failures detected during verification.",
            compatibility_verification_failures_total.clone(),
        );

        let startup_recovered_jobs = Gauge::default();
        registry.register(
            "startup_recovered_jobs",
            "Count of unfinished jobs recovered during startup bootstrap.",
            startup_recovered_jobs.clone(),
        );

        Self {
            registry,
            jobs_total,
            job_duration_seconds,
            imports_total,
            release_instances_in_state,
            metadata_provider_requests_total,
            metadata_provider_rate_limit_hits_total,
            file_operations_total,
            duplicate_detections_total,
            issue_count,
            compatibility_verification_failures_total,
            startup_recovered_jobs,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct JobStatusLabels {
    job_type: String,
    status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct JobDurationLabels {
    job_type: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabels {
    outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StateLabels {
    state: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ProviderResultLabels {
    provider: String,
    result: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ProviderLabels {
    provider: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct FileOperationLabels {
    mode: String,
    result: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ResultLabels {
    result: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct IssueStateLabels {
    issue_type: String,
    state: String,
}

pub fn labels<const N: usize>(entries: [(&str, &str); N]) -> LabelSet {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

pub fn issue_type_name(issue_type: &IssueType) -> &'static str {
    match issue_type {
        IssueType::UnmatchedRelease => "unmatched_release",
        IssueType::AmbiguousReleaseMatch => "ambiguous_release_match",
        IssueType::ConflictingMetadata => "conflicting_metadata",
        IssueType::InconsistentTrackCount => "inconsistent_track_count",
        IssueType::MissingTracks => "missing_tracks",
        IssueType::CorruptFile => "corrupt_file",
        IssueType::UnsupportedFormat => "unsupported_format",
        IssueType::DuplicateReleaseInstance => "duplicate_release_instance",
        IssueType::UndistinguishableReleaseInstance => "undistinguishable_release_instance",
        IssueType::PlayerVisibilityCollision => "player_visibility_collision",
        IssueType::MissingArtwork => "missing_artwork",
        IssueType::BrokenTags => "broken_tags",
        IssueType::MultiDiscAmbiguity => "multi_disc_ambiguity",
        IssueType::CompilationArtistAmbiguity => "compilation_artist_ambiguity",
        IssueType::PlayerCompatibilityFailure => "player_compatibility_failure",
    }
}

fn issue_state_name(state: &IssueState) -> &'static str {
    match state {
        IssueState::Open => "open",
        IssueState::Resolved => "resolved",
        IssueState::Suppressed => "suppressed",
    }
}

fn release_instance_state_name(state: &ReleaseInstanceState) -> &'static str {
    match state {
        ReleaseInstanceState::Discovered => "discovered",
        ReleaseInstanceState::Staged => "staged",
        ReleaseInstanceState::Analyzed => "analyzed",
        ReleaseInstanceState::Matched => "matched",
        ReleaseInstanceState::NeedsReview => "needs_review",
        ReleaseInstanceState::RenderingExport => "rendering_export",
        ReleaseInstanceState::Tagging => "tagging",
        ReleaseInstanceState::Organizing => "organizing",
        ReleaseInstanceState::Imported => "imported",
        ReleaseInstanceState::Verified => "verified",
        ReleaseInstanceState::Quarantined => "quarantined",
        ReleaseInstanceState::Failed => "failed",
    }
}

fn all_issue_types() -> &'static [&'static str] {
    &[
        "unmatched_release",
        "ambiguous_release_match",
        "conflicting_metadata",
        "inconsistent_track_count",
        "missing_tracks",
        "corrupt_file",
        "unsupported_format",
        "duplicate_release_instance",
        "undistinguishable_release_instance",
        "player_visibility_collision",
        "missing_artwork",
        "broken_tags",
        "multi_disc_ambiguity",
        "compilation_artist_ambiguity",
        "player_compatibility_failure",
    ]
}

fn all_issue_states() -> &'static [&'static str] {
    &["open", "resolved", "suppressed"]
}

fn all_release_instance_states() -> &'static [&'static str] {
    &[
        "discovered",
        "staged",
        "analyzed",
        "matched",
        "needs_review",
        "rendering_export",
        "tagging",
        "organizing",
        "imported",
        "verified",
        "quarantined",
        "failed",
    ]
}

fn job_status_labels(labels: &LabelSet) -> JobStatusLabels {
    JobStatusLabels {
        job_type: label_value(labels, "type"),
        status: label_value(labels, "status"),
    }
}

fn job_duration_labels(labels: &LabelSet) -> JobDurationLabels {
    JobDurationLabels {
        job_type: label_value(labels, "type"),
    }
}

fn outcome_labels(labels: &LabelSet) -> OutcomeLabels {
    OutcomeLabels {
        outcome: label_value(labels, "outcome"),
    }
}

fn state_labels(labels: &LabelSet) -> StateLabels {
    StateLabels {
        state: label_value(labels, "state"),
    }
}

fn provider_result_labels(labels: &LabelSet) -> ProviderResultLabels {
    ProviderResultLabels {
        provider: label_value(labels, "provider"),
        result: label_value(labels, "result"),
    }
}

fn provider_labels(labels: &LabelSet) -> ProviderLabels {
    ProviderLabels {
        provider: label_value(labels, "provider"),
    }
}

fn file_operation_labels(labels: &LabelSet) -> FileOperationLabels {
    FileOperationLabels {
        mode: label_value(labels, "mode"),
        result: label_value(labels, "result"),
    }
}

fn issue_state_labels(labels: &LabelSet) -> IssueStateLabels {
    IssueStateLabels {
        issue_type: label_value(labels, "type"),
        state: label_value(labels, "state"),
    }
}

fn result_labels(labels: &LabelSet) -> ResultLabels {
    ResultLabels {
        result: label_value(labels, "result"),
    }
}

fn label_value(labels: &LabelSet, key: &str) -> String {
    labels.get(key).cloned().unwrap_or_default()
}

fn counter_value(value: f64) -> u64 {
    if value.is_sign_negative() {
        0
    } else {
        value.round() as u64
    }
}

fn gauge_value(value: f64) -> i64 {
    value.round() as i64
}

fn unix_timestamp_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::{LogLevel, MetricsRegistry, ObservabilityContext, labels};

    #[test]
    fn prometheus_output_contains_registered_metrics() {
        let metrics = MetricsRegistry::default();
        metrics.increment_counter(
            "metadata_provider_requests_total",
            labels([("provider", "musicbrainz"), ("result", "success")]),
        );
        metrics.observe_duration_seconds(
            "job_duration_seconds",
            labels([("type", "match_release_instance")]),
            0.25,
        );

        let rendered = metrics.render_prometheus();

        assert!(rendered.contains("metadata_provider_requests_total"));
        assert!(rendered.contains("provider=\"musicbrainz\""));
        assert!(rendered.contains("job_duration_seconds_bucket"));
    }

    #[test]
    fn observability_context_accepts_structured_events() {
        let context = ObservabilityContext::default();

        context.emit(
            LogLevel::Info,
            "runtime_bootstrap_completed",
            [("db_path", "/tmp/discern.db".to_string())],
        );
    }
}
