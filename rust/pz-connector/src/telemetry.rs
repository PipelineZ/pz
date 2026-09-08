//! The connector process's own OpenTelemetry composition root. Providers exist only after the host's
//! `HostInfo.otel_endpoint` arrived in `Handshake`; until then no subscriber is installed and every
//! `tracing` span is disabled. Trace context comes in as W3C `traceparent` metadata on every RPC;
//! the data plane carries none, so a write stream inherits the `BeginWrite` RPC's context through
//! its `SessionState`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use tower_http::classify::{GrpcErrorsAsFailures, SharedClassifier};
use tower_http::trace::TraceLayer;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Layer, SubscriberExt};

/// Upper bound on flushing at shutdown: inside the host's ten-second shutdown grace.
pub(crate) const FLUSH_BOUND: Duration = Duration::from_secs(3);
const EXPORT_TIMEOUT: Duration = Duration::from_secs(2);
/// The instrumentation scope every span and instrument this SDK emits is attributed to.
const SCOPE_NAME: &str = "pz.connector";
/// The one gRPC service this process serves; a request path outside it was never routed to a handler.
const SERVICE_PATH_PREFIX: &str = "/pz.connector.v1.PzConnector/";
/// The RPC that gets no span: a `HostChannel` call is a bidirectional stream the host may hold open
/// for this instance's whole lifetime, so a span around it would span the process, not an operation.
const UNTRACED_RPC: &str = "HostChannel";

struct Providers {
    /// `None` when a `tracing` subscriber was already installed and the OpenTelemetry layer could
    /// therefore not be: with no layer feeding it, a tracer provider would only ever export nothing.
    tracer: Option<TracerProvider>,
    meter: SdkMeterProvider,
}

static PROVIDERS: OnceLock<Providers> = OnceLock::new();

/// Whether a span opened now can actually reach the collector. False before [`start`] runs and after
/// a `start` that could not install its layer -- in both cases spans are left unrecorded rather than
/// built and dropped.
pub(crate) fn traces_enabled() -> bool {
    PROVIDERS.get().is_some_and(|p| p.tracer.is_some())
}

/// The meter a connector author records on. Backed by the global provider [`start`] installs; with no
/// endpoint the global default is a no-op provider, so this is always safe to call.
pub fn meter() -> opentelemetry::metrics::Meter {
    global::meter(SCOPE_NAME)
}

/// Builds and installs providers once; a second call changes nothing. The two failures it reports --
/// an endpoint that is not an absolute http(s) URL, and a `tracing` subscriber the connector author
/// installed before `serve_sink` ran -- are told to the caller rather than swallowed, but neither is
/// fatal: the caller prints them and completes the handshake. Metrics survive the second failure
/// (they need no subscriber); traces do not, and [`traces_enabled`] reports that for the rest of the
/// process's life so no span is ever built only to be dropped.
pub(crate) fn start(endpoint: &str, name: &str, version: &str, run_id: &str) -> Result<(), String> {
    if PROVIDERS.get().is_some() {
        return Ok(());
    }
    if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
        return Err(format!(
            "otel endpoint is not an absolute http(s) URL: {endpoint}"
        ));
    }

    let mut attrs = vec![
        KeyValue::new("service.name", "pz-connector"),
        KeyValue::new("service.version", version.to_string()),
        KeyValue::new("pz.connector.name", name.to_string()),
    ];
    if !run_id.is_empty() {
        attrs.push(KeyValue::new("pz.run.id", run_id.to_string()));
    }
    let resource = Resource::new(attrs);

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|e| e.to_string())?;
    let tracer = TracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter, opentelemetry_sdk::runtime::Tokio)
        .build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|e| e.to_string())?;
    let reader =
        PeriodicReader::builder(metric_exporter, opentelemetry_sdk::runtime::Tokio).build();
    let meter = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_meter_provider(meter.clone());

    // The transport crates are silenced because the OTLP exporter itself runs on them: exporting a
    // span opens h2/hyper/tonic spans, which the layer would turn into spans to export, which open
    // more -- a feedback loop that buries the handful of `pcp.*` spans under hundreds of
    // `queue_frame`/`FramedWrite::buffer` ones. Everything else stays at DEBUG so a connector
    // author's own instrumentation is exported. The OpenTelemetry crates' own internal events are
    // kept out of the export for the same reason and go to stderr instead (below).
    let filter = Targets::new()
        .with_default(LevelFilter::DEBUG)
        .with_target("h2", LevelFilter::OFF)
        .with_target("hyper", LevelFilter::OFF)
        .with_target("hyper_util", LevelFilter::OFF)
        .with_target("tonic", LevelFilter::OFF)
        .with_target("tower", LevelFilter::OFF)
        .with_target("opentelemetry", LevelFilter::OFF)
        .with_target("opentelemetry_sdk", LevelFilter::OFF)
        .with_target("opentelemetry-otlp", LevelFilter::OFF)
        .with_target("opentelemetry_otlp", LevelFilter::OFF);
    // A failed export is otherwise invisible: the SDK reports it as a `tracing` warning under its own
    // crate targets, and nothing would be listening. Those warnings go to stderr, which the host
    // keeps as the process's failure tail, so a connector that exports nothing can say why.
    let internal = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_target(true)
        .without_time()
        .with_filter(
            Targets::new()
                .with_target("opentelemetry", LevelFilter::WARN)
                .with_target("opentelemetry_sdk", LevelFilter::WARN)
                .with_target("opentelemetry-otlp", LevelFilter::WARN)
                .with_target("opentelemetry_otlp", LevelFilter::WARN),
        );
    // Source location, thread identity and the busy/idle timing fields are off: they would put
    // `code.filepath` (this crate's absolute path on whoever built the binary), `code.lineno`,
    // `thread.*`, `busy_ns` and `idle_ns` on every exported span, which the C# SDK's spans do not
    // carry -- an operator reading one trace must not find the two SDKs disagreeing about what a
    // `pcp.*` span means.
    let layer = tracing_opentelemetry::layer()
        .with_tracer(tracer.tracer(SCOPE_NAME))
        .with_location(false)
        .with_threads(false)
        .with_tracked_inactivity(false)
        .with_filter(filter);

    if tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(layer).with(internal),
    )
    .is_err()
    {
        // The author installed their own subscriber before `serve_sink` ran; theirs wins and the
        // OpenTelemetry layer is simply absent. The provider it would have fed is shut down rather
        // than left holding a batch worker that exports nothing for the life of the process -- on
        // the blocking pool, because a batch processor's shutdown blocks on its own runtime task and
        // doing that inline would deadlock an author's single-threaded runtime.
        tokio::task::spawn_blocking(move || {
            let _ = tracer.shutdown();
        });
        let _ = PROVIDERS.set(Providers {
            tracer: None,
            meter,
        });
        return Err(
            "a tracing subscriber was already installed; spans will not be exported".to_string(),
        );
    }

    let _ = PROVIDERS.set(Providers {
        tracer: Some(tracer),
        meter,
    });
    Ok(())
}

/// Shuts both providers down concurrently -- `shutdown` flushes on the way out -- and waits at most
/// [`FLUSH_BOUND`] for them. The bound is on this await, not on the work: a shutdown that overruns is
/// abandoned here and keeps running on the blocking pool, so late spans are dropped rather than
/// delaying exit, though dropping the runtime afterwards may still join those threads.
///
/// Concurrently, not in sequence: against an unreachable collector a sequential pair would let the
/// tracer spend the whole budget and leave the meter no time at all.
pub(crate) async fn flush_and_shutdown() {
    let Some(providers) = PROVIDERS.get() else {
        return;
    };
    let traces_done = Arc::new(AtomicBool::new(false));
    let metrics_done = Arc::new(AtomicBool::new(false));
    let traces = {
        let done = traces_done.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(tracer) = providers.tracer.as_ref() {
                let _ = tracer.shutdown();
            }
            done.store(true, Ordering::Release);
        })
    };
    let metrics = {
        let done = metrics_done.clone();
        tokio::task::spawn_blocking(move || {
            let _ = providers.meter.shutdown();
            done.store(true, Ordering::Release);
        })
    };
    let started = std::time::Instant::now();
    if tokio::time::timeout(FLUSH_BOUND, async {
        let _ = tokio::join!(traces, metrics);
    })
    .await
    .is_err()
    {
        // The bound is the contract: exit is never delayed, so whatever was still in flight is lost.
        // Say so, because a connector that exported nothing is otherwise indistinguishable from one
        // that had nothing to export.
        eprintln!(
            "telemetry: flush exceeded {}ms after {}ms; still pending: traces={} metrics={}",
            FLUSH_BOUND.as_millis(),
            started.elapsed().as_millis(),
            !traces_done.load(Ordering::Acquire),
            !metrics_done.load(Ordering::Acquire),
        );
    }
}

struct HeaderExtractor<'a>(&'a http::HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}

/// The W3C trace context carried in a request's headers, or an empty context when there is none.
/// Reads the request's own headers rather than tonic's `MetadataMap` because the span is made by a
/// tower layer, one level below the gRPC codec. Uses the propagator directly (not the global one)
/// so it is deterministic before [`start`] ran.
pub(crate) fn extract_parent(headers: &http::HeaderMap) -> opentelemetry::Context {
    TraceContextPropagator::new().extract(&HeaderExtractor(headers))
}

/// The span name for a request path, or `None` for the paths that get none: anything outside this
/// process's one gRPC service (tonic answers those itself, no handler ever runs) and
/// [`UNTRACED_RPC`].
fn span_name(path: &str) -> Option<String> {
    let rpc = path.strip_prefix(SERVICE_PATH_PREFIX)?;
    if rpc.is_empty() || rpc == UNTRACED_RPC {
        return None;
    }
    Some(format!("pcp.{rpc}"))
}

/// One `pcp.<Rpc>` server span per control-plane request, parented on the request's `traceparent`
/// and tagged `pz.instance` with the host's instance id (the `instance_id` sent with `Configure`:
/// a connection name when the host threaded one in, else `<connector>#<n>`) once `Configure` has
/// run. Applied as a tower layer so the
/// span wraps the whole RPC future, streaming responses included, with no per-handler code.
#[derive(Clone)]
pub(crate) struct PcpMakeSpan {
    pub(crate) instance: Arc<Mutex<Option<String>>>,
}

impl<B> tower_http::trace::MakeSpan<B> for PcpMakeSpan {
    fn make_span(&mut self, request: &http::Request<B>) -> Span {
        if !traces_enabled() {
            return Span::none();
        }
        let Some(name) = span_name(request.uri().path()) else {
            return Span::none();
        };
        let span = tracing::info_span!(
            "pcp",
            otel.name = %name,
            otel.kind = "server",
            pz.instance = tracing::field::Empty
        );
        // Recorded only once `Configure` has named the instance: the RPCs that run before it
        // (`Validate`, `CheckConnection` under `pz connector test`) carry no instance, and an empty
        // string is a worse answer than an absent attribute.
        if let Some(instance) = self.instance.lock().unwrap().as_deref() {
            if !instance.is_empty() {
                span.record("pz.instance", instance);
            }
        }
        span.set_parent(extract_parent(request.headers()));
        span
    }
}

pub(crate) type RpcTraceLayer =
    TraceLayer<SharedClassifier<GrpcErrorsAsFailures>, PcpMakeSpan, (), (), (), (), ()>;

pub(crate) fn rpc_layer(instance: Arc<Mutex<Option<String>>>) -> RpcTraceLayer {
    TraceLayer::new_for_grpc()
        .make_span_with(PcpMakeSpan { instance })
        .on_request(())
        .on_response(())
        .on_body_chunk(())
        .on_eos(())
        .on_failure(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceContextExt;

    #[test]
    fn a_traceparent_header_becomes_the_parent_context() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "traceparent",
            http::HeaderValue::from_static(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            ),
        );

        let cx = extract_parent(&headers);
        let span = cx.span().span_context().clone();

        assert!(span.is_valid());
        assert_eq!(
            span.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(span.span_id().to_string(), "b7ad6b7169203331");
        assert!(span.is_sampled());
    }

    #[test]
    fn no_header_means_an_invalid_empty_context() {
        let cx = extract_parent(&http::HeaderMap::new());
        assert!(!cx.span().span_context().is_valid());
    }

    #[test]
    fn an_rpc_path_becomes_a_pcp_span_name() {
        assert_eq!(
            span_name("/pz.connector.v1.PzConnector/BeginWrite").as_deref(),
            Some("pcp.BeginWrite")
        );
        assert_eq!(
            span_name("/pz.connector.v1.PzConnector/Handshake").as_deref(),
            Some("pcp.Handshake")
        );
    }

    #[test]
    fn the_paths_that_get_no_span_are_excluded() {
        // Lives as long as the process, so a span around it would measure the process.
        assert_eq!(span_name("/pz.connector.v1.PzConnector/HostChannel"), None);
        // Never routed to a handler: tonic answers an unknown path itself.
        assert_eq!(span_name("/grpc.health.v1.Health/Check"), None);
        assert_eq!(span_name("/pz.connector.v1.PzConnector/"), None);
        assert_eq!(span_name(""), None);
        assert_eq!(span_name("/"), None);
    }

    /// One test, not four, because `PROVIDERS` and the `tracing` global dispatcher are per-process
    /// and xunit-style parallel tests would race on them: the whole ordered story of what `start`
    /// installs -- and what it refuses to install -- has to be told in a single test body.
    #[tokio::test]
    async fn start_reports_a_bad_endpoint_and_a_pre_installed_subscriber() {
        assert!(start("not-a-url", "x", "0", "").is_err());
        assert!(PROVIDERS.get().is_none());
        assert!(!traces_enabled());

        // A subscriber the "connector author" installed before serve_sink ever ran.
        tracing::subscriber::set_global_default(tracing_subscriber::registry())
            .expect("no other test installs a global subscriber");

        let err = start("http://127.0.0.1:1", "x", "0", "run-1")
            .expect_err("an already-installed subscriber must be reported, not swallowed");
        assert!(err.contains("already installed"), "unexpected error: {err}");

        // Metrics survive (they need no subscriber); traces are off for good, so no span is built.
        let providers = PROVIDERS.get().expect("the meter provider is still stored");
        assert!(providers.tracer.is_none());
        assert!(!traces_enabled());

        // Idempotent: a second call neither rebuilds anything nor reports the failure again.
        assert!(start("http://127.0.0.1:1", "x", "0", "run-1").is_ok());
        assert!(!traces_enabled());

        // With traces off, the layer opens nothing at all.
        let mut make = PcpMakeSpan {
            instance: Arc::new(Mutex::new(Some("inst".to_string()))),
        };
        let request = http::Request::builder()
            .uri("/pz.connector.v1.PzConnector/BeginWrite")
            .body(())
            .unwrap();
        assert!(
            tower_http::trace::MakeSpan::make_span(&mut make, &request).is_none(),
            "no span may be built when nothing can export it"
        );

        // The bounded flush is a no-op for traces and still returns for metrics.
        flush_and_shutdown().await;
    }
}
