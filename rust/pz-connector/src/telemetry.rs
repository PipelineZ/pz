//! The connector process's own OpenTelemetry composition root. Providers exist only after the host's
//! `HostInfo.otel_endpoint` arrived in `Handshake`; until then no subscriber is installed and every
//! `tracing` span is disabled. Trace context comes in as W3C `traceparent` metadata on every RPC;
//! the data plane carries none, so a write stream inherits the `BeginWrite` RPC's context through
//! its `SessionState`.

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
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Layer, SubscriberExt};

/// Upper bound on flushing at shutdown: inside the host's ten-second shutdown grace.
pub(crate) const FLUSH_BOUND: Duration = Duration::from_secs(3);
const EXPORT_TIMEOUT: Duration = Duration::from_secs(2);
const METER_NAME: &str = "pz.connector";

struct Providers {
    tracer: TracerProvider,
    meter: SdkMeterProvider,
}

static PROVIDERS: OnceLock<Providers> = OnceLock::new();

/// The meter a connector author records on. Backed by the global provider [`start`] installs; with no
/// endpoint the global default is a no-op provider, so this is always safe to call.
pub fn meter() -> opentelemetry::metrics::Meter {
    global::meter(METER_NAME)
}

/// Builds and installs providers once. A malformed endpoint or a second call changes nothing: the
/// host validated the endpoint, and a connector must not fail its handshake over telemetry.
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
    // INFO, not the registry's "everything": the transport stack under this process (h2, hyper,
    // tonic) opens a TRACE-level span per HTTP/2 frame, and an unfiltered layer exports every one of
    // them as an OTLP span -- hundreds of `queue_frame`/`FramedWrite::buffer` spans drowning the
    // handful of `pcp.*` ones the host actually asked for. The SDK's own spans, and any a connector
    // author opens at INFO or above, are the ones that survive this cut.
    let layer = tracing_opentelemetry::layer()
        .with_tracer(tracer.tracer(METER_NAME))
        .with_filter(LevelFilter::INFO);
    // Ignored if the connector author installed their own subscriber first: theirs wins, and the
    // OpenTelemetry layer is simply absent.
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer));
    let _ = PROVIDERS.set(Providers { tracer, meter });
    Ok(())
}

/// Flush and tear down, bounded by [`FLUSH_BOUND`]. Late spans are dropped rather than delaying exit.
pub(crate) async fn flush_and_shutdown() {
    let Some(providers) = PROVIDERS.get() else {
        return;
    };
    let work = tokio::task::spawn_blocking(|| {
        for result in providers.tracer.force_flush() {
            let _ = result;
        }
        let _ = providers.tracer.shutdown();
        let _ = providers.meter.force_flush();
        let _ = providers.meter.shutdown();
    });
    let _ = tokio::time::timeout(FLUSH_BOUND, work).await;
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

/// One `pcp.<Rpc>` server span per control-plane request, parented on the request's `traceparent`
/// and tagged with the connection name once `Configure` has run. `HostChannel` gets no span: it
/// lives as long as the process. Applied as a tower layer so the span wraps the whole RPC future,
/// streaming responses included, with no per-handler code.
#[derive(Clone)]
pub(crate) struct PcpMakeSpan {
    pub(crate) instance: Arc<Mutex<Option<String>>>,
}

impl<B> tower_http::trace::MakeSpan<B> for PcpMakeSpan {
    fn make_span(&mut self, request: &http::Request<B>) -> Span {
        if PROVIDERS.get().is_none() {
            return Span::none();
        }
        let rpc = request.uri().path().rsplit('/').next().unwrap_or("");
        if rpc == "HostChannel" || rpc.is_empty() {
            return Span::none();
        }
        let name = format!("pcp.{rpc}");
        let instance = self.instance.lock().unwrap().clone().unwrap_or_default();
        let span = tracing::info_span!(
            "pcp",
            otel.name = %name,
            otel.kind = "server",
            pz.instance = %instance
        );
        let parent = extract_parent(request.headers());
        span.set_parent(parent);
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
    fn a_non_http_endpoint_is_refused_without_installing_anything() {
        assert!(start("not-a-url", "x", "0", "").is_err());
        assert!(PROVIDERS.get().is_none());
    }
}
