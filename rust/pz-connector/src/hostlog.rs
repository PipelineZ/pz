//! Forwards `tracing` events to the host over the reverse channel as `LogEvent`s, mirroring what the
//! C# SDK's `HostLoggerProvider` does for a connector's injected `ILogger`. The host turns each one
//! into a `connector_log` run event.
//!
//! **Secret hygiene, by construction**: nothing in this module ever touches [`crate::Config`] or any
//! other connector-configured value -- the bridge only ever carries what a `tracing::info!`/`warn!`/
//! `error!` call at a connector author's own call site puts into `message`/`fields`, exactly the same
//! posture the C# doc states for `HostLoggerProvider` ("the host renders message and fields verbatim,
//! which is why nothing here inspects or redacts: what a connector logs is what the operator sees").
//! A configured value can only reach the host through this bridge if a connector author puts it there
//! explicitly, the same responsibility any other logging call carries.
//!
//! **Composition.** [`crate::serve_sink`] wires this in automatically for a connector with no
//! `tracing` subscriber of its own (the common case). A connector that installs its own subscriber
//! before calling `serve_sink` (the same shape `--own-subscriber` takes for [`crate::layer`]) must
//! compose [`log_layer`] into it too, or its logs never reach the host -- the handshake does not
//! report this gap on stderr the way a missing [`crate::layer`] composition does, since a connector
//! with its own subscriber may deliberately not want host-side log forwarding.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use tokio::sync::mpsc;
use tonic::Status;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::pb;

/// Mirrored from `HostChannelPeer.LogBacklogCapacity` (C# SDK): bounded so a connector that logs
/// heavily while the host's `HostChannel` pump is still dialing (or has dropped and not yet
/// reconnected) cannot grow this process's memory without bound. Newest wins, oldest dropped first.
const LOG_BACKLOG_CAPACITY: usize = 256;

/// How many `HostChannelUp` messages the outbound stream a `HostChannel` call serves may hold before
/// a send blocks. Large enough that an ordinary burst of connector logging never falls back to the
/// backlog while a channel is genuinely attached and being drained by the host's pump.
pub(crate) const HOST_CHANNEL_BUFFER: usize = 64;

/// `Microsoft.Extensions.Logging.LogLevel`'s ordinals, which `LogEvent.level` and the host's
/// `connector_log` event both key off. `tracing` has no level distinct from `Critical` (5) -- ERROR is
/// the ceiling this bridge can produce, the same gap every non-.NET tracing/log crate has against that
/// enum.
fn level_ordinal(level: &Level) -> i32 {
    match *level {
        Level::TRACE => 0,
        Level::DEBUG => 1,
        Level::INFO => 2,
        Level::WARN => 3,
        Level::ERROR => 4,
    }
}

/// Extracts a `tracing::Event`'s conventional `message` field and every other field into the
/// (message, fields) shape `LogEvent` carries. A field value is rendered with `{:?}` except the
/// `message` field itself, whose value is `std::fmt::Arguments` -- `{value:?}` on it yields the same
/// resolved text `{}` would (`Arguments` implements `Debug` as `Display`), so `tracing::info!("x={x}")`
/// crosses as the plain formatted string, never a quoted debug rendering.
#[derive(Default)]
struct FieldVisitor {
    message: String,
    fields: HashMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let rendered = format!("{value:?}");
        if field.name() == "message" {
            self.message = rendered;
        } else {
            self.fields.insert(field.name().to_string(), rendered);
        }
    }
}

/// Excludes the same transport crates [`crate::telemetry`]'s span filter does, for the same reason --
/// h2/hyper/tonic/tower emit their own trace/debug-level events, and forwarding those would flood the
/// host's `connector_log` stream with this SDK's own wire chatter instead of the connector's. Unlike
/// the span filter (which defaults to DEBUG to bound span volume), this defaults to TRACE: a `LogEvent`
/// is one send per call, not a span-per-frame, and the host is meant to see everything a connector
/// author chose to log, mirroring the C# SDK's `HostLoggerProvider.IsEnabled` ("enabled unless None").
fn log_filter() -> Targets {
    Targets::new()
        .with_default(LevelFilter::TRACE)
        .with_target("h2", LevelFilter::OFF)
        .with_target("hyper", LevelFilter::OFF)
        .with_target("hyper_util", LevelFilter::OFF)
        .with_target("tonic", LevelFilter::OFF)
        .with_target("tower", LevelFilter::OFF)
        .with_target("opentelemetry", LevelFilter::OFF)
        .with_target("opentelemetry_sdk", LevelFilter::OFF)
        .with_target("opentelemetry-otlp", LevelFilter::OFF)
        .with_target("opentelemetry_otlp", LevelFilter::OFF)
}

struct PeerState {
    attached: Option<mpsc::Sender<Result<pb::HostChannelUp, Status>>>,
    backlog: VecDeque<pb::LogEvent>,
}

/// The connector-process side of the log half of the reverse channel: one instance per process,
/// reused across however many `HostChannel` calls the host makes (normally exactly one, held open for
/// the process's lifetime). Mirrors the C# SDK's `HostChannelPeer`, minus the gate traffic (not yet
/// wired in the Rust SDK -- see this crate's `server::host_channel`).
pub(crate) struct HostLogPeer {
    state: StdMutex<PeerState>,
}

impl HostLogPeer {
    pub(crate) fn new() -> Self {
        HostLogPeer {
            state: StdMutex::new(PeerState {
                attached: None,
                backlog: VecDeque::new(),
            }),
        }
    }

    /// Called once per `HostChannel` RPC, right as it starts serving. Flushes the backlog first,
    /// oldest event first, so nothing queued before this attach is reordered behind anything queued
    /// after it. Best-effort: a send that fails (the outbound stream is already gone) leaves the event
    /// in the backlog for the next attach rather than losing it.
    pub(crate) fn attach(&self, tx: mpsc::Sender<Result<pb::HostChannelUp, Status>>) {
        let mut state = self.state.lock().unwrap();
        while let Some(log) = state.backlog.pop_front() {
            if tx.try_send(Ok(up(log.clone()))).is_err() {
                state.backlog.push_front(log);
                break;
            }
        }
        state.attached = Some(tx);
    }

    /// Called when the `HostChannel` call serving `attach`'s sender ends, whatever the reason (host
    /// closed it, this process is shutting down). Queued events after this point fall back to the
    /// backlog until the next attach, the same as before the first `HostChannel` call ever arrived.
    pub(crate) fn detach(&self) {
        self.state.lock().unwrap().attached = None;
    }

    /// Best-effort, in order: sent immediately when a channel is attached and has room, otherwise held
    /// (bounded, newest wins) until the next [`Self::attach`] flushes it ahead of anything newer.
    pub(crate) fn queue_log(&self, log: pb::LogEvent) {
        let mut state = self.state.lock().unwrap();
        let rejected = match state.attached.as_ref() {
            Some(tx) => tx.try_send(Ok(up(log.clone()))).is_err(),
            None => true,
        };
        if rejected {
            if state.backlog.len() == LOG_BACKLOG_CAPACITY {
                state.backlog.pop_front();
            }
            state.backlog.push_back(log);
        }
    }
}

fn up(log: pb::LogEvent) -> pb::HostChannelUp {
    pb::HostChannelUp {
        msg: Some(pb::host_channel_up::Msg::Log(log)),
    }
}

/// A `tracing` layer that forwards every event to a [`HostLogPeer`] filled in later -- the peer is
/// built inside `serve_sink_inner`, after a connector author's own subscriber (composing [`log_layer`])
/// would already have been installed, so the same deferred-OnceLock shape [`crate::layer`] uses for
/// its tracer applies here too.
struct HostLogLayer<S> {
    inner: Arc<OnceLock<Arc<HostLogPeer>>>,
    _marker: std::marker::PhantomData<S>,
}

impl<S> Layer<S> for HostLogLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let Some(peer) = self.inner.get() else {
            return;
        };
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut fields = visitor.fields;
        fields.insert("target".to_string(), event.metadata().target().to_string());
        peer.queue_log(pb::LogEvent {
            level: level_ordinal(event.metadata().level()),
            message: visitor.message,
            fields,
        });
    }
}

/// Fills whichever [`HostLogLayer`] a call to [`log_layer`] (a connector author's own, or this
/// crate's own self-install) most recently registered -- a no-op if neither ever ran.
type LogInstaller = Box<dyn Fn(Arc<HostLogPeer>) -> Result<(), String> + Send + Sync>;

static LOG_LAYER_INSTALLER: StdMutex<Option<LogInstaller>> = StdMutex::new(None);

/// A `tracing` layer for a connector that installs its own subscriber. Compose it into that
/// subscriber before calling [`crate::serve_sink`], the same shape `--own-subscriber` in
/// `examples/memory_sink.rs` takes for [`crate::layer`]; it forwards nothing until `serve_sink` fills
/// in the peer, then every event reaches the host exactly as this module's own doc describes.
///
/// ```no_run
/// use tracing_subscriber::layer::SubscriberExt;
///
/// tracing::subscriber::set_global_default(
///     tracing_subscriber::registry()
///         .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
///         .with(pz_connector::log_layer()),
/// )
/// .expect("first subscriber in the process");
/// ```
pub fn log_layer<S>() -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync + 'static,
{
    let (layer, installer) = build_deferred_layer();
    let mut slot = LOG_LAYER_INSTALLER.lock().unwrap();
    if slot.is_none() {
        *slot = Some(installer);
    }
    layer
}

/// The same deferred layer [`log_layer`] builds, minus the eager registration -- used only by
/// `serve_sink`'s own early self-install attempt. See `telemetry::build_deferred_layer`'s doc for why
/// eager registration before a `set_global_default` attempt is known to have won would be wrong here.
pub(crate) fn build_deferred_layer<S>() -> (impl Layer<S>, LogInstaller)
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync + 'static,
{
    let inner: Arc<OnceLock<Arc<HostLogPeer>>> = Arc::new(OnceLock::new());
    let installer: LogInstaller = {
        let inner = inner.clone();
        Box::new(move |peer| {
            inner
                .set(peer)
                .map_err(|_| "the deferred host-log layer was already installed".to_string())
        })
    };
    (
        HostLogLayer {
            inner,
            _marker: std::marker::PhantomData,
        }
        .with_filter(log_filter()),
        installer,
    )
}

pub(crate) fn register_if_empty(installer: LogInstaller) {
    let mut slot = LOG_LAYER_INSTALLER.lock().unwrap();
    if slot.is_none() {
        *slot = Some(installer);
    }
}

/// Fills whichever [`HostLogLayer`] ended up registered -- a connector author's own (via
/// [`log_layer`]), or this crate's own self-install -- with the real peer. A no-op if neither ever
/// registered (some other subscriber already owned the process's global-default slot before either
/// attempt ran).
pub(crate) fn install(peer: Arc<HostLogPeer>) {
    let installer = LOG_LAYER_INSTALLER.lock().unwrap().take();
    if let Some(install) = installer {
        let _ = install(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_event(message: &str) -> pb::LogEvent {
        pb::LogEvent {
            level: 2,
            message: message.to_string(),
            fields: HashMap::new(),
        }
    }

    fn extract(up: pb::HostChannelUp) -> pb::LogEvent {
        match up.msg {
            Some(pb::host_channel_up::Msg::Log(log)) => log,
            other => panic!("expected a Log message, got {other:?}"),
        }
    }

    #[test]
    fn level_ordinals_match_microsoft_extensions_logging() {
        assert_eq!(level_ordinal(&Level::TRACE), 0);
        assert_eq!(level_ordinal(&Level::DEBUG), 1);
        assert_eq!(level_ordinal(&Level::INFO), 2);
        assert_eq!(level_ordinal(&Level::WARN), 3);
        assert_eq!(level_ordinal(&Level::ERROR), 4);
    }

    #[tokio::test]
    async fn a_log_queued_before_any_attach_is_held_in_the_backlog() {
        let peer = HostLogPeer::new();
        peer.queue_log(log_event("configured"));

        let (tx, mut rx) = mpsc::channel(HOST_CHANNEL_BUFFER);
        peer.attach(tx);

        let received = extract(rx.recv().await.unwrap().unwrap());
        assert_eq!(received.message, "configured");
    }

    #[tokio::test]
    async fn backlog_flushes_oldest_first_ahead_of_a_freshly_queued_log() {
        let peer = HostLogPeer::new();
        peer.queue_log(log_event("first"));
        peer.queue_log(log_event("second"));

        let (tx, mut rx) = mpsc::channel(HOST_CHANNEL_BUFFER);
        peer.attach(tx);
        peer.queue_log(log_event("third"));

        assert_eq!(extract(rx.recv().await.unwrap().unwrap()).message, "first");
        assert_eq!(extract(rx.recv().await.unwrap().unwrap()).message, "second");
        assert_eq!(extract(rx.recv().await.unwrap().unwrap()).message, "third");
    }

    #[tokio::test]
    async fn a_log_queued_while_attached_is_sent_immediately() {
        let peer = HostLogPeer::new();
        let (tx, mut rx) = mpsc::channel(HOST_CHANNEL_BUFFER);
        peer.attach(tx);

        peer.queue_log(log_event("live"));

        assert_eq!(extract(rx.recv().await.unwrap().unwrap()).message, "live");
    }

    #[test]
    fn detach_falls_back_to_the_backlog_until_the_next_attach() {
        let peer = HostLogPeer::new();
        let (tx, rx) = mpsc::channel(HOST_CHANNEL_BUFFER);
        peer.attach(tx);
        peer.detach();
        drop(rx);

        peer.queue_log(log_event("after detach"));

        let state = peer.state.lock().unwrap();
        assert!(state.attached.is_none());
        assert_eq!(state.backlog.len(), 1);
        assert_eq!(state.backlog[0].message, "after detach");
    }

    #[test]
    fn the_backlog_drops_the_oldest_entry_once_full() {
        let peer = HostLogPeer::new();
        for i in 0..LOG_BACKLOG_CAPACITY + 1 {
            peer.queue_log(log_event(&i.to_string()));
        }

        let state = peer.state.lock().unwrap();
        assert_eq!(state.backlog.len(), LOG_BACKLOG_CAPACITY);
        assert_eq!(state.backlog.front().unwrap().message, "1");
        assert_eq!(
            state.backlog.back().unwrap().message,
            LOG_BACKLOG_CAPACITY.to_string()
        );
    }

    /// End to end through a real [`HostLogLayer`] (not just [`FieldVisitor`] in isolation): a plain
    /// `tracing::info!` call's formatted message crosses as plain text (never a quoted debug
    /// rendering), its extra field crosses verbatim, and `target` names the call site's module --
    /// proving `build_deferred_layer`'s installer actually wires a peer a real `tracing` dispatch can
    /// reach. Uses `tracing::subscriber::with_default` (thread-local) rather than the process-global
    /// default, so this test cannot race the static `LOG_LAYER_INSTALLER` other tests may touch.
    #[tokio::test]
    async fn a_real_tracing_event_forwards_through_a_deferred_layer_to_the_peer() {
        use tracing_subscriber::layer::SubscriberExt;

        let peer = Arc::new(HostLogPeer::new());
        let (layer, installer): (_, LogInstaller) = build_deferred_layer();
        installer(peer.clone()).expect("the freshly built OnceLock is empty");

        let (tx, mut rx) = mpsc::channel(HOST_CHANNEL_BUFFER);
        peer.attach(tx);

        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(user_supplied_field = "value", "hello {}", "world");
        });

        let received = extract(rx.recv().await.unwrap().unwrap());
        assert_eq!(received.message, "hello world");
        assert_eq!(received.level, 2);
        assert_eq!(
            received
                .fields
                .get("user_supplied_field")
                .map(String::as_str),
            Some("value")
        );
        assert!(received.fields.contains_key("target"));
    }
}
