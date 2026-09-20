use std::collections::HashMap;
use std::io;
use std::net::Shutdown;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{oneshot, Mutex as AsyncMutex};
use tokio_stream::Stream;
use tonic::transport::server::Connected;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

use crate::config::Config;
use crate::data_plane;
use crate::error::{to_error_detail, to_status, PzError};
use crate::hostlog;
use crate::pb;
use crate::pb::pz_connector_server::{PzConnector, PzConnectorServer};
use crate::telemetry;
use crate::ticket::{TicketEntry, TicketRegistry, TICKET_LENGTH};

/// Protocol major this SDK speaks, mirrored from `Pz.Connectors.Abstractions.ProtocolVersion.Major`.
const PROTOCOL_MAJOR: i32 = 1;
/// The one transport v1 defines: Arrow IPC over UDS/named pipe, mirrored from
/// `Pz.Connectors.Protocol.ProtocolConstants.TransportPipe`.
const TRANSPORT_PIPE: &str = "pipe";
/// Mirrored from `Pz.Connectors.Protocol.ProtocolConstants.DataSocketSuffix`.
const DATA_SOCKET_SUFFIX: &str = ".data";

// ---------------------------------------------------------------------------------------------
// Connector-author-facing surface
// ---------------------------------------------------------------------------------------------

/// What a connector declares in its `Hello`: identity, the `ConnectorCapabilities` flag bits (same
/// values the host ABI defines), and the JSON Schema strings the host surfaces to authoring tools.
pub struct ConnectorDecl {
    pub name: &'static str,
    pub version: &'static str,
    pub capabilities: u64,
    pub connection_config_schema: &'static str,
    pub dataset_config_schema: &'static str,
    /// JSON Schema for this sink's own write() options (`IOutputConfigSchema` on the C# ABI) -- empty
    /// means tier 3 leaves this connector's output options unchecked, the same as a connector built
    /// before this field existed. This crate is sink-first (see `PzConnector` impl below), so this is
    /// the schema most Rust connectors actually want to fill in.
    pub output_config_schema: &'static str,
}

/// One committed write's identity, mirroring `WriteAttemptMsg`: which node, which run, and which
/// attempt ordinal produced this write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAttempt {
    pub node: String,
    pub run: String,
    pub ordinal: i32,
}

/// Every `OutputSpecMsg` field, in idiomatic Rust types.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputSpec {
    pub sink: String,
    pub output: String,
    pub mode: String,
    pub schema_policy: String,
    pub options: serde_json::Map<String, serde_json::Value>,
    pub keys: Vec<String>,
    pub on_delete: Option<String>,
    /// `None` and `Some(<empty map>)` are distinct on the wire (`max_text_lengths_set` disambiguates a
    /// null map from an empty one) -- kept distinct here rather than collapsed.
    pub max_text_lengths: Option<HashMap<String, i64>>,
    pub attempt: Option<WriteAttempt>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteResult {
    pub rows_written: i64,
    pub batches_written: i64,
}

/// What [`Sink::abort_semantics`] declares this sink's [`WriteSession::abort`] actually achieves.
/// Mirrors `Pz.Connectors.Abstractions.AbortSemantics` by ordinal (`DiscardsAll`=0/`BestEffort`=1/
/// `None`=2) -- the engine surfaces it in run artifacts on a non-`DiscardsAll` write failure, so a
/// non-transactional sink never claims cleanup that did not happen.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AbortSemantics {
    /// Abort removes every trace of the session's writes (temp-write + discard). The contract for an
    /// owned destination, and this trait's default.
    #[default]
    DiscardsAll,
    /// Abort attempts cleanup but cannot guarantee it; some written data may remain visible
    /// downstream.
    BestEffort,
    /// Abort cleans up nothing: every delivered row is already visible downstream (destinations with
    /// side effects -- you cannot un-POST).
    None,
}

fn to_abort_semantics_msg(semantics: AbortSemantics) -> pb::AbortSemanticsMsg {
    match semantics {
        AbortSemantics::DiscardsAll => pb::AbortSemanticsMsg::AbortSemanticsDiscardsAll,
        AbortSemantics::BestEffort => pb::AbortSemanticsMsg::AbortSemanticsBestEffort,
        AbortSemantics::None => pb::AbortSemanticsMsg::AbortSemanticsNone,
    }
}

/// One `(temp_path, final_path)` finalization the host performs after its own copy commits.
pub type FileMove = (String, String);

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NativeCopy {
    pub copy_sql: String,
    pub setup_statements: Vec<String>,
    pub mechanism: Option<String>,
    pub finalizations: Vec<FileMove>,
}

#[async_trait]
pub trait SinkConnector: Send + Sync + 'static {
    /// Aggregate validation errors -- never a single throw-per-error; an empty result means the config
    /// is acceptable as far as this connector can tell offline.
    async fn validate(&self, config: &Config) -> Vec<String>;
    /// Aggregate warnings about a config this connector still accepts -- never fails validation, only
    /// said about it. Additive: a connector written before this method existed inherits the empty
    /// default, exactly as the host treats an absent `ValidationResultMsg.warnings` list.
    async fn validate_warnings(&self, _config: &Config) -> Vec<String> {
        Vec::new()
    }
    /// Network-touching connectivity check. `Ok(())` reports `ConnectionCheckMsg { ok: true }`;
    /// `Err(e)` reports `ConnectionCheckMsg { ok: false, message: Some(e.message) }` -- an ordinary
    /// "no" for this probe, not a protocol-level error. Unlike a `PzError` from `open`/`begin_write`,
    /// one from this method never crosses as a `pz-error-bin` trailer.
    async fn check(&self, config: &Config) -> Result<(), PzError>;
    async fn open(&self, config: Config) -> Result<Box<dyn Sink>, PzError>;
    /// Sink-level, before any session opens: lets DuckDB take over the write entirely instead of
    /// draining Arrow batches through this process. `None` (the default) means "no native path" --
    /// every write then goes through `Sink::begin_write`.
    fn try_native_copy(&self, _spec: &OutputSpec) -> Option<NativeCopy> {
        None
    }
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn begin_write(
        &self,
        spec: OutputSpec,
        schema: SchemaRef,
    ) -> Result<Box<dyn WriteSession>, PzError>;

    /// Abort semantics for sessions this sink opens. Mirrors `Pz.Connectors.Abstractions.
    /// ISink.AbortSemantics`: additive (a defaulted trait method), so a sink written before this
    /// method existed keeps declaring [`AbortSemantics::DiscardsAll`] without any source change.
    fn abort_semantics(&self) -> AbortSemantics {
        AbortSemantics::DiscardsAll
    }
}

#[async_trait]
pub trait WriteSession: Send {
    /// Batches are handed in one at a time, in wire order, and never retained past the call -- the same
    /// "engine owns the batch until this call returns" rule the in-process ABI states.
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), PzError>;
    async fn commit(&mut self) -> Result<WriteResult, PzError>;
    async fn abort(&mut self) -> Result<(), PzError>;
}

// ---------------------------------------------------------------------------------------------
// Process exit shape
// ---------------------------------------------------------------------------------------------

/// Why [`serve_sink`] stopped. `serve_sink` always returns an `anyhow::Error` (never a success value --
/// there is no meaningful "Ok" outcome for a function whose entire job is to run until told to stop), so
/// this is downcast-able from the returned error to tell a usage/setup failure (exit before a single RPC
/// was ever served) apart from a normal stop (Shutdown RPC received, control connection closed).
#[derive(Debug, thiserror::Error)]
pub enum ServeExit {
    /// argv/socket-setup failed before either socket was ever served -- mirrors the CLI convention of
    /// exit code 2 for "config/usage problem", never mixed with a protocol-level failure.
    #[error("{0}")]
    UsageError(String),
    /// Serving ended normally.
    #[error("{0}")]
    Stopped(String),
}

// ---------------------------------------------------------------------------------------------
// Session bookkeeping (SDK-internal)
// ---------------------------------------------------------------------------------------------

/// One open sink write session, reachable from the control plane by session id (`sessions` map) and
/// from the data plane by ticket (`TicketRegistry`).
///
/// `drained` is the ordering primitive `CommitWrite` depends on: the write's data stream must be read
/// to end-of-stream before a commit may run, or a commit could land a prefix of the rows the host
/// believes it sent. The data-plane pump signals it exactly once, however the stream ended (cleanly or
/// not); `CommitWrite` awaits it before ever touching `write_session`.
pub(crate) struct SessionState {
    pub(crate) op_id: String,
    /// The single-use data-plane ticket minted for this session, recorded here (not just handed to the
    /// host) so `abort_write` can revoke it -- see `TicketRegistry::revoke`'s doc for why a session
    /// aborted before its ticket is ever presented would otherwise leave that ticket live forever.
    ticket: [u8; TICKET_LENGTH],
    write_session: AsyncMutex<Option<Box<dyn WriteSession>>>,
    drained_tx: StdMutex<Option<oneshot::Sender<Result<(), PzError>>>>,
    drained_rx: AsyncMutex<Option<oneshot::Receiver<Result<(), PzError>>>>,
    /// Why the drain failed, kept for `GetStreamFailure`. The oneshot above is consumed by the one
    /// `CommitWrite` that awaits it, but the host asks this AFTER the data stream tore and never
    /// commits a torn stream -- so the failure has to survive here, readable any number of times, or
    /// a transient error the sink raised mid-write would reach the host as nothing but a hang-up.
    drain_failure: StdMutex<Option<PzError>>,
    /// A clone of the connected data-plane socket, attached once the pump claims it. `AbortWrite`/
    /// `Cancel`/a `Shutdown`-triggered sweep use it to force a blocking read to fail, unblocking a pump
    /// that would otherwise wait forever for bytes the host is never going to send.
    data_conn: StdMutex<Option<StdUnixStream>>,
    /// The `BeginWrite` RPC's trace context, captured when the session was created: the data plane
    /// carries no headers, so the write stream served later is parented here.
    pub(crate) parent: opentelemetry::Context,
}

impl SessionState {
    fn new(
        op_id: String,
        ticket: [u8; TICKET_LENGTH],
        session: Box<dyn WriteSession>,
        parent: opentelemetry::Context,
    ) -> Arc<Self> {
        let (tx, rx) = oneshot::channel();
        Arc::new(SessionState {
            op_id,
            ticket,
            write_session: AsyncMutex::new(Some(session)),
            drained_tx: StdMutex::new(Some(tx)),
            drained_rx: AsyncMutex::new(Some(rx)),
            drain_failure: StdMutex::new(None),
            data_conn: StdMutex::new(None),
            parent,
        })
    }

    pub(crate) fn ticket(&self) -> [u8; TICKET_LENGTH] {
        self.ticket
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Arc<Self> {
        struct NullSession;
        #[async_trait]
        impl WriteSession for NullSession {
            async fn write_batch(&mut self, _batch: RecordBatch) -> Result<(), PzError> {
                Ok(())
            }

            async fn commit(&mut self) -> Result<WriteResult, PzError> {
                Ok(WriteResult::default())
            }

            async fn abort(&mut self) -> Result<(), PzError> {
                Ok(())
            }
        }

        SessionState::new(
            "test-op".to_string(),
            TicketRegistry::generate(),
            Box::new(NullSession),
            opentelemetry::Context::new(),
        )
    }

    /// Data plane: forwards one batch into the open session, or fails if the session was already
    /// finalized (committed/aborted) before this data connection ever arrived -- a race the control
    /// plane can win when a ticket is minted but its data connection is slow to open.
    pub(crate) async fn write_batch(&self, batch: RecordBatch) -> Result<(), PzError> {
        let mut guard = self.write_session.lock().await;
        match guard.as_mut() {
            Some(session) => session.write_batch(batch).await,
            None => Err(PzError::new(
                "write session was closed before its data stream opened",
            )),
        }
    }

    pub(crate) fn attach_data_conn(&self, conn: StdUnixStream) {
        *self.data_conn.lock().unwrap() = Some(conn);
    }

    /// Best-effort: shuts down a live data connection so a blocking read on it fails, unblocking a pump
    /// that is mid-drain. A pump that never connected has nothing to unblock; one that already finished
    /// has nothing left to shut down either way -- both are harmless no-ops.
    pub(crate) fn force_unblock(&self) {
        if let Some(conn) = self.data_conn.lock().unwrap().as_ref() {
            let _ = conn.shutdown(Shutdown::Both);
        }
    }

    /// Recorded before it is sent: the pump calls this before the data connection closes, so by the
    /// time the host notices the hang-up and asks `GetStreamFailure`, the answer already exists.
    pub(crate) fn signal_drained(&self, result: Result<(), PzError>) {
        if let Err(e) = &result {
            *self.drain_failure.lock().unwrap() = Some(e.clone());
        }
        if let Some(tx) = self.drained_tx.lock().unwrap().take() {
            let _ = tx.send(result);
        }
    }

    /// The failure the drain ended with, if it has ended and failed. Readable any number of times.
    pub(crate) fn drain_failure(&self) -> Option<PzError> {
        self.drain_failure.lock().unwrap().clone()
    }

    /// Taken exactly once, by the first `CommitWrite` attempt that reaches it. A `CommitWrite` call
    /// whose future is dropped before this resolves (client deadline, caller cancellation) leaves the
    /// receiver already taken -- a legitimate retry is out of scope for this SDK's v1 session model, the
    /// same way the reference host-side fixture treats a torn commit attempt as unrecoverable for that
    /// attempt (the session stays abortable, just not re-committable).
    pub(crate) async fn take_drained_receiver(
        &self,
    ) -> Option<oneshot::Receiver<Result<(), PzError>>> {
        self.drained_rx.lock().await.take()
    }

    pub(crate) async fn take_session(&self) -> Option<Box<dyn WriteSession>> {
        self.write_session.lock().await.take()
    }
}

// ---------------------------------------------------------------------------------------------
// The tonic service
// ---------------------------------------------------------------------------------------------

struct PzConnectorService<C: SinkConnector> {
    decl: ConnectorDecl,
    connector: C,
    config: StdMutex<Option<Config>>,
    /// Opened lazily, on the first call that needs a live sink (`TryNativeCopy` needs only the
    /// connector, but `BeginWrite` needs this) -- and cached, since `SinkConnector::open` is meant to
    /// run once per connector instance, exactly as the in-process ABI's `ISinkConnector.OpenAsync` does.
    sink: AsyncMutex<Option<Arc<dyn Sink>>>,
    /// `Arc`-wrapped (not owned outright) so `serve_sink_inner` can keep its own clone: once `Shutdown`
    /// stops the accept loop, it still needs to reach every live session to force-unblock its data
    /// connection -- see the doc on the shutdown sweep in `serve_sink_inner`.
    sessions: Arc<StdMutex<HashMap<String, Arc<SessionState>>>>,
    tickets: Arc<TicketRegistry>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// A `HostChannel` call is a long-lived bidirectional stream the host may hold open for this
    /// instance's entire lifetime -- `host_channel` races its read loop against this so a `Shutdown`
    /// RPC can actually end the connection instead of leaving an in-flight stream that graceful
    /// shutdown would otherwise wait on forever.
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    /// The configured connection's instance id, tagged onto every control-plane span. Shared with the
    /// tower layer that makes those spans, which runs before any handler and so cannot read `config`.
    instance: Arc<StdMutex<Option<String>>>,
    /// The connector-process side of the reverse channel's log half -- see `hostlog`'s own doc.
    /// `HostChannel` attaches/detaches it; a `tracing` layer this process installed queues into it.
    log_peer: Arc<hostlog::HostLogPeer>,
}

impl<C: SinkConnector> PzConnectorService<C> {
    async fn sink(&self) -> Result<Arc<dyn Sink>, PzError> {
        let mut guard = self.sink.lock().await;
        if let Some(sink) = guard.as_ref() {
            return Ok(sink.clone());
        }

        let config = self
            .config
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| PzError::new("connector is not configured; call Configure first"))?;
        let opened: Arc<dyn Sink> = Arc::from(self.connector.open(config).await?);
        *guard = Some(opened.clone());
        Ok(opened)
    }

    fn new_session_id() -> String {
        let mut bytes = [0u8; 16];
        getrandom::getrandom(&mut bytes).expect("system randomness source unavailable");
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

type PlanReadStream =
    Pin<Box<dyn Stream<Item = Result<pb::PartitionMsg, Status>> + Send + 'static>>;
type HostChannelStream =
    Pin<Box<dyn Stream<Item = Result<pb::HostChannelUp, Status>> + Send + 'static>>;

/// Every RPC this trait declares must be implemented -- the four source-direction ones
/// (`GetSchema`/`TryNativeScan`/`PlanRead`/`OpenReadStream`) return `Status::unimplemented`, since this
/// SDK is sink-first in v1 and the host only ever calls them for a source instance.
#[async_trait]
impl<C: SinkConnector> PzConnector for PzConnectorService<C> {
    async fn handshake(
        &self,
        request: Request<pb::HandshakeRequest>,
    ) -> Result<Response<pb::Hello>, Status> {
        let msg = request.into_inner();
        let host_major = msg.protocol_major;
        if host_major != PROTOCOL_MAJOR {
            // Refused here rather than silently answering with our own major and letting the host
            // discover the mismatch some other way: the host's own `PcpClient` already treats a
            // disagreeing `Hello.Info.ProtocolMajor` as a load error, but a connector that noticed the
            // SAME disagreement from its own side of the handshake should say so plainly too, not
            // pretend to be compatible.
            return Err(Status::failed_precondition(format!(
                "host speaks protocol major {host_major}, this connector (pz-connector Rust SDK) speaks major {PROTOCOL_MAJOR}"
            )));
        }

        // Built here, not at Configure, so Validate/CheckConnection under `pz connector test` are
        // covered. A telemetry failure is reported on stderr and never fails the handshake -- it can
        // mean traces are off while metrics still work, which is why it does not say "disabled".
        if let Some(host) = msg.host_info.as_ref() {
            if let Some(endpoint) = host.otel_endpoint.as_deref() {
                if let Err(e) =
                    telemetry::start(endpoint, self.decl.name, self.decl.version, &host.run_id)
                {
                    eprintln!("pz-connector: telemetry: {e}");
                }
            }
        }

        Ok(Response::new(hello_for(&self.decl)))
    }

    async fn configure(
        &self,
        request: Request<pb::ConfigureRequest>,
    ) -> Result<Response<pb::ConfigureResponse>, Status> {
        let msg = request.into_inner();
        let mut config = self.config.lock().unwrap();
        if config.is_some() {
            // Mirrors the C# SDK's `PcpConnectorService.Configure` guard: a connector process is
            // configured exactly once (its `ConnectorConfig` never changes mid-process), so a second
            // Configure is a protocol violation, not a silent re-point.
            return Err(Status::failed_precondition(
                "connector is already configured",
            ));
        }

        *self.instance.lock().unwrap() = Some(msg.instance_id.clone());
        *config = Some(Config::from_struct(msg.config.as_ref()));
        Ok(Response::new(pb::ConfigureResponse {}))
    }

    async fn validate(
        &self,
        request: Request<pb::ValidateRequest>,
    ) -> Result<Response<pb::ValidationResultMsg>, Status> {
        let config = Config::from_struct(request.into_inner().config.as_ref());
        if let Some(errors) = answer_numeric_probe(&config) {
            // Mirrors the C# SDK: the conformance probe never reaches the connector, so it never
            // reaches `validate_warnings` either.
            return Ok(Response::new(pb::ValidationResultMsg {
                errors,
                warnings: Vec::new(),
            }));
        }

        let errors = self.connector.validate(&config).await;
        let warnings = self.connector.validate_warnings(&config).await;
        Ok(Response::new(pb::ValidationResultMsg { errors, warnings }))
    }

    async fn check_connection(
        &self,
        request: Request<pb::CheckRequest>,
    ) -> Result<Response<pb::ConnectionCheckMsg>, Status> {
        let config = Config::from_struct(request.into_inner().config.as_ref());
        // A connectivity failure is an ordinary "no" for this probe, not a protocol-level error --
        // mirrors `PcpConnectorService.CheckConnection`, which never lets a failed `ConnectionCheck`
        // escape as an RpcException. `message` carries exactly what the connector's own `check()`
        // reported, verbatim: no extra wrapping that could add a socket path or endpoint the
        // connector chose not to include itself.
        Ok(Response::new(match self.connector.check(&config).await {
            Ok(()) => pb::ConnectionCheckMsg {
                ok: true,
                message: None,
            },
            Err(e) => pb::ConnectionCheckMsg {
                ok: false,
                message: Some(e.message),
            },
        }))
    }

    async fn get_schema(
        &self,
        _request: Request<pb::GetSchemaRequest>,
    ) -> Result<Response<pb::DatasetSchemaMsg>, Status> {
        Err(source_unimplemented())
    }

    async fn try_native_scan(
        &self,
        _request: Request<pb::NativeScanRequest>,
    ) -> Result<Response<pb::NativeScanResponse>, Status> {
        Err(source_unimplemented())
    }

    type PlanReadStream = PlanReadStream;

    async fn plan_read(
        &self,
        _request: Request<pb::PlanReadRequest>,
    ) -> Result<Response<Self::PlanReadStream>, Status> {
        Err(source_unimplemented())
    }

    async fn open_read_stream(
        &self,
        _request: Request<pb::OpenReadRequest>,
    ) -> Result<Response<pb::ReadStreamTicket>, Status> {
        Err(source_unimplemented())
    }

    async fn get_natural_read_shape(
        &self,
        _request: Request<pb::NaturalReadShapeRequest>,
    ) -> Result<Response<pb::NaturalReadShapeResponse>, Status> {
        Err(source_unimplemented())
    }

    async fn get_read_state(
        &self,
        _request: Request<pb::ReadStateRequest>,
    ) -> Result<Response<pb::ReadStateResponse>, Status> {
        Err(source_unimplemented())
    }

    async fn get_stream_failure(
        &self,
        request: Request<pb::StreamFailureRequest>,
    ) -> Result<Response<pb::StreamFailureResponse>, Status> {
        Ok(Response::new(stream_failure(
            &self.sessions,
            request.into_inner(),
        )))
    }

    async fn try_native_copy(
        &self,
        request: Request<pb::NativeCopyRequest>,
    ) -> Result<Response<pb::NativeCopyResponse>, Status> {
        let msg = request.into_inner();
        let spec = to_output_spec(msg.spec.unwrap_or_default());
        Ok(Response::new(match self.connector.try_native_copy(&spec) {
            None => pb::NativeCopyResponse {
                found: false,
                ..Default::default()
            },
            Some(copy) => pb::NativeCopyResponse {
                found: true,
                copy_sql: copy.copy_sql,
                setup_statements: copy.setup_statements,
                mechanism: copy.mechanism,
                finalizations: copy
                    .finalizations
                    .into_iter()
                    .map(|(temp_path, final_path)| pb::FileMoveMsg {
                        temp_path,
                        final_path,
                    })
                    .collect(),
            },
        }))
    }

    async fn begin_write(
        &self,
        request: Request<pb::BeginWriteRequest>,
    ) -> Result<Response<pb::WriteSessionTicket>, Status> {
        let msg = request.into_inner();
        let spec = to_output_spec(msg.spec.unwrap_or_default());
        let schema = deserialize_schema(&msg.arrow_schema_ipc)
            .map_err(|e| Status::invalid_argument(format!("malformed Arrow schema IPC: {e}")))?;

        let sink = self.sink().await.map_err(|e| to_status(&e))?;
        let session = sink
            .begin_write(spec, schema)
            .await
            .map_err(|e| to_status(&e))?;

        let session_id = Self::new_session_id();
        // Generated before the entry is built, not via `TicketRegistry::mint`: the ticket has to be
        // baked into `SessionState` itself (see its `ticket` field's doc) so `abort_write` can revoke
        // it later, and that means the bytes have to exist before the `Arc` the registry entry wraps
        // does.
        let ticket_bytes = TicketRegistry::generate();
        let state = SessionState::new(msg.op_id, ticket_bytes, session, Span::current().context());
        self.sessions
            .lock()
            .unwrap()
            .insert(session_id.clone(), state.clone());
        self.tickets.insert(ticket_bytes, TicketEntry::Write(state));

        Ok(Response::new(pb::WriteSessionTicket {
            session_id,
            ticket: ticket_bytes.to_vec(),
            // Without this every PCP sink would look like DiscardsAll to the host, whatever it
            // actually wraps -- the sink's own declaration crosses verbatim (Sink::abort_semantics).
            abort_semantics: to_abort_semantics_msg(sink.abort_semantics()) as i32,
        }))
    }

    async fn commit_write(
        &self,
        request: Request<pb::SessionRef>,
    ) -> Result<Response<pb::WriteResultMsg>, Status> {
        let session_id = request.into_inner().session_id;
        let state = {
            let sessions = self.sessions.lock().unwrap();
            sessions.get(&session_id).cloned()
        }
        .ok_or_else(|| unknown_session(&session_id))?;

        // The ticket is deliberately NOT revoked here. A small write fits in the kernel's socket buffer,
        // so the host can finish its whole data stream and send this RPC while the data connection is
        // still waiting to be accepted; revoking now would turn that connection away unread, and the
        // drain awaited below -- which only that connection can signal -- would never come. The ticket
        // lives exactly as long as the session does: burned by the data connection this commit waits
        // for, or revoked by `abort_write`, which is also how a commit the host gave up on is cleaned up.

        // Not removed from `sessions` until the drain actually completes: a cancelled/dropped commit
        // attempt (client deadline, caller cancellation) must leave the session exactly as abortable as
        // it was before this call.
        let receiver = state.take_drained_receiver().await.ok_or_else(|| {
            Status::failed_precondition("CommitWrite already attempted for this session")
        })?;

        match receiver.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(to_status(&e)),
            Err(_) => {
                return Err(Status::internal(
                    "the write pump ended without signaling whether its data stream drained",
                ))
            }
        }

        self.sessions.lock().unwrap().remove(&session_id);
        let mut session = state.take_session().await.ok_or_else(|| {
            Status::failed_precondition("write session has no data left to commit")
        })?;
        let result = session.commit().await.map_err(|e| to_status(&e))?;
        Ok(Response::new(pb::WriteResultMsg {
            rows_written: result.rows_written,
            batches_written: result.batches_written,
        }))
    }

    async fn abort_write(
        &self,
        request: Request<pb::SessionRef>,
    ) -> Result<Response<pb::AbortResponse>, Status> {
        let session_id = request.into_inner().session_id;
        let state = {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.remove(&session_id)
        }
        .ok_or_else(|| unknown_session(&session_id))?;

        // A session aborted before its data connection arrived must not leave a live ticket a later
        // connection could still present.
        self.tickets.revoke(&state.ticket());

        // No drain wait: abort exists precisely for a stream that never completed. Force-unblock first
        // so a pump stuck reading the data socket cannot make this wait forever.
        state.force_unblock();
        if let Some(mut session) = state.take_session().await {
            session.abort().await.map_err(|e| to_status(&e))?;
        }

        Ok(Response::new(pb::AbortResponse {}))
    }

    async fn cancel(
        &self,
        request: Request<pb::CancelRequest>,
    ) -> Result<Response<pb::CancelResponse>, Status> {
        let op_id = request.into_inner().op_id;
        // A write pump reads from the host, not from any read path this SDK has, so the op id is the
        // only handle Cancel has on it -- force-unblock every session still open for this op.
        let sessions = self.sessions.lock().unwrap();
        for state in sessions.values() {
            if state.op_id == op_id {
                state.force_unblock();
            }
        }
        Ok(Response::new(pb::CancelResponse {}))
    }

    async fn shutdown(
        &self,
        _request: Request<pb::ShutdownRequest>,
    ) -> Result<Response<pb::ShutdownResponse>, Status> {
        // Signal only: the process stops after this response is on the wire, which is what keeps a
        // graceful Shutdown distinguishable from a crash on the host side.
        let _ = self.shutdown_tx.send(true);
        Ok(Response::new(pb::ShutdownResponse {}))
    }

    type HostChannelStream = HostChannelStream;

    async fn host_channel(
        &self,
        request: Request<Streaming<pb::HostChannelDown>>,
    ) -> Result<Response<Self::HostChannelStream>, Status> {
        let mut inbound = request.into_inner();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<pb::HostChannelUp, Status>>(
            hostlog::HOST_CHANNEL_BUFFER,
        );
        // Attached before the pump task is even spawned: a log queued between this call starting and
        // the task's first poll must not be able to slip past the backlog flush -- `attach` and
        // `queue_log` share one lock, so there is no window where a log looks "queued after attach"
        // but never gets flushed the outbound sender that now exists.
        self.log_peer.attach(tx.clone());
        let log_peer = self.log_peer.clone();
        tokio::spawn(async move {
            // GateGrant is the only HostChannelDown case; this SDK does not yet expose GateAcquire to
            // connector authors (no host service is consumed in v1), so there is nothing to act on --
            // draining keeps the channel well-formed until the host closes it. Racing against shutdown
            // is what lets a `Shutdown` RPC actually end this stream: the host is free to hold a
            // `HostChannel` call open for the connector's whole lifetime, and graceful server shutdown
            // will not close a connection with an in-flight stream on it, so without this a Shutdown
            // would hang until the host separately drops its own end.
            loop {
                tokio::select! {
                    message = inbound.message() => {
                        match message {
                            Ok(Some(_down)) => continue,
                            _ => break,
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
            // However this call ends (host closed it, deadline, this process shutting down): later
            // logs fall back to the backlog until the next HostChannel call attaches, the same as
            // before the first one ever arrived.
            log_peer.detach();
            drop(tx);
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

/// Installs a `tracing` global-default subscriber composing [`hostlog::build_deferred_layer`] (so
/// this process's own logging always reaches the host, see `hostlog`'s doc) and
/// [`telemetry::build_deferred_layer`] (so OTel export keeps working exactly as it does today for a
/// connector with no subscriber of its own) -- attempted once, unconditionally, before anything else
/// this process does.
///
/// A connector author who installed their own subscriber before calling [`serve_sink`] (composing
/// [`crate::layer`] and/or [`hostlog::log_layer`], the same shape `--own-subscriber` in
/// `examples/memory_sink.rs` takes) has already claimed the process's one global-default slot by the
/// time this runs, so the attempt below simply loses the race and is a no-op: their own composed
/// layers keep working exactly as before this function existed. Only on a WIN does this register its
/// own installers -- see `telemetry::build_deferred_layer`'s doc for why registering unconditionally,
/// win or lose, would be wrong.
fn install_base_subscriber(log_peer: &Arc<hostlog::HostLogPeer>) {
    let (otel_layer, otel_installer) = telemetry::build_deferred_layer();
    let (log_layer, log_installer) = hostlog::build_deferred_layer();
    if tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(otel_layer)
            .with(log_layer),
    )
    .is_ok()
    {
        telemetry::register_author_layer_if_empty(otel_installer);
        hostlog::register_if_empty(log_installer);
    }

    // Resolves whichever installer ended up registered above (this call's own, having just won) or
    // registered earlier by a connector author's explicit `hostlog::log_layer()` composition -- the
    // peer itself exists only from this point on, so nothing could have filled either OnceLock before
    // now regardless of which one is live.
    hostlog::install(log_peer.clone());
}

/// What `Handshake` answers, and independently what `--pz-manifest` prints (see [`render_manifest`]):
/// the same [`ConnectorDecl`] feeds both, so the two can never disagree about name, capabilities, or
/// which SDK built this connector.
fn hello_for(decl: &ConnectorDecl) -> pb::Hello {
    pb::Hello {
        info: Some(pb::ConnectorInfoMsg {
            name: decl.name.to_string(),
            version: decl.version.to_string(),
            protocol_major: PROTOCOL_MAJOR,
        }),
        capabilities: decl.capabilities as i64,
        connection_config_schema: decl.connection_config_schema.to_string(),
        dataset_config_schema: decl.dataset_config_schema.to_string(),
        output_config_schema: decl.output_config_schema.to_string(),
        transports: vec![TRANSPORT_PIPE.to_string()],
        // This crate's own name/version (env!, baked in at compile time from Cargo.toml) --
        // distinct from ConnectorInfoMsg's name/version, which is the CONNECTOR's own identity.
        sdk: Some(pb::SdkInfoMsg {
            name: env!("CARGO_PKG_NAME").to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    }
}

/// Every `ConnectorCapabilities` bit this build's `Pz.Connectors.Abstractions` enum defines, mirrored
/// by value and name (`ConnectorCapabilities.cs`), in ascending bit order -- the same order the C#
/// SDK's `ManifestWriter.CapabilityNames` yields, since that is what a flags enum's own `ToString`
/// produces. A bit set on `ConnectorDecl.capabilities` that is not in this table (impossible for a
/// build of this crate against its own pinned ABI version, but a manifest may be read by a different
/// pz build) is silently excluded, mirroring `ManifestWriter`'s own `KnownCapabilities` masking.
const CAPABILITY_NAMES: &[(u64, &str)] = &[
    (1, "ColumnPruning"),
    (2, "PredicatePushdown"),
    (4, "PartitionedRead"),
    (8, "NativeScan"),
    (16, "NativeCopy"),
    (32, "Merge"),
    (64, "Transactional"),
    (128, "BoundedWindow"),
    (256, "PathTemplating"),
    (512, "StreamingPartitions"),
    (1024, "InclusiveWatermarkBound"),
    (2048, "SyncState"),
    (4096, "GatedOperations"),
    (8192, "StablePartitionIds"),
    (16384, "CheckpointableReads"),
    (32768, "ReplaceWrites"),
    (65536, "CheckpointableWrites"),
    (131_072, "ChangeCapture"),
    (262_144, "ApplyDeletes"),
    (524_288, "TextLengthStats"),
    (1_048_576, "ColumnPartitionedWrites"),
    (2_097_152, "NativeOnlyRead"),
];

fn capability_names(capabilities: u64) -> Vec<String> {
    CAPABILITY_NAMES
        .iter()
        .filter(|(bit, _)| capabilities & bit != 0)
        .map(|(_, name)| (*name).to_string())
        .collect()
}

/// Renders the same document shape, field order and formatting as the C# SDK's `ManifestWriter`: two-
/// space indent, LF line endings, a final newline, `name`/`protocolMajorMin`/`protocolMajorMax`/
/// `capabilities`/`runtime`/`entrypoints`/`sdk` in that order. `entrypoints` is always empty -- this
/// crate ships no RID-based packaging pipeline yet (unlike the C# SDK's `--entrypoint` flag), so a
/// packaging step that adds one must fill this map itself; `projectDirectoryAnchor` is omitted
/// entirely (this SDK's `ConnectorDecl` has no such field to report, the same as a manifest that says
/// nothing about the anchor on the C# side).
fn render_manifest(decl: &ConnectorDecl) -> String {
    let mut doc = serde_json::Map::new();
    doc.insert(
        "name".to_string(),
        serde_json::Value::String(decl.name.to_string()),
    );
    doc.insert(
        "protocolMajorMin".to_string(),
        serde_json::Value::from(PROTOCOL_MAJOR),
    );
    doc.insert(
        "protocolMajorMax".to_string(),
        serde_json::Value::from(PROTOCOL_MAJOR),
    );
    doc.insert(
        "capabilities".to_string(),
        serde_json::Value::from(capability_names(decl.capabilities)),
    );
    doc.insert(
        "runtime".to_string(),
        serde_json::Value::String("process".to_string()),
    );
    doc.insert(
        "entrypoints".to_string(),
        serde_json::Value::Object(serde_json::Map::new()),
    );
    let mut sdk = serde_json::Map::new();
    sdk.insert(
        "name".to_string(),
        serde_json::Value::String(env!("CARGO_PKG_NAME").to_string()),
    );
    sdk.insert(
        "version".to_string(),
        serde_json::Value::String(env!("CARGO_PKG_VERSION").to_string()),
    );
    doc.insert("sdk".to_string(), serde_json::Value::Object(sdk));

    serde_json::to_string_pretty(&serde_json::Value::Object(doc))
        .expect("a Map<String, Value> built entirely from strings/arrays/objects always serializes")
        + "\n"
}

fn source_unimplemented() -> Status {
    Status::unimplemented(
        "this connector does not implement the source direction (the pz-connector Rust SDK is sink-first in v1)",
    )
}

fn unknown_session(session_id: &str) -> Status {
    Status::not_found(format!(
        "unknown or already-finished write session '{session_id}'"
    ))
}

/// Spelled exactly as `pz connector test` spells it.
const NUMERIC_PROBE_KEY: &str = "pz_conformance_numeric_probe";

/// `pz connector test` asks the SDK, not the connector, whether a whole number survives the wire as
/// an integer: that is decided by this crate's own `Config::from_struct`. A Validate whose config is
/// the probe key alone is answered here -- no errors when the value arrived integral, the agreed
/// refusal otherwise -- and never reaches the connector. The host sends 0.5 first; only an SDK that
/// answers the probe refuses it in exactly these words, which is how it tells one from a connector
/// that merely tolerates an unknown key.
fn answer_numeric_probe(config: &Config) -> Option<Vec<String>> {
    if config.0.len() != 1 {
        return None;
    }
    let value = config.0.get(NUMERIC_PROBE_KEY)?;
    Some(if value.as_i64().is_some() {
        Vec::new()
    } else {
        vec![format!("{NUMERIC_PROBE_KEY}: not integral")]
    })
}

/// `GetStreamFailure`: what a write session's drain ended with. Side-effect free -- it never
/// commits, aborts, revokes or removes anything, so the host may ask as often as it likes and the
/// session stays exactly as abortable as before. Absent covers every "nothing to say": a read stream
/// (this SDK serves none), a session already finished and forgotten, a drain still in flight, or one
/// that ended cleanly. Only a failure the pump actually recorded is an answer.
fn stream_failure(
    sessions: &StdMutex<HashMap<String, Arc<SessionState>>>,
    request: pb::StreamFailureRequest,
) -> pb::StreamFailureResponse {
    let failure = match request.stream {
        Some(pb::stream_failure_request::Stream::Write(session)) => sessions
            .lock()
            .unwrap()
            .get(&session.session_id)
            .and_then(|state| state.drain_failure())
            .map(|e| to_error_detail(&e)),
        Some(pb::stream_failure_request::Stream::Read(_)) | None => None,
    };
    pb::StreamFailureResponse { failure }
}

fn to_output_spec(msg: pb::OutputSpecMsg) -> OutputSpec {
    OutputSpec {
        sink: msg.sink,
        output: msg.output,
        mode: msg.mode,
        schema_policy: msg.schema_policy,
        options: Config::from_struct(msg.options.as_ref()).0,
        keys: msg.keys,
        on_delete: msg.on_delete,
        max_text_lengths: msg.max_text_lengths_set.then_some(msg.max_text_lengths),
        attempt: msg.attempt.map(|a| WriteAttempt {
            node: a.node,
            run: a.run,
            ordinal: a.ordinal,
        }),
    }
}

fn deserialize_schema(bytes: &[u8]) -> Result<SchemaRef, arrow::error::ArrowError> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    Ok(reader.schema())
}

// ---------------------------------------------------------------------------------------------
// Orphan prevention (SDK-internal)
// ---------------------------------------------------------------------------------------------
//
// The spec makes it normative that a connector exits when the control socket closes or the host
// dies -- a SIGKILLed host can never orphan this process. This mirrors the reference fixture that
// proves the design out-of-process in C#, `ControlConnectionWatch` in
// `tests/fixtures/PcpFakeConnector/Program.cs`: two timers, not one, because a host that dies before
// ever dialing and one that dies after leave the same orphan, and a connection-close event alone
// only covers the second.

/// Mirrored from `Pz.Connectors.Protocol.ProtocolConstants.HandshakeTimeout`.
const HANDSHAKE_TIMEOUT_SECS: u64 = 15;

/// How long this process waits for its first control connection before deciding the host died (or
/// was killed) before ever dialing it. Twice the handshake timeout: a host still inside its own
/// handshake budget has not failed yet. Mirrors `FirstConnectionDeadline` in the reference fixture.
const FIRST_CONNECTION_DEADLINE: Duration = Duration::from_secs(HANDSHAKE_TIMEOUT_SECS * 2);

/// How long this process keeps running with no control connection open before deciding it has been
/// orphaned. A host that means to keep the connector alive keeps its control connection open and
/// ends the process with the `Shutdown` RPC; anything else -- a crashed host, a killed host process
/// -- leaves this process with no one to serve, and it exits rather than lingering. Mirrors
/// `OrphanExitGrace` in the reference fixture.
const ORPHAN_EXIT_GRACE: Duration = Duration::from_secs(5);

/// Trips `shutdown_tx` if the host never dials the control socket within [`FIRST_CONNECTION_DEADLINE`]
/// of it being served, and again once the last open control connection has been closed for
/// [`ORPHAN_EXIT_GRACE`]. The countdown only ever runs while the open-connection count is zero, so a
/// connection the host is holding open but not currently using -- idle between RPCs, exactly the
/// normal case for a long-lived control channel -- never trips it: only a close does, never mere
/// inactivity on a live connection.
struct ControlConnectionWatch {
    state: StdMutex<WatchState>,
    startup_deadline: Duration,
    idle_grace: Duration,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

#[derive(Default)]
struct WatchState {
    open: usize,
    ever_connected: bool,
    /// Dropping (or sending on) this cancels whichever countdown -- startup or idle -- is currently
    /// running, if any: the paired `tokio::select!` in `spawn_countdown` takes its cancellation
    /// branch instead of firing the timeout.
    cancel: Option<oneshot::Sender<()>>,
}

impl ControlConnectionWatch {
    fn new(
        startup_deadline: Duration,
        idle_grace: Duration,
        shutdown_tx: tokio::sync::watch::Sender<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: StdMutex::new(WatchState::default()),
            startup_deadline,
            idle_grace,
            shutdown_tx,
        })
    }

    /// Starts the first-connection clock. Called once the control socket is actually being served,
    /// so the deadline measures the host's silence and not this process's own startup.
    fn start(self: &Arc<Self>) {
        let mut state = self.state.lock().unwrap();
        if state.ever_connected {
            return;
        }
        let cancel_rx = Self::arm(&mut state);
        drop(state);
        self.spawn_countdown(self.startup_deadline, cancel_rx);
    }

    /// One control connection was accepted.
    fn opened(self: &Arc<Self>) {
        let mut state = self.state.lock().unwrap();
        state.open += 1;
        state.ever_connected = true;
        // A connection just arrived, so whichever countdown was running -- the startup deadline, or
        // an idle-grace countdown left over from a previous connection dropping to zero -- no longer
        // applies.
        state.cancel = None;
    }

    /// One control connection closed. Starts the idle-grace countdown only when this was the last one
    /// open.
    fn closed(self: &Arc<Self>) {
        let mut state = self.state.lock().unwrap();
        state.open = state.open.saturating_sub(1);
        if state.open > 0 {
            return;
        }
        let cancel_rx = Self::arm(&mut state);
        drop(state);
        self.spawn_countdown(self.idle_grace, cancel_rx);
    }

    /// Replaces `state.cancel` with a fresh channel and returns its receiver. Any countdown
    /// previously armed is dropped (and thereby cancelled) as part of the replacement.
    fn arm(state: &mut WatchState) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        state.cancel = Some(tx);
        rx
    }

    fn spawn_countdown(self: &Arc<Self>, delay: Duration, cancel: oneshot::Receiver<()>) {
        let watch = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    let _ = watch.shutdown_tx.send(true);
                }
                _ = cancel => {}
            }
        });
    }
}

/// Wraps an accepted control connection so [`ControlConnectionWatch`] learns about its lifetime:
/// [`ControlConnectionWatch::opened`] fires when this is constructed (right after accept), and
/// [`ControlConnectionWatch::closed`] fires on drop -- whenever tonic tears the connection down, for
/// any reason (peer closed, protocol error, or this process's own graceful shutdown).
struct WatchedUnixStream {
    inner: tokio::net::UnixStream,
    watch: Arc<ControlConnectionWatch>,
}

impl WatchedUnixStream {
    fn new(inner: tokio::net::UnixStream, watch: Arc<ControlConnectionWatch>) -> Self {
        watch.opened();
        Self { inner, watch }
    }
}

impl Drop for WatchedUnixStream {
    fn drop(&mut self) {
        self.watch.closed();
    }
}

impl Connected for WatchedUnixStream {
    type ConnectInfo = <tokio::net::UnixStream as Connected>::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

impl AsyncRead for WatchedUnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for WatchedUnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Wraps the control socket's accept stream so every connection tonic ends up serving is a
/// [`WatchedUnixStream`] -- the only place in this codebase that turns "a `UnixStream` got accepted"
/// into an orphan-watch event.
struct WatchedIncoming {
    inner: tokio_stream::wrappers::UnixListenerStream,
    watch: Arc<ControlConnectionWatch>,
}

impl Stream for WatchedIncoming {
    type Item = io::Result<WatchedUnixStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx).map(|item| {
            item.map(|accepted| {
                accepted.map(|stream| WatchedUnixStream::new(stream, this.watch.clone()))
            })
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Process entry point
// ---------------------------------------------------------------------------------------------

/// Parses `--pz-socket`, serves the `PzConnector` gRPC control plane on that Unix socket (mode 0600)
/// and the raw Arrow IPC data plane on `<socket>.data`, and returns once told to stop (the `Shutdown`
/// RPC).
///
/// Always returns an `anyhow::Error` -- there is no meaningful "success" value for a function whose job
/// is to run until stopped. Downcast the result to [`ServeExit`] to tell a setup/usage failure (neither
/// socket ever served a single RPC) apart from a normal stop.
pub async fn serve_sink<C: SinkConnector>(decl: ConnectorDecl, connector: C) -> anyhow::Error {
    match serve_sink_inner(decl, connector).await {
        Ok(reason) => anyhow::Error::new(ServeExit::Stopped(reason)),
        Err(e) => e,
    }
}

async fn serve_sink_inner<C: SinkConnector>(
    decl: ConnectorDecl,
    connector: C,
) -> Result<String, anyhow::Error> {
    let socket_path = match parse_host_command(std::env::args().skip(1))
        .map_err(|msg| anyhow::Error::new(ServeExit::UsageError(msg)))?
    {
        HostCommand::Manifest => {
            println!("{}", render_manifest(&decl));
            return Ok(
                "printed the manifest (--pz-manifest) and exited without serving".to_string(),
            );
        }
        HostCommand::Serve(path) => path,
    };

    // Always attempted, before either socket binds: this is what lets a connector's own logging
    // (`tracing::info!`/`warn!`/`error!`) reach the host as `connector_log` run events even when no
    // OTel endpoint is ever configured for this run, and even for logging emitted before `Handshake`
    // (e.g. from inside `Configure`). See `hostlog`'s own doc for the composition rules this respects.
    let log_peer = Arc::new(hostlog::HostLogPeer::new());
    install_base_subscriber(&log_peer);

    if let Some(parent) = socket_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|e| {
            anyhow::anyhow!(
                "failed to create socket directory '{}': {e}",
                parent.display()
            )
        })?;
    }

    let data_socket_path = data_socket_path(&socket_path);
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_file(&data_socket_path);

    let control_listener = tokio::net::UnixListener::bind(&socket_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to bind control socket '{}': {e}",
            socket_path.display()
        )
    })?;
    restrict_to_owner(&socket_path)?;

    let data_listener = tokio::net::UnixListener::bind(&data_socket_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to bind data socket '{}': {e}",
            data_socket_path.display()
        )
    })?;
    restrict_to_owner(&data_socket_path)?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let tickets = Arc::new(TicketRegistry::default());
    // Kept here (not just inside `service`) so the shutdown sweep below can still reach every live
    // session's data connection after `service` itself has been moved into the server builder.
    let sessions: Arc<StdMutex<HashMap<String, Arc<SessionState>>>> =
        Arc::new(StdMutex::new(HashMap::new()));
    let instance = Arc::new(StdMutex::new(None));
    let service = PzConnectorService {
        decl,
        connector,
        config: StdMutex::new(None),
        sink: AsyncMutex::new(None),
        sessions: sessions.clone(),
        tickets: tickets.clone(),
        shutdown_tx: shutdown_tx.clone(),
        shutdown_rx: shutdown_rx.clone(),
        instance: instance.clone(),
        log_peer: log_peer.clone(),
    };

    let data_plane_task =
        tokio::spawn(data_plane::run(data_listener, tickets, shutdown_rx.clone()));

    // Orphan prevention: this process stops on its own if the host that spawned it can never come
    // back to say so, whether it dies before ever dialing the control socket or after -- see
    // `ControlConnectionWatch`'s doc.
    let watch = ControlConnectionWatch::new(
        FIRST_CONNECTION_DEADLINE,
        ORPHAN_EXIT_GRACE,
        shutdown_tx.clone(),
    );
    watch.start();
    let incoming = WatchedIncoming {
        inner: tokio_stream::wrappers::UnixListenerStream::new(control_listener),
        watch: watch.clone(),
    };

    let mut server_shutdown = shutdown_rx.clone();
    let serve_result = Server::builder()
        .layer(telemetry::rpc_layer(instance))
        .add_service(PzConnectorServer::new(service))
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = server_shutdown.wait_for(|stopped| *stopped).await;
        })
        .await;

    // Whatever stopped the control-plane serve loop (Shutdown RPC or the server future itself ending)
    // also stops the data-plane accept loop -- both listeners' lifetimes are tied together.
    let _ = shutdown_tx.send(true);

    // A pump parked in a blocking read on its data connection (host mid-write, or simply never getting
    // around to half-closing) would otherwise pin this process past the shutdown grace: the accept loop
    // stopping does nothing for a connection it already handed off to a spawn_blocking thread. Force
    // every live session's data connection closed so each pump unblocks (with a failed drain -- correct,
    // since a write cut short by a forced shutdown is not a completed one) before waiting for the
    // data-plane task to actually finish.
    for state in sessions.lock().unwrap().values() {
        state.force_unblock();
    }
    let _ = data_plane_task.await;

    // After every pump has ended, so nothing can start a span this flush would miss.
    telemetry::flush_and_shutdown().await;

    serve_result.map_err(|e| anyhow::anyhow!("control-plane server failed: {e}"))?;
    Ok("received the Shutdown RPC (or the control-plane listener otherwise stopped)".to_string())
}

/// The two modes argv selects between: serve over PCP (`--pz-socket <path>`), or print the manifest
/// [`render_manifest`] builds from the same [`ConnectorDecl`] `Handshake` answers from
/// (`--pz-manifest`) and exit without ever binding a socket.
#[derive(Debug)]
enum HostCommand {
    Serve(PathBuf),
    Manifest,
}

/// `--pz-manifest` and `--pz-socket` are mutually exclusive; everything else this iterator does not
/// recognize is ignored, matching [`parse_socket_arg`]'s existing leniency (connector configuration
/// never travels on argv, so an unrecognized flag here is not this parser's business to police).
fn parse_host_command(args: impl Iterator<Item = String>) -> Result<HostCommand, String> {
    let args: Vec<String> = args.collect();
    let wants_manifest = args.iter().any(|a| a == "--pz-manifest");
    let wants_socket = args.iter().any(|a| a == "--pz-socket");
    if wants_manifest && wants_socket {
        return Err("--pz-socket and --pz-manifest are separate modes".to_string());
    }
    if wants_manifest {
        return Ok(HostCommand::Manifest);
    }
    parse_socket_arg(args.into_iter()).map(HostCommand::Serve)
}

fn parse_socket_arg(mut args: impl Iterator<Item = String>) -> Result<PathBuf, String> {
    while let Some(arg) = args.next() {
        if arg == "--pz-socket" {
            let value = args
                .next()
                .ok_or_else(|| "--pz-socket needs a socket path".to_string())?;
            return Ok(PathBuf::from(value));
        }
    }
    Err("--pz-socket <path> is required".to_string())
}

fn data_socket_path(control: &Path) -> PathBuf {
    let mut s = control.as_os_str().to_os_string();
    s.push(DATA_SOCKET_SUFFIX);
    PathBuf::from(s)
}

/// Both sockets are owner-only: a unix socket's file permissions are the whole access control on this
/// transport, and the socket carries credentials in one direction and data in the other.
fn restrict_to_owner(path: &Path) -> Result<(), anyhow::Error> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
        anyhow::anyhow!(
            "failed to restrict permissions on socket '{}': {e}",
            path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;

    fn probe_config(number: f64) -> Config {
        let mut root = prost_types::Struct::default();
        root.fields.insert(
            NUMERIC_PROBE_KEY.to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(number)),
            },
        );
        Config::from_struct(Some(&root))
    }

    #[test]
    fn numeric_probe_whole_number_that_crossed_as_f64_is_answered_clean() {
        assert_eq!(answer_numeric_probe(&probe_config(424_242.0)), Some(vec![]));
    }

    #[test]
    fn numeric_probe_fractional_value_is_refused_in_the_agreed_words() {
        assert_eq!(
            answer_numeric_probe(&probe_config(0.5)),
            Some(vec![
                "pz_conformance_numeric_probe: not integral".to_string()
            ])
        );
    }

    #[test]
    fn numeric_probe_leaves_a_real_config_to_the_connector() {
        assert_eq!(answer_numeric_probe(&Config::from_struct(None)), None);
        let mut with_other_keys = probe_config(1.0);
        with_other_keys
            .0
            .insert("host".to_string(), serde_json::Value::from("db"));
        assert_eq!(answer_numeric_probe(&with_other_keys), None);
    }

    fn test_watch(
        startup_deadline: Duration,
        idle_grace: Duration,
    ) -> (
        Arc<ControlConnectionWatch>,
        tokio::sync::watch::Receiver<bool>,
    ) {
        let (tx, rx) = tokio::sync::watch::channel(false);
        (
            ControlConnectionWatch::new(startup_deadline, idle_grace, tx),
            rx,
        )
    }

    /// Advances the paused clock and gives every spawned countdown task a chance to actually run and
    /// observe it. Yields before advancing too: a task spawned just before this call has not been
    /// polled even once yet, so it has not created its `tokio::time::sleep` future -- advancing the
    /// clock before that first poll would jump past a deadline that does not exist yet. `advance`
    /// itself only moves the clock; it does not poll tasks parked on that time, so a settling loop
    /// follows it as well.
    async fn advance_and_settle(by: Duration) {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(by).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn no_connection_within_the_startup_deadline_trips_shutdown() {
        let (watch, shutdown_rx) = test_watch(Duration::from_millis(100), Duration::from_secs(30));
        watch.start();

        advance_and_settle(Duration::from_millis(150)).await;

        assert!(
            *shutdown_rx.borrow(),
            "a host that never dials the control socket must not leave the connector running forever"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_connection_before_the_startup_deadline_cancels_it() {
        let (watch, shutdown_rx) = test_watch(Duration::from_millis(100), Duration::from_secs(30));
        watch.start();

        watch.opened();
        advance_and_settle(Duration::from_millis(150)).await;

        assert!(
            !*shutdown_rx.borrow(),
            "a control connection accepted before the startup deadline must cancel it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_last_connection_closing_trips_shutdown_after_the_idle_grace() {
        let (watch, shutdown_rx) = test_watch(Duration::from_secs(30), Duration::from_millis(100));
        watch.start();
        watch.opened();

        watch.closed();
        assert!(
            !*shutdown_rx.borrow(),
            "shutdown must not trip before the idle grace has elapsed"
        );

        advance_and_settle(Duration::from_millis(150)).await;

        assert!(
            *shutdown_rx.borrow(),
            "the last control connection closing, and staying closed, must eventually stop the process"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_reconnect_during_the_idle_grace_cancels_the_pending_exit() {
        let (watch, shutdown_rx) = test_watch(Duration::from_secs(30), Duration::from_millis(100));
        watch.start();
        watch.opened();
        watch.closed();

        advance_and_settle(Duration::from_millis(50)).await; // still within the grace period
        watch.opened(); // the host reconnected before the grace period ran out

        advance_and_settle(Duration::from_secs(1)).await; // well past the original deadline

        assert!(
            !*shutdown_rx.borrow(),
            "a reconnect within the grace period must cancel the pending orphan exit"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn closing_one_of_several_open_connections_does_not_trip_shutdown() {
        let (watch, shutdown_rx) = test_watch(Duration::from_secs(30), Duration::from_millis(100));
        watch.start();
        watch.opened();
        watch.opened();

        watch.closed(); // one of the two closes; the other is still open
        advance_and_settle(Duration::from_secs(1)).await;

        assert!(
            !*shutdown_rx.borrow(),
            "shutdown must wait for every open control connection to close, not just one"
        );
    }

    /// End-to-end over a real Unix socket: accepts through the same `WatchedUnixStream` production
    /// code wraps every control connection in, then proves that connecting, then dropping, the only
    /// client -- with nothing else ever dialing back in -- is what makes the watch's shutdown signal
    /// trip.
    ///
    /// Time is left running for the connect/accept/drop dance and only paused afterward, right
    /// before the deterministic part: a paused clock auto-advances to the earliest pending timer
    /// whenever the executor would otherwise have nothing to do, and that auto-advance cannot tell a
    /// real socket operation that is about to complete from one that never will -- pausing while a
    /// real accept/read is still in flight risks the clock jumping ahead of it. By the time this
    /// pauses, the connection is fully established and no timer is armed, so there is nothing left
    /// for a premature auto-advance to race against.
    #[tokio::test]
    async fn dropping_the_only_real_control_connection_eventually_trips_shutdown() {
        let dir = tempfile::tempdir().expect("failed to create a scratch dir for the test socket");
        let socket_path = dir.path().join("control.sock");
        let listener =
            tokio::net::UnixListener::bind(&socket_path).expect("failed to bind test socket");

        let (watch, shutdown_rx) = test_watch(Duration::from_secs(30), Duration::from_millis(100));
        watch.start();

        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let watch_for_accept = watch.clone();
        let accept_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept failed");
            let mut conn = WatchedUnixStream::new(stream, watch_for_accept);
            let _ = accepted_tx.send(());
            // Mirrors what the real transport does: block on a read until the peer goes away. The
            // client in this test never writes anything -- it only connects and disconnects.
            let mut buf = [0u8; 1];
            let _ = conn.read(&mut buf).await;
        });

        let client = tokio::net::UnixStream::connect(&socket_path)
            .await
            .expect("connect failed");
        accepted_rx
            .await
            .expect("the accept task never registered the connection");
        assert!(
            !*shutdown_rx.borrow(),
            "a live control connection must not trigger shutdown"
        );

        tokio::time::pause();

        drop(client);
        accept_task.await.expect("accept task panicked");

        advance_and_settle(Duration::from_millis(150)).await;

        assert!(
            *shutdown_rx.borrow(),
            "an orphaned connector (no control connection, none reopened) must stop serving"
        );
    }

    /// A control connection that simply has nothing to do right now -- no RPC in flight, no traffic
    /// at all -- must never be treated as orphaned. Only a close (or never having connected at all)
    /// starts either countdown. Time is paused only after the connection is established, for the same
    /// reason `dropping_the_only_real_control_connection_eventually_trips_shutdown` does.
    #[tokio::test]
    async fn a_long_idle_but_still_open_connection_is_never_treated_as_orphaned() {
        let dir = tempfile::tempdir().expect("failed to create a scratch dir for the test socket");
        let socket_path = dir.path().join("control.sock");
        let listener =
            tokio::net::UnixListener::bind(&socket_path).expect("failed to bind test socket");

        let (watch, shutdown_rx) = test_watch(Duration::from_secs(30), Duration::from_millis(50));
        watch.start();

        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let watch_for_accept = watch.clone();
        let _accept_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept failed");
            let conn = WatchedUnixStream::new(stream, watch_for_accept);
            let _ = accepted_tx.send(());
            // Never reads, never drops -- exactly a live connection sitting idle between RPCs.
            std::future::pending::<()>().await;
            drop(conn);
        });

        let _client = tokio::net::UnixStream::connect(&socket_path)
            .await
            .expect("connect failed");
        accepted_rx
            .await
            .expect("the accept task never registered the connection");

        tokio::time::pause();
        advance_and_settle(Duration::from_secs(3600)).await;

        assert!(
            !*shutdown_rx.borrow(),
            "a long-idle but still-open connection must never be treated as orphaned"
        );
    }

    fn sessions_with(
        session_id: &str,
        state: Arc<SessionState>,
    ) -> Arc<StdMutex<HashMap<String, Arc<SessionState>>>> {
        let mut map = HashMap::new();
        map.insert(session_id.to_string(), state);
        Arc::new(StdMutex::new(map))
    }

    fn write_ref(session_id: &str) -> pb::StreamFailureRequest {
        pb::StreamFailureRequest {
            stream: Some(pb::stream_failure_request::Stream::Write(pb::SessionRef {
                session_id: session_id.to_string(),
            })),
        }
    }

    #[test]
    fn a_transient_drain_failure_is_answered_with_its_transience_and_retry_after() {
        let state = SessionState::new_for_test();
        state.signal_drained(Err(PzError::transient("rate limited mid-stream", 7_000)));
        let sessions = sessions_with("s1", state);

        let failure = stream_failure(&sessions, write_ref("s1")).failure;

        let detail = failure.expect("the failure the pump recorded must be reported");
        assert_eq!(detail.message, "rate limited mid-stream");
        assert!(detail.is_transient);
        assert_eq!(detail.retry_after_ms, 7_000);
    }

    #[test]
    fn a_clean_drain_has_no_failure_to_report() {
        let state = SessionState::new_for_test();
        state.signal_drained(Ok(()));
        let sessions = sessions_with("s1", state);

        assert!(stream_failure(&sessions, write_ref("s1")).failure.is_none());
    }

    #[test]
    fn a_session_still_draining_has_no_failure_to_report() {
        let sessions = sessions_with("s1", SessionState::new_for_test());

        assert!(stream_failure(&sessions, write_ref("s1")).failure.is_none());
    }

    #[test]
    fn an_unknown_session_is_no_failure_known_not_an_error() {
        let sessions = sessions_with("s1", SessionState::new_for_test());

        assert!(stream_failure(&sessions, write_ref("gone"))
            .failure
            .is_none());
    }

    #[test]
    fn a_read_stream_is_never_known_to_a_sink_only_connector() {
        let sessions = sessions_with("s1", SessionState::new_for_test());
        let request = pb::StreamFailureRequest {
            stream: Some(pb::stream_failure_request::Stream::Read(
                pb::ReadStateRequest {
                    op_id: "op".to_string(),
                    partition_id: "p0".to_string(),
                },
            )),
        };

        assert!(stream_failure(&sessions, request).failure.is_none());
    }

    #[test]
    fn parses_the_socket_path() {
        let args = ["--pz-socket".to_string(), "/tmp/x.sock".to_string()];
        assert_eq!(
            parse_socket_arg(args.into_iter()).unwrap(),
            PathBuf::from("/tmp/x.sock")
        );
    }

    #[test]
    fn ignores_unrelated_flags_around_the_socket_path() {
        let args = [
            "--other".to_string(),
            "--pz-socket".to_string(),
            "/tmp/x.sock".to_string(),
            "--endless".to_string(),
        ];
        assert_eq!(
            parse_socket_arg(args.into_iter()).unwrap(),
            PathBuf::from("/tmp/x.sock")
        );
    }

    #[test]
    fn missing_pz_socket_is_a_usage_error() {
        assert!(parse_socket_arg(std::iter::empty()).is_err());
    }

    #[test]
    fn data_socket_path_appends_the_fixed_suffix() {
        assert_eq!(
            data_socket_path(Path::new("/tmp/run/control.sock")),
            PathBuf::from("/tmp/run/control.sock.data")
        );
    }

    #[test]
    fn pz_manifest_is_a_separate_mode_from_pz_socket() {
        let args = [
            "--pz-socket".to_string(),
            "/tmp/x.sock".to_string(),
            "--pz-manifest".to_string(),
        ];
        assert!(parse_host_command(args.into_iter()).is_err());
    }

    #[test]
    fn pz_manifest_alone_selects_the_manifest_command() {
        let args = ["--pz-manifest".to_string()];
        assert!(matches!(
            parse_host_command(args.into_iter()),
            Ok(HostCommand::Manifest)
        ));
    }

    #[test]
    fn pz_socket_alone_still_selects_serve() {
        let args = ["--pz-socket".to_string(), "/tmp/x.sock".to_string()];
        match parse_host_command(args.into_iter()) {
            Ok(HostCommand::Serve(path)) => assert_eq!(path, Path::new("/tmp/x.sock")),
            other => panic!("expected HostCommand::Serve, got {other:?}"),
        }
    }

    // ---- ConnectorDecl fixtures and a minimal SinkConnector for exercising the RPC handlers below --

    fn fixture_decl() -> ConnectorDecl {
        ConnectorDecl {
            name: "acme-sink",
            version: "9.9.9",
            // Merge (32) | Transactional (64) | an unrecognized future bit (1 << 40), to prove
            // render_manifest/capability_names mask exactly the way ManifestWriter does.
            capabilities: 32 | 64 | (1u64 << 40),
            connection_config_schema: "",
            dataset_config_schema: "",
            output_config_schema: "",
        }
    }

    struct FixtureConnector {
        sink_abort_semantics: AbortSemantics,
        check_result: Result<(), PzError>,
        warnings: Vec<String>,
    }

    impl Default for FixtureConnector {
        fn default() -> Self {
            FixtureConnector {
                sink_abort_semantics: AbortSemantics::DiscardsAll,
                check_result: Ok(()),
                warnings: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl SinkConnector for FixtureConnector {
        async fn validate(&self, _config: &Config) -> Vec<String> {
            Vec::new()
        }

        async fn validate_warnings(&self, _config: &Config) -> Vec<String> {
            self.warnings.clone()
        }

        async fn check(&self, _config: &Config) -> Result<(), PzError> {
            self.check_result.clone()
        }

        async fn open(&self, _config: Config) -> Result<Box<dyn Sink>, PzError> {
            Ok(Box::new(FixtureSink {
                abort_semantics: self.sink_abort_semantics,
            }))
        }
    }

    struct FixtureSink {
        abort_semantics: AbortSemantics,
    }

    #[async_trait]
    impl Sink for FixtureSink {
        async fn begin_write(
            &self,
            _spec: OutputSpec,
            _schema: SchemaRef,
        ) -> Result<Box<dyn WriteSession>, PzError> {
            Ok(Box::new(FixtureWriteSession))
        }

        fn abort_semantics(&self) -> AbortSemantics {
            self.abort_semantics
        }
    }

    struct FixtureWriteSession;

    #[async_trait]
    impl WriteSession for FixtureWriteSession {
        async fn write_batch(&mut self, _batch: RecordBatch) -> Result<(), PzError> {
            Ok(())
        }

        async fn commit(&mut self) -> Result<WriteResult, PzError> {
            Ok(WriteResult::default())
        }

        async fn abort(&mut self) -> Result<(), PzError> {
            Ok(())
        }
    }

    fn encode_schema_ipc(schema: &arrow::datatypes::Schema) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema)
                .expect("a two-column schema always encodes");
            writer
                .finish()
                .expect("finishing an empty stream always succeeds");
        }
        buf
    }

    fn test_service(connector: FixtureConnector) -> PzConnectorService<FixtureConnector> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        PzConnectorService {
            decl: fixture_decl(),
            connector,
            config: StdMutex::new(None),
            sink: AsyncMutex::new(None),
            sessions: Arc::new(StdMutex::new(HashMap::new())),
            tickets: Arc::new(TicketRegistry::default()),
            shutdown_tx,
            shutdown_rx,
            instance: Arc::new(StdMutex::new(None)),
            log_peer: Arc::new(hostlog::HostLogPeer::new()),
        }
    }

    #[test]
    fn abort_semantics_maps_to_proto_ordinals_matching_the_csharp_abi() {
        assert_eq!(
            to_abort_semantics_msg(AbortSemantics::DiscardsAll),
            pb::AbortSemanticsMsg::AbortSemanticsDiscardsAll
        );
        assert_eq!(
            to_abort_semantics_msg(AbortSemantics::BestEffort),
            pb::AbortSemanticsMsg::AbortSemanticsBestEffort
        );
        assert_eq!(
            to_abort_semantics_msg(AbortSemantics::None),
            pb::AbortSemanticsMsg::AbortSemanticsNone
        );
    }

    #[tokio::test]
    async fn begin_write_reports_the_sinks_declared_abort_semantics() {
        let service = test_service(FixtureConnector {
            sink_abort_semantics: AbortSemantics::BestEffort,
            ..Default::default()
        });
        service
            .configure(Request::new(pb::ConfigureRequest {
                instance_id: "a".to_string(),
                config: None,
            }))
            .await
            .expect("Configure must succeed before BeginWrite can open a sink");

        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "value",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let response = service
            .begin_write(Request::new(pb::BeginWriteRequest {
                op_id: "op1".to_string(),
                spec: Some(pb::OutputSpecMsg::default()),
                arrow_schema_ipc: encode_schema_ipc(&schema),
            }))
            .await
            .expect("BeginWrite must succeed against the fixture sink")
            .into_inner();

        // Without Sink::abort_semantics being threaded through, this would always answer DiscardsAll
        // -- see BeginWrite's own comment on why every PCP sink used to look transactional.
        assert_eq!(
            response.abort_semantics,
            pb::AbortSemanticsMsg::AbortSemanticsBestEffort as i32
        );
    }

    #[tokio::test]
    async fn check_connection_reports_a_connectors_failure_as_ok_false_not_a_status() {
        let service = test_service(FixtureConnector {
            check_result: Err(PzError::new("could not reach the destination")),
            ..Default::default()
        });

        let response = service
            .check_connection(Request::new(pb::CheckRequest { config: None }))
            .await
            .expect("a connector-reported check failure must not cross as an RpcException")
            .into_inner();

        assert!(!response.ok);
        assert_eq!(
            response.message.as_deref(),
            Some("could not reach the destination")
        );
    }

    #[tokio::test]
    async fn check_connection_reports_success_with_no_message() {
        let service = test_service(FixtureConnector::default());

        let response = service
            .check_connection(Request::new(pb::CheckRequest { config: None }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.ok);
        assert_eq!(response.message, None);
    }

    #[tokio::test]
    async fn a_second_configure_is_refused_as_already_configured() {
        let service = test_service(FixtureConnector::default());
        service
            .configure(Request::new(pb::ConfigureRequest {
                instance_id: "a".to_string(),
                config: None,
            }))
            .await
            .expect("the first Configure must succeed");

        let err = service
            .configure(Request::new(pb::ConfigureRequest {
                instance_id: "b".to_string(),
                config: None,
            }))
            .await
            .expect_err("a second Configure on the same process must be refused");

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(err.message(), "connector is already configured");
    }

    #[test]
    fn capability_names_masks_unknown_bits_and_stays_in_ascending_order() {
        let names = capability_names(fixture_decl().capabilities);
        // The unrecognized `1u64 << 40` bit fixture_decl() also sets must not appear -- mirrors
        // ManifestWriter's KnownCapabilities masking.
        assert_eq!(
            names,
            vec!["Merge".to_string(), "Transactional".to_string()]
        );
    }

    #[test]
    fn manifest_and_hello_agree_on_name_capabilities_and_sdk() {
        let decl = fixture_decl();
        let hello = hello_for(&decl);
        let manifest: serde_json::Value = serde_json::from_str(&render_manifest(&decl))
            .expect("render_manifest emits valid JSON");

        let info = hello.info.expect("Hello always carries ConnectorInfoMsg");
        assert_eq!(manifest["name"], decl.name);
        assert_eq!(manifest["protocolMajorMin"], info.protocol_major);
        assert_eq!(manifest["protocolMajorMax"], info.protocol_major);

        let hello_sdk = hello.sdk.expect("Hello always carries SdkInfoMsg");
        assert_eq!(manifest["sdk"]["name"], hello_sdk.name);
        assert_eq!(manifest["sdk"]["version"], hello_sdk.version);

        let manifest_caps: Vec<String> =
            serde_json::from_value(manifest["capabilities"].clone()).unwrap();
        assert_eq!(manifest_caps, capability_names(hello.capabilities as u64));
    }

    #[test]
    fn manifest_rendering_is_byte_stable() {
        let decl = ConnectorDecl {
            name: "acme-sink",
            version: "1.0.0",
            capabilities: 0,
            connection_config_schema: "",
            dataset_config_schema: "",
            output_config_schema: "",
        };

        let expected = format!(
            "{{\n  \"name\": \"acme-sink\",\n  \"protocolMajorMin\": 1,\n  \"protocolMajorMax\": 1,\n  \"capabilities\": [],\n  \"runtime\": \"process\",\n  \"entrypoints\": {{}},\n  \"sdk\": {{\n    \"name\": \"{}\",\n    \"version\": \"{}\"\n  }}\n}}\n",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        );
        assert_eq!(render_manifest(&decl), expected);
    }

    #[tokio::test]
    async fn validate_forwards_a_connectors_warnings() {
        let service = test_service(FixtureConnector {
            warnings: vec!["'legacy_option' is accepted but deprecated".to_string()],
            ..Default::default()
        });

        let response = service
            .validate(Request::new(pb::ValidateRequest { config: None }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.errors.is_empty());
        assert_eq!(
            response.warnings,
            vec!["'legacy_option' is accepted but deprecated".to_string()]
        );
    }

    #[tokio::test]
    async fn the_numeric_conformance_probe_never_reaches_validate_warnings() {
        let service = test_service(FixtureConnector {
            warnings: vec!["should never be seen".to_string()],
            ..Default::default()
        });

        let mut root = prost_types::Struct::default();
        root.fields.insert(
            NUMERIC_PROBE_KEY.to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(1.0)),
            },
        );

        let response = service
            .validate(Request::new(pb::ValidateRequest { config: Some(root) }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.errors.is_empty());
        assert!(response.warnings.is_empty());
    }

    /// A small write fits in the kernel's socket buffer, so the host can finish its whole data stream
    /// and send `CommitWrite` while the data connection still sits un-accepted in the listener's
    /// backlog. The commit must wait for that stream, however late this side gets around to reading
    /// its ticket.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_commit_that_arrives_before_the_data_connection_is_claimed_still_completes() {
        use std::io::Write;

        let service = test_service(FixtureConnector::default());
        service
            .configure(Request::new(pb::ConfigureRequest {
                instance_id: "a".to_string(),
                config: None,
            }))
            .await
            .expect("Configure must succeed before BeginWrite can open a sink");

        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "value",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let begun = service
            .begin_write(Request::new(pb::BeginWriteRequest {
                op_id: "op1".to_string(),
                spec: Some(pb::OutputSpecMsg::default()),
                arrow_schema_ipc: encode_schema_ipc(&schema),
            }))
            .await
            .expect("BeginWrite must succeed against the fixture sink")
            .into_inner();

        // The host's whole side of the write, finished before this process accepts anything: ticket,
        // a complete Arrow IPC stream (end-of-stream marker included), half-close.
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("c.sock.data");
        let listener = tokio::net::UnixListener::bind(&data_path).unwrap();
        let mut host_side = StdUnixStream::connect(&data_path).unwrap();
        host_side.write_all(&begun.ticket).unwrap();
        host_side.write_all(&encode_schema_ipc(&schema)).unwrap();
        host_side.shutdown(Shutdown::Write).unwrap();

        // Polled exactly once: far enough to be parked on the drain, before the accept loop exists.
        let commit = service.commit_write(Request::new(pb::SessionRef {
            session_id: begun.session_id.clone(),
        }));
        tokio::pin!(commit);
        tokio::select! {
            biased;
            early = &mut commit => panic!("CommitWrite answered before any data was read: {early:?}"),
            _ = std::future::ready(()) => {}
        }

        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(data_plane::run(listener, service.tickets.clone(), stop_rx));

        let answered = tokio::time::timeout(Duration::from_secs(10), &mut commit)
            .await
            .expect("CommitWrite never answered although its data stream was complete");
        answered.expect("a completely delivered write must commit");
    }
}
