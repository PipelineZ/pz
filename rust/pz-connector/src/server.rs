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

use crate::config::Config;
use crate::data_plane;
use crate::error::{to_status, PzError};
use crate::pb;
use crate::pb::pz_connector_server::{PzConnector, PzConnectorServer};
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
/// values the host ABI defines), and the two JSON Schema strings the host surfaces to authoring tools.
pub struct ConnectorDecl {
    pub name: &'static str,
    pub version: &'static str,
    pub capabilities: u64,
    pub connection_config_schema: &'static str,
    pub dataset_config_schema: &'static str,
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
    /// Network-touching connectivity check. `Ok(())` is the only success shape this trait can express;
    /// anything else reported here crosses the wire as an operational failure (`pz-error-bin` trailer),
    /// never a soft `ok: false` result -- that distinction is not exposed to Rust connector authors in
    /// v1.
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
    /// host) so `commit_write`/`abort_write` can revoke it -- see `TicketRegistry::revoke`'s doc for why
    /// a session that finishes before its ticket is ever presented would otherwise leave that ticket
    /// live forever.
    ticket: [u8; TICKET_LENGTH],
    write_session: AsyncMutex<Option<Box<dyn WriteSession>>>,
    drained_tx: StdMutex<Option<oneshot::Sender<Result<(), PzError>>>>,
    drained_rx: AsyncMutex<Option<oneshot::Receiver<Result<(), PzError>>>>,
    /// A clone of the connected data-plane socket, attached once the pump claims it. `AbortWrite`/
    /// `Cancel`/a `Shutdown`-triggered sweep use it to force a blocking read to fail, unblocking a pump
    /// that would otherwise wait forever for bytes the host is never going to send.
    data_conn: StdMutex<Option<StdUnixStream>>,
}

impl SessionState {
    fn new(
        op_id: String,
        ticket: [u8; TICKET_LENGTH],
        session: Box<dyn WriteSession>,
    ) -> Arc<Self> {
        let (tx, rx) = oneshot::channel();
        Arc::new(SessionState {
            op_id,
            ticket,
            write_session: AsyncMutex::new(Some(session)),
            drained_tx: StdMutex::new(Some(tx)),
            drained_rx: AsyncMutex::new(Some(rx)),
            data_conn: StdMutex::new(None),
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

    pub(crate) fn signal_drained(&self, result: Result<(), PzError>) {
        if let Some(tx) = self.drained_tx.lock().unwrap().take() {
            let _ = tx.send(result);
        }
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
        let host_major = request.into_inner().protocol_major;
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

        Ok(Response::new(pb::Hello {
            info: Some(pb::ConnectorInfoMsg {
                name: self.decl.name.to_string(),
                version: self.decl.version.to_string(),
                protocol_major: PROTOCOL_MAJOR,
            }),
            capabilities: self.decl.capabilities as i64,
            connection_config_schema: self.decl.connection_config_schema.to_string(),
            dataset_config_schema: self.decl.dataset_config_schema.to_string(),
            transports: vec![TRANSPORT_PIPE.to_string()],
        }))
    }

    async fn configure(
        &self,
        request: Request<pb::ConfigureRequest>,
    ) -> Result<Response<pb::ConfigureResponse>, Status> {
        let msg = request.into_inner();
        *self.config.lock().unwrap() = Some(Config::from_struct(msg.config.as_ref()));
        Ok(Response::new(pb::ConfigureResponse {}))
    }

    async fn validate(
        &self,
        request: Request<pb::ValidateRequest>,
    ) -> Result<Response<pb::ValidationResultMsg>, Status> {
        let config = Config::from_struct(request.into_inner().config.as_ref());
        let errors = self.connector.validate(&config).await;
        Ok(Response::new(pb::ValidationResultMsg { errors }))
    }

    async fn check_connection(
        &self,
        request: Request<pb::CheckRequest>,
    ) -> Result<Response<pb::ConnectionCheckMsg>, Status> {
        let config = Config::from_struct(request.into_inner().config.as_ref());
        match self.connector.check(&config).await {
            Ok(()) => Ok(Response::new(pb::ConnectionCheckMsg {
                ok: true,
                message: None,
            })),
            Err(e) => Err(to_status(&e)),
        }
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
        // baked into `SessionState` itself (see its `ticket` field's doc) so `commit_write`/
        // `abort_write` can revoke it later, and that means the bytes have to exist before the `Arc`
        // the registry entry wraps does.
        let ticket_bytes = TicketRegistry::generate();
        let state = SessionState::new(msg.op_id, ticket_bytes, session);
        self.sessions
            .lock()
            .unwrap()
            .insert(session_id.clone(), state.clone());
        self.tickets.insert(ticket_bytes, TicketEntry::Write(state));

        Ok(Response::new(pb::WriteSessionTicket {
            session_id,
            ticket: ticket_bytes.to_vec(),
            // The trait surface this SDK exposes has no way for a connector author to declare anything
            // else yet -- DiscardsAll is the ABI's own default, and every PCP sink looks like it until
            // this is threaded through.
            abort_semantics: pb::AbortSemanticsMsg::AbortSemanticsDiscardsAll as i32,
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

        // Revoked as soon as the control plane claims this session for finalization, whether or not the
        // data connection ever burned it itself (the premature-commit case: no data connection ever
        // opened at all). Without this a session that finishes control-plane-first leaves its ticket
        // live forever -- a later connection presenting it would reach a `SessionState` already taken
        // apart. See `TicketRegistry::revoke`'s doc.
        self.tickets.revoke(&state.ticket());

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

        // See commit_write's identical revoke: a session that finishes control-plane-first must not
        // leave a live ticket a later connection could still present.
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
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<pb::HostChannelUp, Status>>(1);
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
            drop(tx);
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
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
    let socket_path = parse_socket_arg(std::env::args().skip(1))
        .map_err(|msg| anyhow::Error::new(ServeExit::UsageError(msg)))?;

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
    let service = PzConnectorService {
        decl,
        connector,
        config: StdMutex::new(None),
        sink: AsyncMutex::new(None),
        sessions: sessions.clone(),
        tickets: tickets.clone(),
        shutdown_tx: shutdown_tx.clone(),
        shutdown_rx: shutdown_rx.clone(),
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

    serve_result.map_err(|e| anyhow::anyhow!("control-plane server failed: {e}"))?;
    Ok("received the Shutdown RPC (or the control-plane listener otherwise stopped)".to_string())
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
}
