use std::cell::RefCell;
use std::io::{self, Read};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::error::PzError;
use crate::server::SessionState;
use crate::ticket::{TicketEntry, TicketRegistry, TICKET_LENGTH};

/// The data-plane half of PCP: a bare accept loop on `<control socket>.data` that speaks Arrow IPC and
/// nothing else. Nothing on this socket is protobuf-framed and nothing here reads configuration -- a
/// connection carries a 16-byte ticket preamble, and everything needed to serve it was decided on the
/// control plane when that ticket was minted. The host always dials; this side never has to locate
/// anything.
pub(crate) async fn run(
    listener: UnixListener,
    tickets: Arc<TicketRegistry>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let tickets = tickets.clone();
                tokio::spawn(serve_connection(stream, tickets));
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn serve_connection(mut stream: UnixStream, tickets: Arc<TicketRegistry>) {
    let mut ticket = [0u8; TICKET_LENGTH];
    if stream.read_exact(&mut ticket).await.is_err() {
        return;
    }

    let Some(entry) = tickets.burn(&ticket) else {
        // Unknown or already-burned ticket: close without reading further. A protocol violation is
        // never answered with a diagnosis on this socket -- the host learns only that the peer hung up.
        return;
    };

    let TicketEntry::Write(session) = entry;

    // Every early-return from here on happens AFTER the ticket resolved to a real session, so every one
    // of them must signal `drained` explicitly -- otherwise a `CommitWrite` awaiting this session's
    // drained receiver would hang on a sender that was silently dropped instead of failing cleanly.
    let std_stream = match stream.into_std() {
        Ok(s) => s,
        Err(e) => {
            session.signal_drained(Err(PzError::new(format!(
                "failed to hand the data connection to the blocking Arrow IPC reader: {e}"
            ))));
            return;
        }
    };
    if let Err(e) = std_stream.set_nonblocking(false) {
        session.signal_drained(Err(PzError::new(format!(
            "failed to configure the data connection for blocking reads: {e}"
        ))));
        return;
    }

    // arrow-ipc's reader is synchronous (std::io::Read, no async support in this arrow version), so the
    // blocking read loop runs on a dedicated blocking-pool thread and hands each decoded batch to this
    // async task over a channel -- never via `Handle::block_on` from inside that thread, which would
    // deadlock a connector author's `#[tokio::main(flavor = "current_thread")]` runtime (there would be
    // no free worker thread left to drive the very future being blocked on).
    let (tx, rx) = mpsc::channel::<PumpMessage>(4);
    let pump_session = session.clone();
    let reader_thread = tokio::task::spawn_blocking(move || read_to_end(std_stream, session, tx));
    pump_batches(&pump_session, rx).await;
    if let Err(join_err) = reader_thread.await {
        // The pump has already signaled a drained result from whatever the reader produced before this
        // -- a panic here is unusual enough that stderr, not a second (impossible -- the sender side is
        // already gone) signal, is the right place for it.
        eprintln!("pz-connector: data-plane reader thread failed: {join_err}");
    }
}

/// One message the blocking reader thread hands to the async pump task: a decoded batch, or the final
/// verdict once the read loop has ended (successfully or not).
enum PumpMessage {
    Batch(RecordBatch),
    Done(Result<(), PzError>),
}

/// Consumes `rx` until the reader thread reports its final verdict (or the channel closes some other
/// way), forwarding each batch into the session and signaling `drained` exactly once at the end.
///
/// Defaults to a failure ("ended without reporting") rather than success: a reader thread that panics or
/// is dropped before ever sending [`PumpMessage::Done`] must never be mistaken for a clean drain.
async fn pump_batches(session: &SessionState, mut rx: mpsc::Receiver<PumpMessage>) {
    let mut result: Result<(), PzError> = Err(PzError::new(
        "the data-plane reader ended without reporting a result (it may have panicked or been killed)",
    ));

    while let Some(message) = rx.recv().await {
        match message {
            PumpMessage::Batch(batch) => {
                if let Err(e) = session.write_batch(batch).await {
                    result = Err(e);
                    // Dropping `rx` here is what tells the reader thread's next `blocking_send` (if any)
                    // that nobody is listening anymore, so a session-level failure stops the read loop
                    // instead of draining batches nobody will ever accept.
                    break;
                }
            }
            PumpMessage::Done(verdict) => {
                result = verdict;
                break;
            }
        }
    }

    session.signal_drained(result);
}

/// Reads the write's Arrow IPC stream to completion on a blocking thread, forwarding each decoded batch
/// to `tx`. Returns/sends `Ok(())` only when the stream ends via the Arrow IPC end-of-stream marker;
/// `arrow-ipc`'s `StreamReader` returns a clean `None` for an abrupt close at a message boundary exactly
/// as it does for a proper end-of-stream (both look like "no more bytes" to it), so this wraps the
/// stream in a tail-tracking reader and refuses to call anything short of that marker a drain -- a
/// truncated write (peer died mid-stream, or `Cancel`/`Shutdown` force-closing the socket) must fail
/// `CommitWrite`, never commit a partial write as a success.
fn read_to_end(stream: StdUnixStream, session: Arc<SessionState>, tx: mpsc::Sender<PumpMessage>) {
    if let Ok(clone) = stream.try_clone() {
        session.attach_data_conn(clone);
    }

    let result = drain(stream, &tx);
    // `tx` may already be closed (the pump gave up after a batch failure) -- a failed send here is
    // silently discarded, exactly as it would be if the pump had simply stopped listening a message
    // earlier; `pump_batches`'s own result already reflects the real failure in that case.
    let _ = tx.blocking_send(PumpMessage::Done(result));
}

fn drain(stream: StdUnixStream, tx: &mpsc::Sender<PumpMessage>) -> Result<(), PzError> {
    let tail = Rc::new(RefCell::new(TailState::default()));
    let tracked = TailTrackingReader {
        inner: stream,
        tail: tail.clone(),
    };
    let reader = StreamReader::try_new(tracked, None)
        .map_err(|e| PzError::new(format!("failed to open the Arrow IPC write stream: {e}")))?;

    for batch in reader {
        let batch =
            batch.map_err(|e| PzError::new(format!("write stream failed mid-message: {e}")))?;
        if tx.blocking_send(PumpMessage::Batch(batch)).is_err() {
            return Err(PzError::new(
                "write session was closed while its data stream was still being read",
            ));
        }
    }

    if !tail.borrow().ends_with_eos_marker() {
        return Err(PzError::new(
            "write stream ended without the Arrow IPC end-of-stream marker -- the host crashed, was \
             cancelled, or aborted mid-stream rather than completing the write",
        ));
    }

    Ok(())
}

/// The Arrow IPC end-of-stream marker: the continuation token (`0xFFFFFFFF`) followed by a zero
/// metadata length -- exactly 8 bytes, and the only way a write stream may legitimately end. Mirrors the
/// host's own `DataPlane.TailTrackingStream` (`src/Pz.PackageManagement/ProcessHosting/DataPlane.cs`),
/// which exists for the identical reason on the read side of this same ambiguity.
const EOS_MARKER: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

/// Remembers the last 8 bytes delivered to a reader without buffering or otherwise altering the stream.
/// Its only purpose is telling apart the two shapes `arrow-ipc`'s `StreamReader` cannot distinguish on
/// its own: a stream that ends at a message boundary because the writer sent the proper end-of-stream
/// marker, versus one that ends there because the peer simply stopped sending (crashed, was killed, was
/// force-closed) between two well-formed messages -- both make the reader's iterator yield `None`.
#[derive(Default)]
struct TailState {
    bytes: [u8; 8],
    filled: usize,
}

impl TailState {
    fn ends_with_eos_marker(&self) -> bool {
        self.filled == 8 && self.bytes == EOS_MARKER
    }

    /// Folds `data` into the trailing-8-bytes window in delivery order. A chunk of 8 or more bytes
    /// replaces the window outright (its own last 8 bytes); a smaller chunk shifts the window left and
    /// appends.
    fn record(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        if data.len() >= 8 {
            self.bytes.copy_from_slice(&data[data.len() - 8..]);
            self.filled = 8;
            return;
        }

        let shift = data.len();
        let keep = 8 - shift;
        self.bytes.copy_within(shift..8, 0);
        self.bytes[keep..].copy_from_slice(data);
        self.filled = (self.filled + shift).min(8);
    }
}

struct TailTrackingReader<R> {
    inner: R,
    tail: Rc<RefCell<TailState>>,
}

impl<R: Read> Read for TailTrackingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.tail.borrow_mut().record(&buf[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixStream;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;

    use super::*;

    fn test_schema() -> Schema {
        Schema::new(vec![Field::new("value", DataType::Int64, false)])
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(test_schema());
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    /// A `PzError` message string is all a test needs to assert on -- deliberately not any richer
    /// matcher, since `drain`'s exact wording is not part of any contract.
    fn drain_result(reader_side: UnixStream, tx: mpsc::Sender<PumpMessage>) -> Result<(), PzError> {
        drain(reader_side, &tx)
    }

    #[test]
    fn a_stream_missing_the_eos_marker_fails_the_drain() {
        let (mut writer_side, reader_side) = UnixStream::pair().unwrap();
        let schema = test_schema();
        {
            // WriteStart + one batch, but deliberately never `.finish()` -- no end-of-stream marker is
            // ever written, exactly the shape a killed/cancelled peer leaves mid-stream.
            let mut writer = StreamWriter::try_new(&mut writer_side, &schema).unwrap();
            writer.write(&test_batch()).unwrap();
            writer.flush().unwrap();
        }
        drop(writer_side); // abrupt close at a message boundary, no EOS marker

        let (tx, mut rx) = mpsc::channel(4);
        let result = drain_result(reader_side, tx);

        // Drain the channel so the test doesn't rely on the sender's buffer capacity.
        while rx.try_recv().is_ok() {}

        let err = result.expect_err("a stream cut off without the EOS marker must fail the drain");
        assert!(
            err.message.contains("end-of-stream marker"),
            "unexpected error message: {}",
            err.message
        );
    }

    #[test]
    fn a_stream_with_the_eos_marker_drains_cleanly() {
        let (mut writer_side, reader_side) = UnixStream::pair().unwrap();
        let schema = test_schema();
        {
            let mut writer = StreamWriter::try_new(&mut writer_side, &schema).unwrap();
            writer.write(&test_batch()).unwrap();
            writer.finish().unwrap(); // writes the Arrow IPC end-of-stream marker
        }
        drop(writer_side);

        let (tx, mut rx) = mpsc::channel(4);
        let result = drain_result(reader_side, tx);
        while rx.try_recv().is_ok() {}

        assert!(
            result.is_ok(),
            "a properly terminated stream must drain cleanly: {result:?}"
        );
    }
}
