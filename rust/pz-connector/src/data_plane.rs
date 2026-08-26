use std::os::unix::net::UnixStream as StdUnixStream;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};

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

    // arrow-ipc's reader is synchronous (std::io::Read, no async support in this arrow version), so the
    // rest of this connection's life runs on a blocking-pool thread: the async socket converts to a std
    // one and is driven with ordinary blocking reads, the same pattern any sync-only library is bridged
    // into a tokio runtime with.
    let Ok(std_stream) = stream.into_std() else {
        return;
    };
    if std_stream.set_nonblocking(false).is_err() {
        return;
    }

    let handle = tokio::runtime::Handle::current();
    let _ = tokio::task::spawn_blocking(move || serve_write_blocking(std_stream, session, &handle))
        .await;
}

fn serve_write_blocking(
    stream: StdUnixStream,
    session: Arc<SessionState>,
    handle: &tokio::runtime::Handle,
) {
    if let Ok(clone) = stream.try_clone() {
        session.attach_data_conn(clone);
    }

    let result = drain(stream, &session, handle);
    session.signal_drained(result);
}

/// Reads the write's Arrow IPC stream to completion, forwarding each batch into the open session.
/// Returns `Ok(())` only once the stream ends (cleanly, via the IPC end-of-stream marker, or because the
/// host half-closed after its last batch) with every batch accepted; any read/decode failure or a
/// rejected batch stops the drain immediately and reports why -- `CommitWrite` (which awaits this
/// result) must never see a torn stream as success.
fn drain(
    stream: StdUnixStream,
    session: &SessionState,
    handle: &tokio::runtime::Handle,
) -> Result<(), PzError> {
    let reader = StreamReader::try_new(stream, None)
        .map_err(|e| PzError::new(format!("failed to open the Arrow IPC write stream: {e}")))?;

    for batch in reader {
        let batch =
            batch.map_err(|e| PzError::new(format!("write stream failed mid-message: {e}")))?;
        handle.block_on(session.write_batch(batch))?;
    }

    Ok(())
}
