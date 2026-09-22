//! The PCP socket transport: AF_UNIX stream sockets on every platform, because that is what the host
//! dials (`control.sock` and `control.sock.data` in a directory it created owner-only). Everything
//! else in this crate reaches a socket only through the names below, so the one platform split lives
//! here.
//!
//! - [`Listener`]: binds a path and accepts asynchronously.
//! - [`Stream`]: one accepted connection, `AsyncRead + AsyncWrite` for tonic and the data plane.
//! - [`BlockingStream`]: the same connection after [`into_blocking`], for arrow-ipc's synchronous
//!   reader.
//! - [`DataConn`]: a blocking data connection shared between the thread reading it and whoever may
//!   have to end that read early.

use std::io::{self, Read};
use std::net::Shutdown;
use std::sync::Arc;

pub(crate) use imp::*;

impl Listener {
    pub(crate) async fn accept(&self) -> io::Result<Stream> {
        std::future::poll_fn(|cx| self.poll_accept(cx)).await
    }
}

/// One handle, not a clone: on Windows only an operation on the very handle a read is blocked on can
/// end that read (see [`DataConn::unblock`]), and a `try_clone` is a different handle there.
#[derive(Clone)]
pub(crate) struct DataConn(Arc<BlockingStream>);

impl DataConn {
    pub(crate) fn new(stream: BlockingStream) -> Self {
        Self(Arc::new(stream))
    }

    /// Makes a read blocked on this connection return, and every later read return end-of-stream.
    /// `shutdown` alone does the first on unix, but a Windows AF_UNIX `recv` already in progress
    /// ignores it and only returns when that I/O is cancelled. Shutting down first leaves no gap: a
    /// read that starts after it sees end-of-stream, and one already waiting is cancelled after it.
    pub(crate) fn unblock(&self) {
        let _ = self.0.shutdown(Shutdown::Both);
        #[cfg(windows)]
        cancel_pending_io(&self.0);
    }
}

impl Read for DataConn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self.0).read(buf)
    }
}

#[cfg(unix)]
mod imp {
    use std::io;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::task::{Context, Poll};

    pub(crate) type Stream = tokio::net::UnixStream;
    pub(crate) type BlockingStream = std::os::unix::net::UnixStream;

    pub(crate) struct Listener(tokio::net::UnixListener);

    impl Listener {
        pub(crate) fn bind(path: &Path) -> io::Result<Self> {
            tokio::net::UnixListener::bind(path).map(Self)
        }

        pub(crate) fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<Stream>> {
            self.0.poll_accept(cx).map_ok(|(stream, _)| stream)
        }
    }

    pub(crate) fn into_blocking(stream: Stream) -> io::Result<BlockingStream> {
        let stream = stream.into_std()?;
        stream.set_nonblocking(false)?;
        Ok(stream)
    }

    /// A unix socket's file mode is the whole access control on this transport, and the socket
    /// carries credentials in one direction and data in the other.
    pub(crate) fn restrict_to_owner(path: &Path) -> io::Result<()> {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
    }

    #[cfg(test)]
    pub(crate) async fn connect(path: &Path) -> io::Result<Stream> {
        tokio::net::UnixStream::connect(path).await
    }

    #[cfg(test)]
    pub(crate) fn connect_blocking(path: &Path) -> io::Result<BlockingStream> {
        BlockingStream::connect(path)
    }
}

/// Tokio exposes AF_UNIX only on unix, but a Windows AF_UNIX socket is an ordinary AFD socket, and
/// tokio's Windows reactor polls any AFD socket regardless of address family. So each socket is carried
/// in tokio's TCP type, which only ever issues `recv`/`send` and readiness polls on it. The address
/// methods of that type parse the peer address as IP and fail on an AF_UNIX socket, so nothing here
/// calls them -- `accept` goes through socket2, which reads the address generically.
#[cfg(windows)]
mod imp {
    use std::io;
    use std::os::windows::io::{FromRawSocket, IntoRawSocket};
    use std::path::Path;
    use std::task::{ready, Context, Poll};

    use socket2::{Domain, SockAddr, SockRef, Socket, Type};
    use tokio::io::Interest;

    pub(crate) type Stream = tokio::net::TcpStream;
    pub(crate) type BlockingStream = std::net::TcpStream;

    /// The listening socket, registered with the reactor only so `accept` can await readiness.
    pub(crate) struct Listener(tokio::net::TcpStream);

    /// tokio's own `UnixListener` backlog.
    const BACKLOG: i32 = 1024;

    impl Listener {
        pub(crate) fn bind(path: &Path) -> io::Result<Self> {
            let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
            socket.bind(&SockAddr::unix(path)?)?;
            socket.listen(BACKLOG)?;
            register(socket).map(Self)
        }

        pub(crate) fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<Stream>> {
            loop {
                ready!(self.0.poll_read_ready(cx))?;
                // `try_io` clears the readiness on `WouldBlock` (another waiter took the connection),
                // so the next `poll_read_ready` parks instead of spinning.
                match self
                    .0
                    .try_io(Interest::READABLE, || SockRef::from(&self.0).accept())
                {
                    Ok((accepted, _)) => return Poll::Ready(register(accepted)),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
        }
    }

    fn register(socket: Socket) -> io::Result<tokio::net::TcpStream> {
        socket.set_nonblocking(true)?;
        // SAFETY: `into_raw_socket` gives up ownership, so the std type below becomes the socket's
        // only owner.
        let std = unsafe { std::net::TcpStream::from_raw_socket(socket.into_raw_socket()) };
        tokio::net::TcpStream::from_std(std)
    }

    pub(crate) fn into_blocking(stream: Stream) -> io::Result<BlockingStream> {
        let stream = stream.into_std()?;
        stream.set_nonblocking(false)?;
        Ok(stream)
    }

    /// A Windows AF_UNIX socket file takes its DACL from its directory, which the host created
    /// owner-only (protected, one rule for the current user) before spawning this process. There is
    /// no mode to set on the file itself.
    pub(crate) fn restrict_to_owner(_path: &Path) -> io::Result<()> {
        Ok(())
    }

    /// Best-effort, like the `shutdown` before it: a failure means nothing was pending to cancel.
    pub(crate) fn cancel_pending_io(stream: &BlockingStream) {
        use std::os::windows::io::AsRawSocket;

        // SAFETY: the socket stays open for the whole call because `stream` borrows its only owner,
        // and a null OVERLAPPED asks for every pending I/O on the handle, from any thread.
        unsafe {
            windows_sys::Win32::System::IO::CancelIoEx(
                stream.as_raw_socket() as usize as windows_sys::Win32::Foundation::HANDLE,
                std::ptr::null(),
            );
        }
    }

    #[cfg(test)]
    pub(crate) async fn connect(path: &Path) -> io::Result<Stream> {
        let path = path.to_path_buf();
        let socket = tokio::task::spawn_blocking(move || connect_socket(&path))
            .await
            .map_err(io::Error::other)??;
        register(socket)
    }

    #[cfg(test)]
    pub(crate) fn connect_blocking(path: &Path) -> io::Result<BlockingStream> {
        let socket = connect_socket(path)?;
        // SAFETY: as in `register`.
        Ok(unsafe { std::net::TcpStream::from_raw_socket(socket.into_raw_socket()) })
    }

    #[cfg(test)]
    fn connect_socket(path: &Path) -> io::Result<Socket> {
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        socket.connect(&SockAddr::unix(path)?)?;
        Ok(socket)
    }
}

/// A connected pair for tests that need both ends of one connection. Unix has `socketpair`; Windows
/// AF_UNIX does not, so it binds a listener in a scratch directory and dials it.
#[cfg(test)]
pub(crate) fn blocking_pair() -> std::io::Result<(BlockingStream, BlockingStream)> {
    #[cfg(unix)]
    {
        BlockingStream::pair()
    }
    #[cfg(windows)]
    {
        use socket2::{Domain, SockAddr, Socket, Type};
        use std::os::windows::io::{FromRawSocket, IntoRawSocket};

        let dir = tempfile::tempdir()?;
        let addr = SockAddr::unix(dir.path().join("pair.sock"))?;
        let listener = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        listener.bind(&addr)?;
        listener.listen(1)?;
        let dialer = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        dialer.connect(&addr)?;
        let (accepted, _) = listener.accept()?;
        // SAFETY: each `into_raw_socket` gives up ownership to the one std value built from it.
        Ok(unsafe {
            (
                BlockingStream::from_raw_socket(dialer.into_raw_socket()),
                BlockingStream::from_raw_socket(accepted.into_raw_socket()),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    fn read_once(mut conn: DataConn) -> io::Result<usize> {
        let mut buf = [0u8; 16];
        conn.read(&mut buf)
    }

    /// `SessionState::force_unblock` ends a drain the host will never finish. The peer here stays
    /// open and silent, so only `unblock` can end the read. The pause gives the read time to be
    /// waiting inside the socket before `unblock` runs -- the case plain `shutdown` misses on
    /// Windows -- and the assertion holds whichever side wins.
    #[test]
    fn unblock_ends_a_read_already_waiting() {
        let (_peer, stream) = blocking_pair().unwrap();
        let conn = DataConn::new(stream);

        let (done_tx, done_rx) = mpsc::channel();
        let reader_conn = conn.clone();
        let reader = std::thread::spawn(move || {
            let _ = done_tx.send(read_once(reader_conn).map_err(|e| e.kind()));
        });

        assert!(
            done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the read must wait while the peer is open and silent"
        );
        conn.unblock();
        let ended = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the waiting read never returned after unblock");
        assert!(
            matches!(ended, Ok(0) | Err(_)),
            "an unblocked connection must read as closed or failed, got {ended:?}"
        );
        reader.join().unwrap();
    }

    #[test]
    fn a_read_after_unblock_sees_end_of_stream() {
        let (_peer, stream) = blocking_pair().unwrap();
        let conn = DataConn::new(stream);
        conn.unblock();
        assert_eq!(read_once(conn).unwrap(), 0);
    }
}
