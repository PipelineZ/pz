//! The OTLP/HTTP client a connector exports through. It wraps reqwest's blocking client (the exporters run on their
//! own threads, never on the tokio runtime) and, when the host named a headers file, reads that file just before every
//! request, so a token the host rewrites mid-run is used by the next export. A missing, unreadable or malformed file
//! sends the request without it and says so once on stderr, naming the file and the problem, never its content.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use http::{HeaderName, HeaderValue};
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};

pub(crate) struct HeadersFileClient {
    inner: reqwest::blocking::Client,
    path: Option<PathBuf>,
    noted: AtomicBool,
}

impl std::fmt::Debug for HeadersFileClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeadersFileClient")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl HeadersFileClient {
    pub(crate) fn new(path: Option<PathBuf>, timeout: Duration) -> Result<Self, String> {
        // reqwest is built without a bundled crypto provider (ring is far smaller than aws-lc-rs in every connector
        // binary); install ring as the process default unless the connector already chose one.
        let _ = rustls::crypto::ring::default_provider().install_default();
        // reqwest's blocking client panics when it is built inside an async runtime, and the handshake runs in one.
        let inner = std::thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()
        })
        .join()
        .map_err(|_| "building the OTLP/HTTP client panicked".to_string())?
        .map_err(|e| format!("could not build the OTLP/HTTP client: {e}"))?;
        Ok(Self {
            inner,
            path,
            noted: AtomicBool::new(false),
        })
    }

    /// Sets the file's headers on `request`, read now. Returns the stderr note when this call is the one that reported
    /// a problem (once per process), for tests; the note never carries a header value.
    pub(crate) fn apply(&self, request: &mut Request<Bytes>) -> Option<String> {
        let path = self.path.as_ref()?;
        match read_headers(path) {
            Ok(headers) => {
                for (name, value) in headers {
                    request.headers_mut().insert(name, value);
                }
                None
            }
            Err(problem) => {
                if self.noted.swap(true, Ordering::AcqRel) {
                    return None;
                }
                let note = format!(
                    "telemetry headers file '{}' {problem}; exporting without it",
                    path.display()
                );
                eprintln!("pz-connector: telemetry: {note}");
                Some(note)
            }
        }
    }
}

/// `Name=value` lines; blank lines and `#` comments are skipped. The error names the problem only.
fn read_headers(path: &Path) -> Result<Vec<(HeaderName, HeaderValue)>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("could not be read ({:?})", e.kind()))?;
    let mut headers = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parsed = line.split_once('=').and_then(|(name, value)| {
            Some((
                HeaderName::from_bytes(name.trim().as_bytes()).ok()?,
                HeaderValue::from_str(value.trim()).ok()?,
            ))
        });
        match parsed {
            Some(header) => headers.push(header),
            None => return Err(format!("line {} is not Name=value", i + 1)),
        }
    }
    Ok(headers)
}

#[async_trait]
impl HttpClient for HeadersFileClient {
    async fn send_bytes(&self, mut request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        self.apply(&mut request);
        self.inner.send_bytes(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> Request<Bytes> {
        Request::builder()
            .uri("http://x/v1/traces")
            .body(Bytes::new())
            .unwrap()
    }

    fn client(path: &Path) -> HeadersFileClient {
        HeadersFileClient::new(Some(path.to_path_buf()), Duration::from_secs(1)).unwrap()
    }

    #[test]
    fn the_file_is_read_before_every_request() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h");
        std::fs::write(&path, "# token\nAuthorization=Bearer one\n\n").unwrap();
        let client = client(&path);

        let mut first = request();
        assert_eq!(client.apply(&mut first), None);
        std::fs::write(&path, "Authorization=Bearer two\n").unwrap();
        let mut second = request();
        client.apply(&mut second);

        assert_eq!(first.headers()["authorization"], "Bearer one");
        assert_eq!(second.headers()["authorization"], "Bearer two");
    }

    #[test]
    fn a_malformed_file_sends_without_it_and_notes_once_without_the_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h");
        std::fs::write(&path, "Authorization Bearer secret-value\n").unwrap();
        let client = client(&path);

        let mut first = request();
        let note = client
            .apply(&mut first)
            .expect("the first failure is reported");
        assert!(!note.contains("secret-value"), "{note}");
        assert!(first.headers().get("authorization").is_none());
        assert_eq!(client.apply(&mut request()), None, "reported once only");
    }

    #[test]
    fn a_missing_file_is_reported_by_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent");
        let note = client(&path).apply(&mut request()).unwrap();
        assert!(note.contains("absent"), "{note}");
    }
}
