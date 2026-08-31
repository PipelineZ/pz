use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

use crate::pb::PzErrorDetail;

/// Fixed v1 protocol constant, mirrored from `Pz.Connectors.Protocol.ProtocolConstants` (that C# type is
/// the source of truth for the VALUE; this crate has no way to reference it directly across languages).
pub(crate) const ERROR_DETAIL_TRAILER_KEY: &str = "pz-error-bin";

/// An operational failure a connector reports back to the host: something about the destination or the
/// data, not a protocol violation. Serialized into the `pz-error-bin` gRPC trailer (a binary-encoded
/// `PzErrorDetail`) so the host can rebuild a real host-side exception with transience and retry-after
/// intact, rather than guessing from a bare gRPC status code -- the trailer is the contract, never the
/// status code, which exists only for readable logs.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PzError {
    /// `PZ####` where this connector has an assigned code, empty otherwise.
    pub code: Option<String>,
    pub message: String,
    pub is_transient: bool,
    /// Absent means "no retry-after was set", never conflated with an immediate (zero-delay) retry.
    pub retry_after_ms: Option<i64>,
    pub hint: Option<String>,
}

impl PzError {
    pub fn new(message: impl Into<String>) -> Self {
        PzError {
            message: message.into(),
            ..Default::default()
        }
    }

    pub fn transient(message: impl Into<String>, retry_after_ms: i64) -> Self {
        PzError {
            message: message.into(),
            is_transient: true,
            retry_after_ms: Some(retry_after_ms),
            ..Default::default()
        }
    }
}

impl std::fmt::Display for PzError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PzError {}

/// Builds the `RpcException` shape the host's `PcpClient.MapRpcException` expects: a gRPC status
/// (`Unavailable` for a transient failure, `FailedPrecondition` otherwise -- chosen only for readable
/// logs, since the host decides by trailer presence, not status code) carrying the prost-encoded
/// `PzErrorDetail` under the `pz-error-bin` binary trailer key.
pub(crate) fn to_status(err: &PzError) -> Status {
    let detail = PzErrorDetail {
        code: err.code.clone().unwrap_or_default(),
        message: err.message.clone(),
        is_transient: err.is_transient,
        // Absent (None) and Some(0) both cross the wire as 0 -- "no retry-after was set" is the
        // ABI's own contract for this field, not a distinction this crate's wire shape can carry.
        retry_after_ms: err.retry_after_ms.unwrap_or(0),
        hint: err.hint.clone().unwrap_or_default(),
    };

    let code = if err.is_transient {
        Code::Unavailable
    } else {
        Code::FailedPrecondition
    };
    let mut status = Status::new(code, err.message.clone());
    let bytes = detail.encode_to_vec();
    // A key ending in "-bin" is required to carry binary (non-UTF8-safe) metadata over gRPC.
    status
        .metadata_mut()
        .insert_bin(ERROR_DETAIL_TRAILER_KEY, MetadataValue::from_bytes(&bytes));
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trailer_round_trips_through_prost() {
        let err = PzError {
            code: Some("PZ0999".to_string()),
            message: "destination unreachable".to_string(),
            is_transient: true,
            retry_after_ms: Some(250),
            hint: Some("retry shortly".to_string()),
        };

        let status = to_status(&err);
        assert_eq!(status.code(), Code::Unavailable);

        let trailer = status
            .metadata()
            .get_bin(ERROR_DETAIL_TRAILER_KEY)
            .expect("pz-error-bin trailer present");
        let bytes = trailer.to_bytes().expect("valid binary metadata value");
        let decoded = PzErrorDetail::decode(bytes.as_ref()).expect("valid PzErrorDetail bytes");

        assert_eq!(decoded.code, "PZ0999");
        assert_eq!(decoded.message, "destination unreachable");
        assert!(decoded.is_transient);
        assert_eq!(decoded.retry_after_ms, 250);
        assert_eq!(decoded.hint, "retry shortly");
    }

    #[test]
    fn absent_retry_after_crosses_as_zero_not_missing() {
        let err = PzError::new("permanent failure");
        let status = to_status(&err);
        assert_eq!(status.code(), Code::FailedPrecondition);

        let trailer = status.metadata().get_bin(ERROR_DETAIL_TRAILER_KEY).unwrap();
        let decoded = PzErrorDetail::decode(trailer.to_bytes().unwrap().as_ref()).unwrap();
        assert_eq!(decoded.retry_after_ms, 0);
        assert!(!decoded.is_transient);
    }
}
