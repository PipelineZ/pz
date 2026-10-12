//! Spans and statistics for calls a connector makes to the system it talks to. [`start`] opens a
//! `remote.<operation>` client span and, when the returned [`RemoteCall`] is dropped, counts one
//! request in the session's [`stats`](crate::stats). Spans carry only the operation name and numbers,
//! never URLs, headers, SQL or error messages. At most 200 spans open per write session; further calls
//! still count as requests and are tallied as dropped spans. A call started inside a `tokio::spawn`ed
//! task loses the session's task-local and records no statistics.

use tracing::Span;

use crate::stats::{stats, Stats};

/// One remote call. Dropping it counts one request and ends its span. Drop cannot see an error in
/// flight, so call [`RemoteCall::fail`] before it when the call failed.
pub struct RemoteCall {
    span: Span,
    stats: Stats,
}

/// Starts a `remote.<operation>` span (kind client) parented on the current span. The operation
/// matches `[a-z][a-z0-9_]{0,31}`; anything else is recorded as `call`. Outside a write session the
/// call works and records no statistics.
#[must_use]
pub fn start(operation: &str) -> RemoteCall {
    start_with(operation, crate::telemetry::traces_enabled())
}

/// `start` with the trace-export gate supplied: without an exporter no span is built and the budget
/// is untouched, so `spans_dropped` only ever counts spans that would have been exported.
fn start_with(operation: &str, traces: bool) -> RemoteCall {
    let op = operation_name(operation);
    let stats = stats();
    let span = if !traces {
        Span::none()
    } else if stats.try_take_span() {
        tracing::info_span!(
            "remote",
            otel.name = %format!("remote.{op}"),
            otel.kind = "client",
            otel.status_code = tracing::field::Empty,
            pz.remote.operation = %op,
            error.type = tracing::field::Empty,
        )
    } else {
        stats.add_spans_dropped(1);
        Span::none()
    };
    RemoteCall { span, stats }
}

impl RemoteCall {
    /// Adds bytes sent or received on the wire for this call.
    pub fn wire_bytes(&self, n: i64) -> &Self {
        self.stats.add_wire_bytes(n);
        self
    }

    /// Adds milliseconds spent waiting on the remote system.
    pub fn remote_ms(&self, ms: i64) -> &Self {
        self.stats.add_remote_ms(ms);
        self
    }

    /// Counts one throttled (rate limited) request.
    pub fn throttled(&self, n: i64) -> &Self {
        self.stats.add_throttled(n);
        self
    }

    /// Adds billed cost in a unit such as `ru`; the first unit reported for a session wins.
    pub fn cost(&self, units: f64, unit: &str) -> &Self {
        self.stats.add_cost(units, unit);
        self
    }

    /// Adds a connector-specific number named `vendor.metric`.
    pub fn extra(&self, name: &str, value: f64) -> &Self {
        self.stats.add_extra(name, value);
        self
    }

    /// Marks the span failed. The error value is used only for its type name; its message is never
    /// read or recorded.
    ///
    /// Generic so the concrete type is named; a `&dyn Error` records `dyn Error`.
    pub fn fail<E: std::error::Error + ?Sized>(&self, _error: &E) {
        self.span.record("otel.status_code", "ERROR");
        self.span.record("error.type", std::any::type_name::<E>());
    }
}

impl Drop for RemoteCall {
    fn drop(&mut self) {
        self.stats.add_requests(1);
    }
}

/// The operation name to record: `op` when it matches `[a-z][a-z0-9_]{0,31}`, else `call`.
fn operation_name(op: &str) -> &str {
    let valid = op.len() <= 32
        && op.starts_with(|c: char| c.is_ascii_lowercase())
        && op
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valid {
        op
    } else {
        "call"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{scope, Stats};

    #[tokio::test]
    async fn drop_counts_one_request() {
        let s = Stats::new();
        scope(s.clone(), async {
            let call = start("query");
            call.wire_bytes(10).remote_ms(4).throttled(1);
            drop(call);
        })
        .await;
        let p = s.to_proto().unwrap();
        assert_eq!(
            (p.requests, p.wire_bytes, p.remote_ms, p.throttled),
            (1, 10, 4, 1)
        );
    }

    #[tokio::test]
    async fn cap_of_200_spans_drops_the_rest_but_counts_every_request() {
        let s = Stats::new();
        scope(s.clone(), async {
            for _ in 0..250 {
                drop(start_with("get_object", true));
            }
        })
        .await;
        let p = s.to_proto().unwrap();
        assert_eq!((p.requests, p.spans_dropped), (250, 50));
    }

    #[tokio::test]
    async fn without_trace_export_no_span_is_dropped_but_requests_count() {
        let s = Stats::new();
        scope(s.clone(), async {
            for _ in 0..250 {
                drop(start_with("get_object", false));
            }
        })
        .await;
        let p = s.to_proto().unwrap();
        assert_eq!((p.requests, p.spans_dropped), (250, 0));
    }

    #[test]
    fn invalid_operation_falls_back_to_call() {
        assert_eq!(operation_name("Bad-Op"), "call");
        assert_eq!(operation_name(""), "call");
        assert_eq!(operation_name("1x"), "call");
        assert_eq!(operation_name(&"a".repeat(33)), "call");
        assert_eq!(operation_name(&"a".repeat(32)), "a".repeat(32));
        assert_eq!(operation_name("get_object2"), "get_object2");
    }

    #[tokio::test]
    async fn cost_and_extra_feed_the_stats() {
        let s = Stats::new();
        scope(s.clone(), async {
            start("q").cost(2.0, "ru").extra("v.m", 1.0);
        })
        .await;
        let p = s.to_proto().unwrap();
        assert_eq!((p.cost_units, p.cost_unit.as_str()), (2.0, "ru"));
        assert_eq!(p.extras["v.m"], 1.0);
    }

    #[test]
    fn outside_a_scope_nothing_panics() {
        let call = start("q");
        call.fail(&std::io::Error::other("secret"));
    }
}
