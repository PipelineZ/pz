//! SDK for writing PipelineZ (pz) out-of-process connectors (PCP) in Rust: [`serve_sink`] parses
//! `--pz-socket`, serves the `PzConnector` gRPC control plane on that Unix socket (mode 0600) and the
//! raw Arrow IPC data plane on `<socket>.data`, and dispatches every RPC to a [`SinkConnector`]/[`Sink`]/
//! [`WriteSession`] the connector author implements. Source support is deferred (additive) -- the wire
//! protocol already covers it, only this crate's trait surface does not yet.
//!
//! # Telemetry
//!
//! Providers are built only when the host passes an OTLP endpoint in the handshake (`pz run
//! --otel-endpoint`, or `PZ_OTEL_ENDPOINT`); with none, nothing is installed and nothing is exported.
//! When there is one, this crate opens a `pcp.<Rpc>` server span per control-plane RPC and a
//! `pcp.write_stream` span per data-plane transfer, each parented on the engine's node span through
//! the W3C `traceparent` the host sends -- so a connector's work shows up inside the run's own trace.
//! Ordinary [`tracing`] spans and events a connector opens inside those handlers are exported too, so
//! instrumenting a connector needs nothing from this crate beyond `tracing` itself. For metrics,
//! [`meter`] returns the meter to record instruments on.
//!
//! One constraint: exporting spans requires installing a global `tracing` subscriber, so a binary that
//! installs its own before calling [`serve_sink`] keeps it and NO spans are exported (the handshake
//! reports that on stderr); meters are unaffected and still work. Never put a configuration value in
//! a span name, a span field, or a metric label -- what is emitted is what the operator sees.

pub(crate) mod pb {
    #![allow(
        clippy::doc_markdown,
        clippy::large_enum_variant,
        clippy::enum_variant_names
    )]
    tonic::include_proto!("pz.connector.v1");
}

mod config;
mod data_plane;
mod error;
mod server;
mod telemetry;
mod ticket;

pub use config::Config;
pub use error::PzError;
pub use server::{
    serve_sink, ConnectorDecl, NativeCopy, OutputSpec, ServeExit, Sink, SinkConnector,
    WriteAttempt, WriteResult, WriteSession,
};
pub use telemetry::meter;
