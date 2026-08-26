//! SDK for writing PipelineZ (pz) out-of-process connectors (PCP) in Rust: [`serve_sink`] parses
//! `--pz-socket`, serves the `PzConnector` gRPC control plane on that Unix socket (mode 0600) and the
//! raw Arrow IPC data plane on `<socket>.data`, and dispatches every RPC to a [`SinkConnector`]/[`Sink`]/
//! [`WriteSession`] the connector author implements. Source support is deferred (additive) -- the wire
//! protocol already covers it, only this crate's trait surface does not yet.

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
mod ticket;

pub use config::Config;
pub use error::PzError;
pub use server::{
    serve_sink, ConnectorDecl, NativeCopy, OutputSpec, ServeExit, Sink, SinkConnector,
    WriteAttempt, WriteResult, WriteSession,
};
