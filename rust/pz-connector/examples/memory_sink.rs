//! A tiny in-memory `SinkConnector`, used both to exercise the SDK during development and as the
//! target `scripts/rust-conformance.sh` runs the host's `pz connector test` black-box protocol checks
//! against. Every write's batches accumulate under its output name in memory (nothing is ever actually
//! persisted -- there is no destination); `commit` reports the row/batch counts the conformance suite
//! checks, and `abort` simply drops whatever was buffered.
//!
//! `--own-subscriber` makes it install its own `tracing` subscriber (a `fmt` layer to stderr) with
//! `pz_connector::layer()` composed in before serving, the shape a connector with its own logging
//! takes; the host-side telemetry facts run it both ways.

use std::sync::LazyLock;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use opentelemetry::metrics::Counter;
use pz_connector::{
    Config, ConnectorDecl, NativeCopy, OutputSpec, PzError, Sink, SinkConnector, WriteResult,
    WriteSession,
};
use tracing_subscriber::layer::{Layer, SubscriberExt};

/// The one instrument this example records on, so the host-side telemetry test can prove a connector's
/// own metrics reach the collector the host named. Built lazily rather than at startup: `meter()`
/// resolves against the global provider, which `serve_sink` installs only once the host's handshake has
/// named an endpoint. A static, connector-authored name -- nothing from the output spec ever becomes an
/// instrument name or a label.
static BEGIN_WRITE_CALLS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    pz_connector::meter()
        .u64_counter("pz.memory_sink.begin_write_calls")
        .build()
});

struct MemorySinkConnector;

#[async_trait]
impl SinkConnector for MemorySinkConnector {
    async fn validate(&self, config: &Config) -> Vec<String> {
        // Nothing this in-memory sink needs from a connection config is actually required to run --
        // an unrecognized key is not an error either, since there is no schema to enforce here.
        let _ = config;
        Vec::new()
    }

    async fn check(&self, _config: &Config) -> Result<(), PzError> {
        // Always reachable: there is no real destination to fail to reach.
        Ok(())
    }

    async fn open(&self, _config: Config) -> Result<Box<dyn Sink>, PzError> {
        Ok(Box::new(MemorySink))
    }

    fn try_native_copy(&self, _spec: &OutputSpec) -> Option<NativeCopy> {
        // An in-memory destination has no SQL engine on the other end for DuckDB to hand a COPY to.
        None
    }
}

/// The exact output name `pz connector test`'s `transient-error-shape` vector probes with, mirrored
/// from `Pz.PackageManagement.ProcessHosting.Conformance.ConformanceSuite`'s own `missingName` constant.
/// Rejecting it here (rather than accepting any output name unconditionally) is what lets that vector
/// actually observe a decoded `pz-error-bin` trailer end-to-end instead of reporting a vacuous pass.
const CONFORMANCE_PROBE_MISSING_OUTPUT: &str = "__pz_conformance_probe_missing__";

struct MemorySink;

#[async_trait]
impl Sink for MemorySink {
    async fn begin_write(
        &self,
        spec: OutputSpec,
        schema: SchemaRef,
    ) -> Result<Box<dyn WriteSession>, PzError> {
        BEGIN_WRITE_CALLS.add(1, &[]);

        if spec.output == CONFORMANCE_PROBE_MISSING_OUTPUT {
            return Err(PzError::transient(
                format!(
                    "no output named '{}' -- the memory sink only accepts the output its probe config names",
                    spec.output
                ),
                250,
            ));
        }

        Ok(Box::new(MemoryWriteSession {
            output: spec.output,
            schema,
            batches: Vec::new(),
        }))
    }
}

struct MemoryWriteSession {
    output: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl WriteSession for MemoryWriteSession {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), PzError> {
        if batch.schema() != self.schema {
            return Err(PzError::new(format!(
                "batch schema does not match the schema declared at BeginWrite for output '{}'",
                self.output
            )));
        }

        self.batches.push(batch);
        Ok(())
    }

    async fn commit(&mut self) -> Result<WriteResult, PzError> {
        let rows_written: i64 = self.batches.iter().map(|b| b.num_rows() as i64).sum();
        let batches_written = self.batches.len() as i64;
        Ok(WriteResult {
            rows_written,
            batches_written,
        })
    }

    async fn abort(&mut self) -> Result<(), PzError> {
        self.batches.clear();
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|a| a == "--own-subscriber") {
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(std::io::stderr)
                        .with_filter(tracing_subscriber::filter::LevelFilter::WARN),
                )
                .with(pz_connector::layer()),
        )
        .expect("memory_sink: the first subscriber in the process");
    }

    let decl = ConnectorDecl {
        name: "memory-sink",
        version: env!("CARGO_PKG_VERSION"),
        capabilities: 0,
        connection_config_schema: "",
        dataset_config_schema: "",
    };

    let err = pz_connector::serve_sink(decl, MemorySinkConnector).await;
    match err.downcast_ref::<pz_connector::ServeExit>() {
        Some(pz_connector::ServeExit::UsageError(msg)) => {
            eprintln!("memory_sink: {msg}");
            std::process::exit(2);
        }
        Some(pz_connector::ServeExit::Stopped(reason)) => {
            eprintln!("memory_sink: stopped ({reason})");
        }
        None => {
            eprintln!("memory_sink: {err:#}");
            std::process::exit(1);
        }
    }
}
