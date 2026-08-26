//! `pz-deltalake`: an out-of-process Delta Lake sink for PipelineZ, served over the `pz-connector`
//! Rust SDK's PCP protocol. See `sink.rs` for the connector itself.

mod redact;
mod schemas;
mod sink;

#[tokio::main]
async fn main() {
    let err = pz_connector::serve_sink(sink::decl(), sink::DeltaSinkConnector).await;
    match err.downcast_ref::<pz_connector::ServeExit>() {
        Some(pz_connector::ServeExit::UsageError(msg)) => {
            eprintln!("pz-deltalake: {msg}");
            std::process::exit(2);
        }
        Some(pz_connector::ServeExit::Stopped(reason)) => {
            eprintln!("pz-deltalake: stopped ({reason})");
        }
        None => {
            eprintln!("pz-deltalake: {err:#}");
            std::process::exit(1);
        }
    }
}
