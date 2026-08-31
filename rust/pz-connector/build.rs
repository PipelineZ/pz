/// The normative proto lives at `src/Pz.Connectors.Protocol/pz_connector.proto`; `proto/pz_connector.proto`
/// is a symlink to it so there is exactly one copy of the wire contract. A checkout that cannot carry
/// symlinks (some Windows configurations, some archive extractions) leaves a broken link here rather than
/// silently building against a stale copy, so that case fails the build with a clear message instead of
/// compiling an empty/garbage proto.
fn main() {
    let proto = "proto/pz_connector.proto";
    let metadata = std::fs::symlink_metadata(proto).unwrap_or_else(|e| {
        panic!("{proto} is missing ({e}); expected a symlink to ../../../src/Pz.Connectors.Protocol/pz_connector.proto")
    });
    if metadata.file_type().is_symlink() && std::fs::read(proto).is_err() {
        panic!(
            "{proto} is a symlink that does not resolve -- this checkout cannot follow it to \
             ../../../src/Pz.Connectors.Protocol/pz_connector.proto (the one normative copy of the wire \
             contract). Re-clone with symlink support rather than copying the file, which would drift \
             silently."
        );
    }

    println!("cargo:rerun-if-changed={proto}");

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&[proto], &["proto"])
        .expect("failed to compile pz_connector.proto");
}
