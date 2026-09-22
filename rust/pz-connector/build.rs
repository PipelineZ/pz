use std::path::{Path, PathBuf};

/// The normative proto lives at `src/Pz.Connectors.Protocol/pz_connector.proto`; `proto/pz_connector.proto`
/// is a symlink to it so there is exactly one copy of the wire contract. Two checkouts cannot carry the
/// link as a link:
///
/// - Git with `core.symlinks=false` (the Windows default) writes a plain file whose whole content is the
///   link target. That target is followed here, so the build still reads the one normative copy.
/// - Some archive extractions leave a link that does not resolve. That fails the build with a clear
///   message rather than compiling an empty/garbage proto or a stale copy.
fn main() {
    let proto = resolve_proto(Path::new("proto/pz_connector.proto"));
    let include = proto.parent().expect("a proto file has a parent directory");

    println!("cargo:rerun-if-changed=proto/pz_connector.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&[proto.as_path()], &[include])
        .expect("failed to compile pz_connector.proto");
}

const TARGET: &str = "../../../src/Pz.Connectors.Protocol/pz_connector.proto";

fn resolve_proto(proto: &Path) -> PathBuf {
    let metadata = std::fs::symlink_metadata(proto).unwrap_or_else(|e| {
        panic!(
            "{} is missing ({e}); expected a symlink to {TARGET}",
            proto.display()
        )
    });

    if metadata.file_type().is_symlink() {
        if std::fs::read(proto).is_err() {
            panic!(
                "{} is a symlink that does not resolve -- this checkout cannot follow it to {TARGET} \
                 (the one normative copy of the wire contract). Re-clone with symlink support rather \
                 than copying the file, which would drift silently.",
                proto.display()
            );
        }
        return proto.to_path_buf();
    }

    // A real proto is kilobytes of text and never a bare relative path; git's placeholder for a link
    // is exactly the target, with no trailing newline.
    let content = std::fs::read_to_string(proto).unwrap_or_default();
    if content == TARGET {
        let followed = proto.parent().unwrap().join(TARGET);
        if !followed.is_file() {
            panic!(
                "{} is git's placeholder for a symlink to {TARGET}, which does not exist from here",
                proto.display()
            );
        }
        return followed;
    }

    proto.to_path_buf()
}
