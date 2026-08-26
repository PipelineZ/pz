//! The SDK's real contract: not anything provable from Rust-side unit tests alone, but whether the
//! host's own black-box PCP conformance verb (`pz connector test`) accepts what `serve_sink` produces
//! on the wire. Builds the `memory_sink` example and runs every sink-side vector against it via
//! `dotnet run --project src/Pz.Cli -- connector test <entrypoint> --config <probe.yml>`.
//!
//! SKIPs cleanly (prints a message, does not fail the test) when either toolchain this needs is
//! missing -- `dotnet` (the host's own build) or a repo root this test can actually locate `src/Pz.Cli`
//! under (a `cargo package`/vendored build of this crate, with no host repo alongside it, has no
//! conformance verb to run against at all). Matches every other docker/toolchain-gated suite in this
//! repository (`Xunit.SkippableFact`, the various `scripts/macro-bench-*.sh` scripts): absence of an
//! optional dependency is a skip, never a failure.

use std::path::{Path, PathBuf};
use std::process::Command;

#[test]
fn memory_sink_passes_every_sink_conformance_vector() {
    let Some(repo_root) = find_repo_root() else {
        eprintln!("SKIP: could not locate the pz repo root (src/Pz.Cli) above this crate");
        return;
    };

    if Command::new("dotnet").arg("--version").output().is_err() {
        eprintln!("SKIP: dotnet not available");
        return;
    }

    let build = Command::new(env!("CARGO"))
        .args(["build", "--example", "memory_sink"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("failed to invoke cargo to build the memory_sink example");
    assert!(build.success(), "cargo build --example memory_sink failed");

    let entrypoint = locate_example_binary();
    assert!(
        entrypoint.is_file(),
        "expected the built example at '{}'",
        entrypoint.display()
    );

    let work_dir = tempfile::Builder::new()
        .prefix("pzrs.")
        .tempdir_in(std::env::var_os("TMPDIR").unwrap_or_else(|| "/tmp".into()))
        .expect("failed to create a short-path scratch dir for the unix sockets this test opens");
    let config_path = work_dir.path().join("conformance.yml");
    std::fs::write(
        &config_path,
        "connection: {}\nwrite:\n  output: conformance_probe\n  mode: replace\n  schema_policy: match\n",
    )
    .expect("failed to write the conformance probe config");

    let output = Command::new("dotnet")
        .args(["run", "--project"])
        .arg(repo_root.join("src/Pz.Cli"))
        .args(["-c", "Release", "--", "connector", "test"])
        .arg(&entrypoint)
        .args(["--config"])
        .arg(&config_path)
        .output()
        .expect("failed to invoke dotnet run -- connector test");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "pz connector test reported a failure (exit {:?}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    assert!(
        stdout
            .lines()
            .any(|line| line.starts_with("PASS handshake")),
        "expected at least the handshake vector to pass; got:\n{stdout}"
    );
}

fn find_repo_root() -> Option<PathBuf> {
    let mut dir = Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
    loop {
        if dir.join("src/Pz.Cli/Pz.Cli.csproj").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

fn locate_example_binary() -> PathBuf {
    // `cargo build --example` above always places it at <target-dir>/debug/examples/memory_sink; the
    // manifest dir's own `../target` is right whether this crate is built standalone or as a workspace
    // member (a workspace shares one target dir at the workspace root, one level up from here).
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_target = manifest_dir.join("../target/debug/examples/memory_sink");
    if workspace_target.is_file() {
        return workspace_target;
    }
    manifest_dir.join("target/debug/examples/memory_sink")
}
