//! The SDK's real contract: not anything provable from Rust-side unit tests alone, but whether the
//! host's own black-box PCP conformance verb (`pz connector test`) accepts what `serve_sink` produces
//! on the wire. Builds the `memory_sink` example and runs every sink-side vector against it via
//! `dotnet run --project src/Pz.Cli -- connector test <entrypoint> --config <probe.yml>`.
//!
//! SKIPs cleanly (prints a message, does not fail the test) when either toolchain this needs is
//! missing -- a `dotnet` that can run the SDK the repo's `global.json` requires (the first `dotnet` on
//! PATH may be a runtime-only or older install, so every `dotnet` on PATH, `$DOTNET_ROOT` and
//! `~/.dotnet` are tried; under `CI` a missing SDK fails instead, so CI is never weakened into a skip)
//! or a repo root this test can actually locate `src/Pz.Cli` under (a `cargo package`/vendored build of this crate, with no host repo alongside it, has no
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

    let Some(dotnet) = find_dotnet(&repo_root) else {
        let message =
            "no dotnet that can run the .NET SDK global.json requires was found (tried every \
                       `dotnet` on PATH, $DOTNET_ROOT and ~/.dotnet)";
        assert!(std::env::var_os("CI").is_none(), "{message}");
        eprintln!("SKIP: {message}");
        return;
    };

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
        .tempdir_in(std::env::temp_dir())
        .expect("failed to create a scratch dir for the conformance probe config");
    let config_path = work_dir.path().join("conformance.yml");
    std::fs::write(
        &config_path,
        "connection: {}\nwrite:\n  output: conformance_probe\n  mode: replace\n  schema_policy: match\n",
    )
    .expect("failed to write the conformance probe config");

    let output = Command::new(&dotnet)
        .env("DOTNET_ROOT", dotnet.parent().unwrap_or(Path::new(".")))
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
    // A whole-number option crosses the wire as an f64; this crate has to hand it to the connector
    // as an integer, and the host asks this crate -- not the example sink -- whether it did.
    assert!(
        stdout
            .lines()
            .any(|line| line.starts_with("PASS numeric-option-fidelity")),
        "expected the numeric-option-fidelity vector to pass; got:\n{stdout}"
    );
}

/// The first `dotnet` that resolves the SDK `global.json` asks for: `dotnet --version`, run from the repo root,
/// fails for a runtime-only install or one without a matching SDK.
fn find_dotnet(repo_root: &Path) -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(root) = std::env::var_os("DOTNET_ROOT") {
        candidates.push(PathBuf::from(root).join(dotnet_binary()));
    }
    if let Some(path) = std::env::var_os("PATH") {
        candidates.extend(std::env::split_paths(&path).map(|dir| dir.join(dotnet_binary())));
    }
    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        candidates.push(PathBuf::from(home).join(".dotnet").join(dotnet_binary()));
    }
    candidates
        .into_iter()
        .find(|candidate| runs_repo_sdk(candidate, repo_root))
}

fn runs_repo_sdk(dotnet: &Path, repo_root: &Path) -> bool {
    dotnet.is_file()
        && Command::new(dotnet)
            .arg("--version")
            .current_dir(repo_root)
            .env("DOTNET_ROOT", dotnet.parent().unwrap_or(Path::new(".")))
            .output()
            .is_ok_and(|output| output.status.success())
}

fn dotnet_binary() -> String {
    format!("dotnet{}", std::env::consts::EXE_SUFFIX)
}

#[test]
fn a_dotnet_that_cannot_run_the_repo_sdk_is_not_chosen() {
    let Some(repo_root) = find_repo_root() else {
        return;
    };
    assert!(!runs_repo_sdk(Path::new("/nonexistent/dotnet"), &repo_root));
    // A program that runs but is not a dotnet with the SDK (it exits non-zero on `--version`).
    if Path::new("/bin/false").is_file() {
        assert!(!runs_repo_sdk(Path::new("/bin/false"), &repo_root));
    }
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
    // `cargo build --example` above always places it at <target-dir>/debug/examples/memory_sink(.exe); the
    // manifest dir's own `../target` is right whether this crate is built standalone or as a workspace
    // member (a workspace shares one target dir at the workspace root, one level up from here).
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let binary = format!("memory_sink{}", std::env::consts::EXE_SUFFIX);
    let workspace_target = manifest_dir.join("../target/debug/examples").join(&binary);
    if workspace_target.is_file() {
        return workspace_target;
    }
    manifest_dir.join("target/debug/examples").join(binary)
}
