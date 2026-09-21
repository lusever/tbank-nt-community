use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Build scripts run before Cargo starts compiling dependents; setting PROTOC here keeps
    // this crate independent of a system protobuf installation.
    unsafe {
        env::set_var("PROTOC", protoc);
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let revision = git_revision(&manifest_dir);
    println!("cargo:rustc-env=TBANK_ADAPTER_GIT_REVISION={revision}");
    let proto_root = manifest_dir.join("proto");
    let contracts_dir = proto_root.join("tinkoff/public/invest/api/contract/v1");

    let protos = [
        "common.proto",
        "instruments.proto",
        "marketdata.proto",
        "operations.proto",
        "orders.proto",
        "sandbox.proto",
        "signals.proto",
        "stoporders.proto",
        "users.proto",
    ]
    .into_iter()
    .map(|name| contracts_dir.join(name))
    .chain(std::iter::once(
        proto_root.join("google/api/field_behavior.proto"),
    ))
    .collect::<Vec<_>>();

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("proto/contracts.lock").display()
    );

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .server_mod_attribute(".", "#[cfg(test)]")
        .compile_well_known_types(false)
        .extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp")
        .compile_protos(&protos, &[contracts_dir, proto_root])?;

    Ok(())
}

fn git_revision(manifest_dir: &Path) -> String {
    if let Some(git_dir) = git_output(manifest_dir, &["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
    }

    if let Some(common_dir) = git_output(manifest_dir, &["rev-parse", "--git-common-dir"])
        .map(|path| resolve_path(manifest_dir, &path))
    {
        println!(
            "cargo:rerun-if-changed={}",
            common_dir.join("packed-refs").display()
        );

        if let Some(reference) = git_output(manifest_dir, &["symbolic-ref", "-q", "HEAD"]) {
            println!(
                "cargo:rerun-if-changed={}",
                common_dir.join(reference).display()
            );
        }
    }

    git_output(manifest_dir, &["rev-parse", "HEAD"])
        .filter(|revision| {
            !revision.is_empty() && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        .unwrap_or_else(|| {
            println!(
                "cargo:warning=adapter Git revision unavailable; using 'unknown' as provenance"
            );
            "unknown".to_string()
        })
}

fn git_output(directory: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(directory)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn resolve_path(directory: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        directory.join(path)
    }
}
