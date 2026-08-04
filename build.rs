use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Build scripts run before Cargo starts compiling dependents; setting PROTOC here keeps
    // this crate independent of a system protobuf installation.
    unsafe {
        env::set_var("PROTOC", protoc);
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
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
