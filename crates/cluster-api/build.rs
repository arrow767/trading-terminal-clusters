fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Vendored protoc removes the dev-machine prerequisite — anyone with
    // a working Cargo can build this crate without installing protoc.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    println!("cargo:rerun-if-changed=proto/cluster.proto");
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/cluster.proto"], &["proto"])?;
    Ok(())
}
