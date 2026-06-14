fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let wkt_includes = protoc_bin_vendored::include_path()?;

    // Point prost-build at the bundled protoc so no system install is needed.
    // SAFETY: build scripts run single-threaded; no other thread reads env vars here.
    unsafe { std::env::set_var("PROTOC", &protoc) };

    tonic_prost_build::configure().compile_protos(
            &["proto/interstellar.proto"],
            // Include both our own proto dir and protoc's well-known-type headers.
            &["proto", wkt_includes.to_str().unwrap()],
        )?;

    Ok(())
}
