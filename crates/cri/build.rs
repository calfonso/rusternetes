//! Generate the CRI v1 gRPC client from the vendored kubernetes proto.
//!
//! The proto (`proto/runtime/v1/api.proto`) is a verbatim copy of
//! `staging/src/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto` from the
//! kubernetes v1.35 release. It is self-contained — proto3, no imports, no gogo
//! annotations — so vanilla tonic-build compiles it without extra include paths.
//!
//! We build only the client side: the kubelet is a CRI client, never a server.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/runtime/v1/api.proto";

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&[proto], &["proto"])?;

    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
