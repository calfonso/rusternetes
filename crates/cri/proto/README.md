# Vendored CRI proto

`runtime/v1/api.proto` is the Kubernetes Container Runtime Interface v1 protocol,
vendored from the kubernetes **v1.35** source tree:

    staging/src/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto

It is generated into a tonic client by `../build.rs` at build time. The proto is
self-contained — proto3, no `import`s, no gogo annotations — so vanilla
`tonic-build` compiles it.

## Local modification

Four `[debug_redact = true]` field options were stripped from `message
AuthConfig` (fields `password`, `auth`, `identity_token`, `registry_token`).

`debug_redact` is a protobuf-22+ field option that only marks a field for
redaction in protobuf's own debug/log output. It has **no effect on the
generated Rust types or the wire format**. The system `protoc` (3.21, protobuf
21) predates the option and rejects it, so it is removed here rather than pulling
in a vendored modern `protoc` binary blob. Re-apply this strip when re-vendoring
from a newer kubernetes tag:

    sed -i 's/ \[debug_redact = true\]//g' runtime/v1/api.proto
