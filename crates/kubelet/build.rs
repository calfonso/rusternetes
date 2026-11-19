fn main() {
    // CSI spec v1.13.0 (github.com/container-storage-interface/spec, Apache-2.0,
    // see proto/csi/LICENSE) and the kubelet plugin registration API v1
    // (k8s.io/kubelet pkg/apis/pluginregistration/v1, Apache-2.0).
    tonic_build::configure()
        .compile_protos(
            &["proto/csi/csi.proto", "proto/pluginregistration/api.proto"],
            &["proto/csi", "proto/pluginregistration"],
        )
        .expect("Failed to compile CSI protos");
}
