use rusternetes_common::resources::endpointslice::EndpointSlice;
use rusternetes_common::validation::endpointslice::validate_endpoint_slice_update;

fn slice(address_type: &str, addr: &str) -> EndpointSlice {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {"name": "web-abc", "namespace": "default"},
        "addressType": address_type,
        "endpoints": [{"addresses": [addr]}],
        "ports": []
    }))
    .unwrap()
}

#[test]
fn address_type_immutable() {
    let old = slice("IPv4", "10.0.0.1");
    // New slice is internally valid (IPv6 type + IPv6 address) so the only
    // error must be the addressType immutability violation.
    let new = slice("IPv6", "2001:db8::1");
    let errs = validate_endpoint_slice_update(&new, &old);
    assert_eq!(
        errs.len(),
        1,
        "expected exactly the immutability error: {errs:?}"
    );
    assert!(errs[0].to_string().contains("addressType"));
    assert!(errs[0].to_string().contains("immutable"));
}

#[test]
fn same_address_type_ok() {
    let old = slice("IPv4", "10.0.0.1");
    let new = slice("IPv4", "10.0.0.2");
    assert!(validate_endpoint_slice_update(&new, &old).is_empty());
}

#[test]
fn field_errors_still_surface_on_update() {
    let old = slice("IPv4", "10.0.0.1");
    // Invalid address for the (unchanged) IPv4 type still reported on update.
    let new = slice("IPv4", "not-an-ip");
    let errs = validate_endpoint_slice_update(&new, &old);
    assert!(!errs.is_empty(), "field validation must run on update too");
}
