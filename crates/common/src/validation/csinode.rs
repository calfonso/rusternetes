//! CSINode validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateCSINode` (release-1.35).
//!
//! Covers each `spec.drivers[]`: CSI driver name format, required nodeID
//! (length-bounded), non-negative `allocatable.count`, topologyKeys
//! (non-empty + unique + qualified names), and duplicate driver names across
//! the list. ObjectMeta is validated separately. CSI is a non-negotiable
//! project contract.

use crate::resources::csi::{CSINode, CSINodeDriver};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_subdomain, is_qualified_name};
use crate::validation::objectmeta::validate_nonnegative_field;
use std::collections::HashSet;

const CSI_DRIVER_NAME_MAX_LENGTH: usize = 63;
const CSI_NODE_ID_MAX_LENGTH: usize = 192;

/// Port of upstream `ValidateCSIDriverName`: required, ≤63 chars, and a
/// DNS-1123 subdomain when lowercased (caseless).
fn validate_csi_driver_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if name.is_empty() {
        errs.push(Error::required(fld_path, ""));
        return errs;
    }
    if name.len() > CSI_DRIVER_NAME_MAX_LENGTH {
        errs.push(Error::too_long(fld_path, CSI_DRIVER_NAME_MAX_LENGTH));
    }
    for msg in is_dns1123_subdomain(&name.to_lowercase()) {
        errs.push(Error::invalid(fld_path, name.to_string(), msg));
    }
    errs
}

/// Port of upstream `validateCSINodeDriver`.
fn validate_csi_node_driver(
    driver: &CSINodeDriver,
    seen_names: &mut HashSet<String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = validate_csi_driver_name(&driver.name, &fld_path.child("name"));

    // nodeID — always required, length-bounded.
    let node_id_path = fld_path.child("nodeID");
    if driver.node_id.is_empty() {
        errs.push(Error::required(&node_id_path, ""));
    }
    if driver.node_id.len() > CSI_NODE_ID_MAX_LENGTH {
        errs.push(Error::invalid(
            &node_id_path,
            driver.node_id.clone(),
            format!("must be {} characters or less", CSI_NODE_ID_MAX_LENGTH),
        ));
    }

    // allocatable.count — non-negative when present.
    if let Some(alloc) = &driver.allocatable {
        if let Some(count) = alloc.count {
            errs.extend(validate_nonnegative_field(
                count as i64,
                &fld_path.child("allocatable").child("count"),
            ));
        }
    }

    // topologyKeys — non-empty, unique, qualified names. Upstream attaches these
    // to the driver path (not a topologyKeys child).
    if let Some(keys) = &driver.topology_keys {
        let mut topo_keys: HashSet<&str> = HashSet::new();
        for key in keys {
            if key.is_empty() {
                errs.push(Error::required(fld_path, ""));
            }
            if !topo_keys.insert(key.as_str()) {
                errs.push(Error::duplicate(fld_path, key.clone()));
            }
            for msg in is_qualified_name(key) {
                errs.push(Error::invalid(fld_path, key.clone(), msg));
            }
        }
    }

    // duplicate driver name across the spec.
    if !seen_names.insert(driver.name.clone()) {
        errs.push(Error::duplicate(
            &fld_path.child("name"),
            driver.name.clone(),
        ));
    }

    errs
}

/// Validate a `CSINode` on create. Mirrors upstream `ValidateCSINode` minus
/// ObjectMeta.
pub fn validate_csi_node(node: &CSINode) -> ErrorList {
    let drivers_path = Path::new("spec").child("drivers");
    let mut errs: ErrorList = Vec::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    for (i, driver) in node.spec.drivers.iter().enumerate() {
        errs.extend(validate_csi_node_driver(
            driver,
            &mut seen_names,
            &drivers_path.index(i),
        ));
    }
    errs
}
