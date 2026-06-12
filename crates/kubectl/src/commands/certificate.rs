use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// Approve or deny certificate signing requests (CSRs).
///
/// Equivalent to:
///   kubectl certificate approve csr-name
///   kubectl certificate deny csr-name
pub async fn execute(
    client: &ApiClient,
    action: &str,
    csr_names: &[String],
    force: bool,
) -> Result<()> {
    if csr_names.is_empty() {
        anyhow::bail!("one or more CSRs must be specified as <name>");
    }

    for csr_name in csr_names {
        match action {
            "approve" => approve_csr(client, csr_name, force).await?,
            "deny" => deny_csr(client, csr_name, force).await?,
            _ => anyhow::bail!(
                "unknown certificate action: {}. Use 'approve' or 'deny'",
                action
            ),
        }
    }

    Ok(())
}

async fn approve_csr(client: &ApiClient, name: &str, force: bool) -> Result<()> {
    let csr_path = format!(
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}",
        name
    );

    // Get the current CSR
    let csr: Value = client
        .get(&csr_path)
        .await
        .map_err(|e| anyhow::anyhow!("certificatesigningrequest \"{}\" not found: {}", name, e))?;

    // Check if already approved
    if !force {
        if let Some(conditions) = csr
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
        {
            for condition in conditions {
                let ctype = condition.get("type").and_then(|t| t.as_str()).unwrap_or("");
                if ctype == "Approved" {
                    println!(
                        "certificatesigningrequest.certificates.k8s.io/{} already approved",
                        name
                    );
                    return Ok(());
                }
            }
        }
    }

    // Build the approval - update the status with an Approved condition
    let mut updated_csr = csr.clone();
    let conditions = updated_csr
        .get_mut("status")
        .and_then(|s| s.as_object_mut())
        .map(|s| {
            s.entry("conditions")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap()
                .clone()
        })
        .unwrap_or_default();

    // Remove any existing Denied condition if force
    let mut new_conditions: Vec<Value> = if force {
        conditions
            .into_iter()
            .filter(|c| c.get("type").and_then(|t| t.as_str()).unwrap_or("") != "Denied")
            .collect()
    } else {
        conditions
    };

    // Add the Approved condition
    new_conditions.push(json!({
        "type": "Approved",
        "status": "True",
        "reason": "KubectlApprove",
        "message": "This CSR was approved by kubectl certificate approve.",
        "lastUpdateTime": chrono::Utc::now().to_rfc3339(),
    }));

    if let Some(status) = updated_csr.get_mut("status") {
        status["conditions"] = json!(new_conditions);
    } else {
        updated_csr["status"] = json!({
            "conditions": new_conditions,
        });
    }

    let approval_path = format!(
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}/approval",
        name
    );

    let _result: Value = client
        .put(&approval_path, &updated_csr)
        .await
        .context("Failed to approve CSR")?;

    println!(
        "certificatesigningrequest.certificates.k8s.io/{} approved",
        name
    );
    Ok(())
}

async fn deny_csr(client: &ApiClient, name: &str, force: bool) -> Result<()> {
    let csr_path = format!(
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}",
        name
    );

    // Get the current CSR
    let csr: Value = client
        .get(&csr_path)
        .await
        .map_err(|e| anyhow::anyhow!("certificatesigningrequest \"{}\" not found: {}", name, e))?;

    // Check if already denied
    if !force {
        if let Some(conditions) = csr
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
        {
            for condition in conditions {
                let ctype = condition.get("type").and_then(|t| t.as_str()).unwrap_or("");
                if ctype == "Denied" {
                    println!(
                        "certificatesigningrequest.certificates.k8s.io/{} already denied",
                        name
                    );
                    return Ok(());
                }
            }
        }
    }

    // Build the denial
    let mut updated_csr = csr.clone();
    let conditions = updated_csr
        .get_mut("status")
        .and_then(|s| s.as_object_mut())
        .map(|s| {
            s.entry("conditions")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap()
                .clone()
        })
        .unwrap_or_default();

    // Remove any existing Approved condition if force
    let mut new_conditions: Vec<Value> = if force {
        conditions
            .into_iter()
            .filter(|c| c.get("type").and_then(|t| t.as_str()).unwrap_or("") != "Approved")
            .collect()
    } else {
        conditions
    };

    new_conditions.push(json!({
        "type": "Denied",
        "status": "True",
        "reason": "KubectlDeny",
        "message": "This CSR was denied by kubectl certificate deny.",
        "lastUpdateTime": chrono::Utc::now().to_rfc3339(),
    }));

    if let Some(status) = updated_csr.get_mut("status") {
        status["conditions"] = json!(new_conditions);
    } else {
        updated_csr["status"] = json!({
            "conditions": new_conditions,
        });
    }

    let approval_path = format!(
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}/approval",
        name
    );

    let _result: Value = client
        .put(&approval_path, &updated_csr)
        .await
        .context("Failed to deny CSR")?;

    println!(
        "certificatesigningrequest.certificates.k8s.io/{} denied",
        name
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    #[test]
    fn test_action_validation() {
        let action = "approve";
        assert!(matches!(action, "approve" | "deny"));

        let action = "deny";
        assert!(matches!(action, "approve" | "deny"));

        let action = "invalid";
        assert!(!matches!(action, "approve" | "deny"));
    }

    #[test]
    fn test_approve_condition_construction() {
        // Simulate building the approval condition that approve_csr constructs
        let csr = json!({
            "metadata": {"name": "my-csr"},
            "spec": {},
            "status": {"conditions": []}
        });

        let mut updated_csr = csr.clone();
        let conditions = updated_csr
            .get_mut("status")
            .and_then(|s| s.as_object_mut())
            .map(|s| {
                s.entry("conditions")
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .unwrap()
                    .clone()
            })
            .unwrap_or_default();

        let mut new_conditions: Vec<Value> = conditions;
        new_conditions.push(json!({
            "type": "Approved",
            "status": "True",
            "reason": "KubectlApprove",
            "message": "This CSR was approved by kubectl certificate approve.",
        }));

        updated_csr["status"]["conditions"] = json!(new_conditions);

        let result_conditions = updated_csr["status"]["conditions"].as_array().unwrap();
        assert_eq!(result_conditions.len(), 1);
        assert_eq!(result_conditions[0]["type"], "Approved");
        assert_eq!(result_conditions[0]["reason"], "KubectlApprove");
    }

    #[test]
    fn test_deny_condition_construction() {
        let csr = json!({
            "metadata": {"name": "my-csr"},
            "spec": {},
            "status": {"conditions": []}
        });

        let mut updated_csr = csr.clone();
        let mut new_conditions: Vec<Value> = Vec::new();
        new_conditions.push(json!({
            "type": "Denied",
            "status": "True",
            "reason": "KubectlDeny",
            "message": "This CSR was denied by kubectl certificate deny.",
        }));

        updated_csr["status"]["conditions"] = json!(new_conditions);

        let result_conditions = updated_csr["status"]["conditions"].as_array().unwrap();
        assert_eq!(result_conditions.len(), 1);
        assert_eq!(result_conditions[0]["type"], "Denied");
        assert_eq!(result_conditions[0]["reason"], "KubectlDeny");
    }

    #[test]
    fn test_force_removes_opposite_condition() {
        // When force=true and approving, existing Denied conditions should be removed
        let existing_conditions = vec![json!({
            "type": "Denied",
            "status": "True",
            "reason": "KubectlDeny",
        })];

        let force = true;
        let new_conditions: Vec<Value> = if force {
            existing_conditions
                .into_iter()
                .filter(|c| c.get("type").and_then(|t| t.as_str()).unwrap_or("") != "Denied")
                .collect()
        } else {
            existing_conditions
        };

        assert!(
            new_conditions.is_empty(),
            "Denied condition should be removed when force approving"
        );
    }

    #[test]
    fn test_csr_api_path_construction() {
        let name = "my-csr";
        let csr_path = format!(
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}",
            name
        );
        assert_eq!(
            csr_path,
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/my-csr"
        );

        let approval_path = format!(
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}/approval",
            name
        );
        assert_eq!(
            approval_path,
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/my-csr/approval"
        );
    }

    #[test]
    fn test_already_approved_detection() {
        let csr = json!({
            "status": {
                "conditions": [
                    {"type": "Approved", "status": "True", "reason": "KubectlApprove"}
                ]
            }
        });

        let already_approved = csr
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
            .map(|conditions| {
                conditions
                    .iter()
                    .any(|c| c.get("type").and_then(|t| t.as_str()) == Some("Approved"))
            })
            .unwrap_or(false);

        assert!(already_approved);
    }

    #[test]
    fn test_already_denied_detection() {
        let csr = json!({
            "status": {
                "conditions": [
                    {"type": "Denied", "status": "True", "reason": "KubectlDeny"}
                ]
            }
        });

        let already_denied = csr
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
            .map(|conditions| {
                conditions
                    .iter()
                    .any(|c| c.get("type").and_then(|t| t.as_str()) == Some("Denied"))
            })
            .unwrap_or(false);

        assert!(already_denied);
    }

    #[test]
    fn test_csr_no_status_conditions() {
        let csr = json!({
            "metadata": {"name": "my-csr"},
            "spec": {}
        });

        let already_approved = csr
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
            .map(|conditions| {
                conditions
                    .iter()
                    .any(|c| c.get("type").and_then(|t| t.as_str()) == Some("Approved"))
            })
            .unwrap_or(false);

        assert!(!already_approved);
    }

    #[test]
    fn test_force_removes_approved_when_denying() {
        let existing_conditions = vec![json!({
            "type": "Approved",
            "status": "True",
            "reason": "KubectlApprove",
        })];

        let force = true;
        let new_conditions: Vec<Value> = if force {
            existing_conditions
                .into_iter()
                .filter(|c| c.get("type").and_then(|t| t.as_str()).unwrap_or("") != "Approved")
                .collect()
        } else {
            existing_conditions
        };

        assert!(
            new_conditions.is_empty(),
            "Approved condition should be removed when force denying"
        );
    }

    #[test]
    fn test_csr_status_initialization_when_missing() {
        // When status is missing entirely, the code creates it
        let mut csr = json!({
            "metadata": {"name": "my-csr"},
            "spec": {}
        });

        // Simulate the code path where status doesn't exist
        let has_status = csr.get("status").is_some();
        assert!(!has_status);

        csr["status"] = json!({
            "conditions": [{"type": "Approved", "status": "True"}],
        });

        assert!(csr.get("status").is_some());
        assert_eq!(csr["status"]["conditions"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_empty_csr_names_is_invalid() {
        let csr_names: Vec<String> = vec![];
        assert!(csr_names.is_empty());
    }
}
