use rusternetes_common::resources::certificates::CertificateSigningRequestCondition as Cond;
use rusternetes_common::validation::certificatesigningrequest::validate_csr_status_conditions;

fn cond(t: &str, s: &str) -> Cond {
    Cond {
        type_: t.to_string(),
        status: s.to_string(),
        reason: None,
        message: None,
        last_update_time: None,
        last_transition_time: None,
    }
}

#[test]
fn approved_true_ok() {
    assert!(validate_csr_status_conditions(&[cond("Approved", "True")]).is_empty());
}

#[test]
fn approved_and_denied_mutually_exclusive() {
    let errs = validate_csr_status_conditions(&[cond("Approved", "True"), cond("Denied", "True")]);
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("mutually exclusive")),
        "{errs:?}"
    );
}

#[test]
fn approved_must_be_true() {
    let errs = validate_csr_status_conditions(&[cond("Approved", "False")]);
    assert!(
        !errs.is_empty(),
        "Approved with status False must be rejected"
    );
}

#[test]
fn empty_type_rejected() {
    let errs = validate_csr_status_conditions(&[cond("", "True")]);
    assert!(
        errs.iter().any(|e| e.to_string().contains("type")),
        "{errs:?}"
    );
}

#[test]
fn bad_status_value_rejected() {
    let errs = validate_csr_status_conditions(&[cond("SomeType", "Bogus")]);
    assert!(
        errs.iter().any(|e| e.to_string().contains("status")),
        "{errs:?}"
    );
}
