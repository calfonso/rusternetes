//! Shared helpers for reading Kubernetes manifests from a file (or stdin) and
//! splitting them into individual YAML documents.
//!
//! `apply`, `create -f`, `diff -f`, and `delete -f` all need the same
//! "read the source, then parse the multi-document YAML into a list of
//! documents" step. This module centralises that so the four commands cannot
//! drift apart.

use anyhow::{Context, Result};
use serde::Deserialize;

/// Read a manifest source (a file path, or `-` for stdin) and parse it into its
/// YAML documents.
///
/// Multi-document YAML (documents separated by `---`) is supported. Empty
/// documents — including a trailing `---` with nothing after it — are skipped,
/// matching the behaviour each command previously implemented inline.
pub fn read_documents(source: &str) -> Result<Vec<serde_yaml::Value>> {
    let contents = if source == "-" {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)
            .context("failed to read manifest from stdin")?;
        buf
    } else {
        std::fs::read_to_string(source)
            .with_context(|| format!("failed to read manifest file {source}"))?
    };

    let mut docs = Vec::new();
    for de in serde_yaml::Deserializer::from_str(&contents) {
        let value = serde_yaml::Value::deserialize(de)?;
        // Skip empty documents / trailing `---`.
        if !value.is_null() {
            docs.push(value);
        }
    }
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp(contents: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_read_documents_single() {
        let f = write_temp("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n");
        let docs = read_documents(f.path().to_str().unwrap()).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(
            docs[0].get("kind").and_then(|k| k.as_str()),
            Some("ConfigMap")
        );
    }

    #[test]
    fn test_read_documents_two_docs() {
        let f = write_temp(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
        );
        let docs = read_documents(f.path().to_str().unwrap()).unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(
            docs[0]
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str()),
            Some("a")
        );
        assert_eq!(
            docs[1]
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str()),
            Some("b")
        );
    }

    #[test]
    fn test_read_documents_trailing_separator_skipped() {
        // A trailing `---` produces an extra empty (null) document that must be
        // skipped, leaving only the real document.
        let f = write_temp("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\n");
        let docs = read_documents(f.path().to_str().unwrap()).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(
            docs[0]
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str()),
            Some("a")
        );
    }

    #[test]
    fn test_read_documents_empty_middle_doc_skipped() {
        // An empty document in the middle (between two `---`) is also skipped.
        let f = write_temp(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
        );
        let docs = read_documents(f.path().to_str().unwrap()).unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_read_documents_missing_file_errors() {
        let result = read_documents("/nonexistent/path/xyz.yaml");
        assert!(result.is_err());
    }
}
