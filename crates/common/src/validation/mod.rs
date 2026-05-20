//! Validation primitives ported from upstream Kubernetes
//! `k8s.io/apimachinery/pkg/util/validation` and
//! `k8s.io/apimachinery/pkg/apis/meta/v1/validation`.
//!
//! See [`field`] for the [`field::Error`] / [`field::Path`] types and
//! [`metav1`] for the metav1 validators.

pub mod field;
pub mod metav1;
pub mod objectmeta;
pub mod pod;
