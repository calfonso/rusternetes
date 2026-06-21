//! Validation primitives ported from upstream Kubernetes
//! `k8s.io/apimachinery/pkg/util/validation` and
//! `k8s.io/apimachinery/pkg/apis/meta/v1/validation`.
//!
//! See [`field`] for the [`field::Error`] / [`field::Path`] types and
//! [`metav1`] for the metav1 validators.

pub mod apps;
pub mod certificatesigningrequest;
pub mod csistoragecapacity;
pub mod endpoints;
pub mod endpointslice;
pub mod events;
pub mod field;
pub mod hpa;
pub mod ingress;
pub mod lease;
pub mod limitrange;
pub mod metav1;
pub mod namespace;
pub mod networkpolicy;
pub mod node;
pub mod objectmeta;
pub mod pdb;
pub mod pod;
pub mod podtemplate;
pub mod pvc;
pub mod resourcequota;
pub mod service;
pub mod storageclass;
pub mod volumeattributesclass;
