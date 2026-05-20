//! JSON roundtrip tests for `networking.k8s.io/v1` and `discovery.k8s.io/v1` resources.
//!
//! These tests mirror the upstream Kubernetes "roundtrip" layer (layer #1 in the
//! kube-apiserver test stack). Each fixture is a hand-written JSON document that:
//!
//! 1. Deserialises into the strongly-typed Rust resource via `serde_json::from_str`,
//! 2. Re-serialises back to a JSON string,
//! 3. Re-deserialises from that string, and
//! 4. Compares the two decoded values for equality.
//!
//! Equality is asserted at the typed level when the resource derives `PartialEq`.
//! For resources that do not (`NetworkPolicy`, `EndpointSlice`), we compare
//! `serde_json::Value` round-trips, which is equivalent for a stable serializer
//! (it normalises field order / whitespace / map ordering).
//!
//! Resources covered:
//! - `Ingress` (TLS, pathType Prefix/Exact/ImplementationSpecific, defaultBackend, multi-rule)
//! - `IngressClass` (controller only, cluster/namespaced parameters)
//! - `NetworkPolicy` (podSelector, ingress + egress, ipBlock, namespaceSelector, named ports)
//! - `EndpointSlice` (IPv4/IPv6/FQDN, multi-port, topology hints)

use rusternetes_common::resources::{EndpointSlice, Ingress, IngressClass, NetworkPolicy};

// ---------- helpers ----------

/// Roundtrip a fixture through a type that derives `PartialEq`.
///
/// Asserts that decode + re-encode + re-decode produces an equal value.
fn assert_roundtrip_eq<T>(fixture: &str)
where
    T: serde::de::DeserializeOwned + serde::Serialize + PartialEq + std::fmt::Debug,
{
    let decoded: T =
        serde_json::from_str(fixture).expect("fixture should decode into the target type");

    let re_encoded =
        serde_json::to_string(&decoded).expect("decoded value should re-encode to JSON");

    let re_decoded: T = serde_json::from_str(&re_encoded)
        .expect("re-encoded JSON should decode into the target type");

    assert_eq!(
        decoded, re_decoded,
        "roundtrip should be stable: decoded != re_decoded"
    );
}

/// Roundtrip a fixture through a type that does NOT derive `PartialEq`.
///
/// Falls back to `serde_json::Value` equality on the canonical re-encoded form.
fn assert_roundtrip_value_eq<T>(fixture: &str)
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let decoded: T =
        serde_json::from_str(fixture).expect("fixture should decode into the target type");

    let first_encode =
        serde_json::to_string(&decoded).expect("decoded value should re-encode to JSON");

    let re_decoded: T = serde_json::from_str(&first_encode)
        .expect("re-encoded JSON should decode into the target type");

    let second_encode =
        serde_json::to_string(&re_decoded).expect("re-decoded value should re-encode to JSON");

    let first_value: serde_json::Value = serde_json::from_str(&first_encode).unwrap();
    let second_value: serde_json::Value = serde_json::from_str(&second_encode).unwrap();

    assert_eq!(
        first_value, second_value,
        "roundtrip should be stable: first encode != second encode"
    );
}

// =====================================================================
// Ingress (networking.k8s.io/v1)
// =====================================================================

#[test]
fn roundtrip_ingress_minimal() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "minimal",
            "namespace": "default"
        }
    }"#;
    assert_roundtrip_eq::<Ingress>(fixture);
}

#[test]
fn roundtrip_ingress_default_backend_only() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "default-backend",
            "namespace": "default"
        },
        "spec": {
            "ingressClassName": "nginx",
            "defaultBackend": {
                "service": {
                    "name": "fallback",
                    "port": {
                        "number": 8080
                    }
                }
            }
        }
    }"#;
    assert_roundtrip_eq::<Ingress>(fixture);
}

#[test]
fn roundtrip_ingress_path_type_prefix() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "prefix-rules",
            "namespace": "web"
        },
        "spec": {
            "ingressClassName": "nginx",
            "rules": [
                {
                    "host": "example.com",
                    "http": {
                        "paths": [
                            {
                                "path": "/api",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "api",
                                        "port": {
                                            "number": 80
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }"#;
    assert_roundtrip_eq::<Ingress>(fixture);
}

#[test]
fn roundtrip_ingress_path_type_exact() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "exact-rules",
            "namespace": "web"
        },
        "spec": {
            "rules": [
                {
                    "host": "exact.example.com",
                    "http": {
                        "paths": [
                            {
                                "path": "/healthz",
                                "pathType": "Exact",
                                "backend": {
                                    "service": {
                                        "name": "health",
                                        "port": {
                                            "name": "http"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }"#;
    assert_roundtrip_eq::<Ingress>(fixture);
}

#[test]
fn roundtrip_ingress_path_type_implementation_specific() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "impl-specific",
            "namespace": "legacy"
        },
        "spec": {
            "rules": [
                {
                    "host": "legacy.example.com",
                    "http": {
                        "paths": [
                            {
                                "path": "/.*",
                                "pathType": "ImplementationSpecific",
                                "backend": {
                                    "service": {
                                        "name": "legacy",
                                        "port": {
                                            "number": 443
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }"#;
    assert_roundtrip_eq::<Ingress>(fixture);
}

#[test]
fn roundtrip_ingress_with_tls() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "tls-ingress",
            "namespace": "secure"
        },
        "spec": {
            "ingressClassName": "nginx",
            "tls": [
                {
                    "hosts": ["secure.example.com", "www.secure.example.com"],
                    "secretName": "secure-tls"
                },
                {
                    "hosts": ["api.secure.example.com"],
                    "secretName": "api-tls"
                }
            ],
            "rules": [
                {
                    "host": "secure.example.com",
                    "http": {
                        "paths": [
                            {
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "web",
                                        "port": {
                                            "number": 443
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }"#;
    assert_roundtrip_eq::<Ingress>(fixture);
}

#[test]
fn roundtrip_ingress_multiple_rules_and_paths() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "multi-rule",
            "namespace": "platform",
            "labels": {
                "app.kubernetes.io/name": "platform-router"
            }
        },
        "spec": {
            "ingressClassName": "nginx",
            "defaultBackend": {
                "service": {
                    "name": "default-svc",
                    "port": {
                        "number": 80
                    }
                }
            },
            "rules": [
                {
                    "host": "a.example.com",
                    "http": {
                        "paths": [
                            {
                                "path": "/v1",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "svc-v1",
                                        "port": {
                                            "number": 80
                                        }
                                    }
                                }
                            },
                            {
                                "path": "/v2",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "svc-v2",
                                        "port": {
                                            "number": 80
                                        }
                                    }
                                }
                            }
                        ]
                    }
                },
                {
                    "host": "b.example.com",
                    "http": {
                        "paths": [
                            {
                                "path": "/foo",
                                "pathType": "Exact",
                                "backend": {
                                    "service": {
                                        "name": "svc-foo",
                                        "port": {
                                            "name": "http"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }
    }"#;
    assert_roundtrip_eq::<Ingress>(fixture);
}

#[test]
fn roundtrip_ingress_with_status_loadbalancer() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "status-ingress",
            "namespace": "default"
        },
        "spec": {
            "ingressClassName": "nginx",
            "rules": [
                {
                    "host": "status.example.com",
                    "http": {
                        "paths": [
                            {
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": "svc",
                                        "port": {
                                            "number": 80
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        },
        "status": {
            "loadBalancer": {
                "ingress": [
                    {
                        "ip": "203.0.113.10",
                        "hostname": "lb.example.com",
                        "ports": [
                            {
                                "port": 80,
                                "protocol": "TCP"
                            },
                            {
                                "port": 443,
                                "protocol": "TCP",
                                "error": "cert-pending"
                            }
                        ]
                    }
                ]
            }
        }
    }"#;
    assert_roundtrip_eq::<Ingress>(fixture);
}

// =====================================================================
// IngressClass (networking.k8s.io/v1)
// =====================================================================

#[test]
fn roundtrip_ingressclass_minimal() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IngressClass",
        "metadata": {
            "name": "nginx"
        }
    }"#;
    assert_roundtrip_eq::<IngressClass>(fixture);
}

#[test]
fn roundtrip_ingressclass_controller_only() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IngressClass",
        "metadata": {
            "name": "nginx",
            "annotations": {
                "ingressclass.kubernetes.io/is-default-class": "true"
            }
        },
        "spec": {
            "controller": "k8s.io/ingress-nginx"
        }
    }"#;
    assert_roundtrip_eq::<IngressClass>(fixture);
}

#[test]
fn roundtrip_ingressclass_cluster_scoped_parameters() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IngressClass",
        "metadata": {
            "name": "acme"
        },
        "spec": {
            "controller": "acme.io/ingress-controller",
            "parameters": {
                "apiGroup": "acme.io",
                "kind": "IngressConfig",
                "name": "global-config",
                "scope": "Cluster"
            }
        }
    }"#;
    assert_roundtrip_eq::<IngressClass>(fixture);
}

#[test]
fn roundtrip_ingressclass_namespace_scoped_parameters() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IngressClass",
        "metadata": {
            "name": "external-lb"
        },
        "spec": {
            "controller": "example.com/ingress-controller",
            "parameters": {
                "apiGroup": "k8s.example.com",
                "kind": "IngressParameters",
                "name": "external-lb",
                "namespace": "ingress-system",
                "scope": "Namespace"
            }
        }
    }"#;
    assert_roundtrip_eq::<IngressClass>(fixture);
}

#[test]
fn roundtrip_ingressclass_core_api_group_parameters() {
    // apiGroup omitted -> core API group (configmap reference)
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IngressClass",
        "metadata": {
            "name": "core-params"
        },
        "spec": {
            "controller": "example.com/ingress-controller",
            "parameters": {
                "kind": "ConfigMap",
                "name": "ingress-config",
                "namespace": "kube-system",
                "scope": "Namespace"
            }
        }
    }"#;
    assert_roundtrip_eq::<IngressClass>(fixture);
}

// =====================================================================
// NetworkPolicy (networking.k8s.io/v1)
// =====================================================================
//
// `NetworkPolicy` does not derive `PartialEq`, so we use JSON-Value equality.

#[test]
fn roundtrip_networkpolicy_pod_selector_only() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "default-deny",
            "namespace": "default"
        },
        "spec": {
            "podSelector": {},
            "policyTypes": ["Ingress"]
        }
    }"#;
    assert_roundtrip_value_eq::<NetworkPolicy>(fixture);
}

#[test]
fn roundtrip_networkpolicy_ingress_from_pod_selector() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "allow-from-app",
            "namespace": "default"
        },
        "spec": {
            "podSelector": {
                "matchLabels": {
                    "app": "db"
                }
            },
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {
                            "podSelector": {
                                "matchLabels": {
                                    "app": "web"
                                }
                            }
                        }
                    ],
                    "ports": [
                        {
                            "protocol": "TCP",
                            "port": 5432
                        }
                    ]
                }
            ]
        }
    }"#;
    assert_roundtrip_value_eq::<NetworkPolicy>(fixture);
}

#[test]
fn roundtrip_networkpolicy_ingress_namespace_selector() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "allow-from-monitoring",
            "namespace": "production"
        },
        "spec": {
            "podSelector": {
                "matchLabels": {
                    "tier": "backend"
                }
            },
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "purpose": "monitoring"
                                }
                            }
                        }
                    ],
                    "ports": [
                        {
                            "protocol": "TCP",
                            "port": "metrics"
                        }
                    ]
                }
            ]
        }
    }"#;
    assert_roundtrip_value_eq::<NetworkPolicy>(fixture);
}

#[test]
fn roundtrip_networkpolicy_ingress_ip_block() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "allow-cidr",
            "namespace": "default"
        },
        "spec": {
            "podSelector": {
                "matchLabels": {
                    "app": "api"
                }
            },
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {
                            "ipBlock": {
                                "cidr": "10.0.0.0/8",
                                "except": [
                                    "10.0.0.0/24",
                                    "10.0.1.0/24"
                                ]
                            }
                        }
                    ],
                    "ports": [
                        {
                            "protocol": "TCP",
                            "port": 8080,
                            "endPort": 8090
                        }
                    ]
                }
            ]
        }
    }"#;
    assert_roundtrip_value_eq::<NetworkPolicy>(fixture);
}

#[test]
fn roundtrip_networkpolicy_egress_only() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "allow-egress-dns",
            "namespace": "default"
        },
        "spec": {
            "podSelector": {},
            "policyTypes": ["Egress"],
            "egress": [
                {
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "kubernetes.io/metadata.name": "kube-system"
                                }
                            },
                            "podSelector": {
                                "matchLabels": {
                                    "k8s-app": "kube-dns"
                                }
                            }
                        }
                    ],
                    "ports": [
                        {
                            "protocol": "UDP",
                            "port": 53
                        },
                        {
                            "protocol": "TCP",
                            "port": 53
                        }
                    ]
                }
            ]
        }
    }"#;
    assert_roundtrip_value_eq::<NetworkPolicy>(fixture);
}

#[test]
fn roundtrip_networkpolicy_ingress_and_egress() {
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "full-policy",
            "namespace": "production"
        },
        "spec": {
            "podSelector": {
                "matchExpressions": [
                    {
                        "key": "tier",
                        "operator": "In",
                        "values": ["backend", "frontend"]
                    }
                ]
            },
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [
                {
                    "from": [
                        {
                            "podSelector": {
                                "matchLabels": {
                                    "role": "client"
                                }
                            },
                            "namespaceSelector": {
                                "matchLabels": {
                                    "env": "prod"
                                }
                            }
                        },
                        {
                            "ipBlock": {
                                "cidr": "172.16.0.0/12"
                            }
                        }
                    ],
                    "ports": [
                        {
                            "protocol": "TCP",
                            "port": 8443
                        }
                    ]
                }
            ],
            "egress": [
                {
                    "to": [
                        {
                            "ipBlock": {
                                "cidr": "0.0.0.0/0",
                                "except": ["10.0.0.0/8"]
                            }
                        }
                    ],
                    "ports": [
                        {
                            "protocol": "TCP",
                            "port": 443
                        }
                    ]
                }
            ]
        }
    }"#;
    assert_roundtrip_value_eq::<NetworkPolicy>(fixture);
}

#[test]
fn roundtrip_networkpolicy_allow_all() {
    // Empty `from` + empty `ports` means "allow all" upstream semantics.
    let fixture = r#"{
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "allow-all",
            "namespace": "default"
        },
        "spec": {
            "podSelector": {},
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [{}],
            "egress": [{}]
        }
    }"#;
    assert_roundtrip_value_eq::<NetworkPolicy>(fixture);
}

// =====================================================================
// EndpointSlice (discovery.k8s.io/v1)
// =====================================================================
//
// `EndpointSlice` itself does not derive `PartialEq`, so we use JSON-Value
// equality. The nested types (`Endpoint`, `EndpointPort`, …) do, but the
// outermost roundtrip still needs the value-comparison helper.

#[test]
fn roundtrip_endpointslice_ipv4_minimal() {
    let fixture = r#"{
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "my-svc-abc",
            "namespace": "default",
            "labels": {
                "kubernetes.io/service-name": "my-svc"
            }
        },
        "addressType": "IPv4",
        "endpoints": [
            {
                "addresses": ["10.1.2.3"],
                "conditions": {
                    "ready": true,
                    "serving": true,
                    "terminating": false
                }
            }
        ],
        "ports": [
            {
                "name": "http",
                "port": 80,
                "protocol": "TCP"
            }
        ]
    }"#;
    assert_roundtrip_value_eq::<EndpointSlice>(fixture);
}

#[test]
fn roundtrip_endpointslice_ipv6() {
    let fixture = r#"{
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "my-svc-v6",
            "namespace": "default"
        },
        "addressType": "IPv6",
        "endpoints": [
            {
                "addresses": ["2001:db8::1", "2001:db8::2"],
                "conditions": {
                    "ready": true
                },
                "hostname": "pod-1",
                "nodeName": "node-1",
                "zone": "us-east-1a"
            }
        ],
        "ports": [
            {
                "name": "https",
                "port": 443,
                "protocol": "TCP",
                "appProtocol": "https"
            }
        ]
    }"#;
    assert_roundtrip_value_eq::<EndpointSlice>(fixture);
}

#[test]
fn roundtrip_endpointslice_fqdn() {
    let fixture = r#"{
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "external-svc-fqdn",
            "namespace": "default"
        },
        "addressType": "FQDN",
        "endpoints": [
            {
                "addresses": ["external.example.com"],
                "conditions": {
                    "ready": true,
                    "serving": true,
                    "terminating": false
                }
            }
        ],
        "ports": [
            {
                "port": 443,
                "protocol": "TCP"
            }
        ]
    }"#;
    assert_roundtrip_value_eq::<EndpointSlice>(fixture);
}

#[test]
fn roundtrip_endpointslice_multiple_ports() {
    let fixture = r#"{
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "multi-port-svc",
            "namespace": "platform",
            "labels": {
                "kubernetes.io/service-name": "multi-port-svc",
                "endpointslice.kubernetes.io/managed-by": "endpointslice-controller.k8s.io"
            }
        },
        "addressType": "IPv4",
        "endpoints": [
            {
                "addresses": ["10.0.0.5"],
                "conditions": {
                    "ready": true,
                    "serving": true,
                    "terminating": false
                },
                "targetRef": {
                    "kind": "Pod",
                    "namespace": "platform",
                    "name": "backend-7d8f-abc",
                    "uid": "11111111-2222-3333-4444-555555555555"
                },
                "nodeName": "node-1",
                "zone": "us-west-2a"
            }
        ],
        "ports": [
            {
                "name": "http",
                "port": 8080,
                "protocol": "TCP",
                "appProtocol": "http"
            },
            {
                "name": "grpc",
                "port": 9090,
                "protocol": "TCP",
                "appProtocol": "grpc"
            },
            {
                "name": "metrics",
                "port": 9100,
                "protocol": "TCP"
            }
        ]
    }"#;
    assert_roundtrip_value_eq::<EndpointSlice>(fixture);
}

#[test]
fn roundtrip_endpointslice_with_topology_hints() {
    let fixture = r#"{
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "topology-slice",
            "namespace": "default"
        },
        "addressType": "IPv4",
        "endpoints": [
            {
                "addresses": ["10.0.0.10"],
                "conditions": {
                    "ready": true
                },
                "zone": "us-east-1a",
                "nodeName": "node-1",
                "hints": {
                    "forZones": [
                        {"name": "us-east-1a"},
                        {"name": "us-east-1b"}
                    ]
                }
            },
            {
                "addresses": ["10.0.0.11"],
                "conditions": {
                    "ready": true
                },
                "zone": "us-east-1b",
                "nodeName": "node-2",
                "hints": {
                    "forZones": [
                        {"name": "us-east-1b"}
                    ]
                }
            }
        ],
        "ports": [
            {
                "name": "http",
                "port": 80,
                "protocol": "TCP"
            }
        ]
    }"#;
    assert_roundtrip_value_eq::<EndpointSlice>(fixture);
}

#[test]
fn roundtrip_endpointslice_not_ready_endpoint() {
    let fixture = r#"{
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "mixed-readiness",
            "namespace": "default"
        },
        "addressType": "IPv4",
        "endpoints": [
            {
                "addresses": ["10.0.0.20"],
                "conditions": {
                    "ready": true,
                    "serving": true,
                    "terminating": false
                }
            },
            {
                "addresses": ["10.0.0.21"],
                "conditions": {
                    "ready": false,
                    "serving": true,
                    "terminating": true
                },
                "targetRef": {
                    "kind": "Pod",
                    "namespace": "default",
                    "name": "terminating-pod"
                }
            },
            {
                "addresses": ["10.0.0.22"],
                "conditions": {
                    "ready": false,
                    "serving": false,
                    "terminating": false
                }
            }
        ],
        "ports": [
            {
                "name": "http",
                "port": 80,
                "protocol": "TCP"
            }
        ]
    }"#;
    assert_roundtrip_value_eq::<EndpointSlice>(fixture);
}

#[test]
fn roundtrip_endpointslice_multiple_addresses_per_endpoint() {
    // Per upstream: addresses[] may contain multiple addresses of the same type
    // (e.g. dual-IPv4 NICs for a single endpoint).
    let fixture = r#"{
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "dual-nic-slice",
            "namespace": "default"
        },
        "addressType": "IPv4",
        "endpoints": [
            {
                "addresses": ["10.0.0.30", "10.0.0.31"],
                "conditions": {
                    "ready": true,
                    "serving": true,
                    "terminating": false
                },
                "hostname": "dual-nic-host",
                "nodeName": "node-3"
            }
        ],
        "ports": [
            {
                "port": 80,
                "protocol": "TCP"
            }
        ]
    }"#;
    assert_roundtrip_value_eq::<EndpointSlice>(fixture);
}

#[test]
fn roundtrip_endpointslice_no_ports_headless() {
    // Headless service variant: addressType set, endpoints present, no ports.
    let fixture = r#"{
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "headless-slice",
            "namespace": "default",
            "labels": {
                "kubernetes.io/service-name": "headless"
            }
        },
        "addressType": "IPv4",
        "endpoints": [
            {
                "addresses": ["10.0.0.40"],
                "conditions": {
                    "ready": true
                },
                "hostname": "pod-0"
            }
        ]
    }"#;
    assert_roundtrip_value_eq::<EndpointSlice>(fixture);
}
