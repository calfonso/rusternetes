// CEL (Common Expression Language) evaluation engine
//
// This module provides CEL expression evaluation for ValidatingAdmissionPolicy

use anyhow::{anyhow, Result};
use cel::{
    objects::{Key, Map},
    Context, Program, Value,
};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;

/// CELEvaluator evaluates CEL expressions with given context
pub struct CELEvaluator {
    // Cache compiled programs for performance
    program_cache: HashMap<String, Program>,
}

impl CELEvaluator {
    /// Create a new CEL evaluator
    pub fn new() -> Self {
        Self {
            program_cache: HashMap::new(),
        }
    }

    /// Evaluate a CEL expression with the given context
    ///
    /// # Arguments
    /// * `expression` - The CEL expression to evaluate
    /// * `context` - Variables to make available in the CEL environment
    ///
    /// # Returns
    /// The result of the evaluation as a boolean, or an error
    pub fn evaluate(&mut self, expression: &str, context: &CELContext) -> Result<bool> {
        // Get or compile the program
        let program = if let Some(prog) = self.program_cache.get(expression) {
            prog
        } else {
            let prog = Program::compile(expression)
                .map_err(|e| anyhow!("Failed to compile CEL expression '{}': {}", expression, e))?;
            self.program_cache.insert(expression.to_string(), prog);
            self.program_cache.get(expression).unwrap()
        };

        // Create CEL context
        let mut cel_context = Context::default();

        // Add variables to context
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }

        // Execute the program
        let result = program
            .execute(&cel_context)
            .map_err(|e| anyhow!("Failed to execute CEL expression '{}': {}", expression, e))?;

        // Convert result to boolean
        match result {
            Value::Bool(b) => Ok(b),
            _ => Err(anyhow!(
                "CEL expression did not return a boolean: {:?}",
                result
            )),
        }
    }

    /// Evaluate a CEL expression that returns a string (for messages, audit annotations, etc.)
    pub fn evaluate_string(&mut self, expression: &str, context: &CELContext) -> Result<String> {
        // Get or compile the program
        let program = if let Some(prog) = self.program_cache.get(expression) {
            prog
        } else {
            let prog = Program::compile(expression)
                .map_err(|e| anyhow!("Failed to compile CEL expression '{}': {}", expression, e))?;
            self.program_cache.insert(expression.to_string(), prog);
            self.program_cache.get(expression).unwrap()
        };

        // Create CEL context
        let mut cel_context = Context::default();

        // Add variables to context
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }

        // Execute the program
        let result = program
            .execute(&cel_context)
            .map_err(|e| anyhow!("Failed to execute CEL expression '{}': {}", expression, e))?;

        // Convert result to string
        match result {
            Value::String(s) => Ok(s.to_string()),
            Value::Int(i) => Ok(i.to_string()),
            Value::UInt(u) => Ok(u.to_string()),
            Value::Float(f) => Ok(f.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            _ => Ok(format!("{:?}", result)),
        }
    }

    /// Evaluate a CEL expression and return the raw Value (for VAP variables)
    pub fn evaluate_to_value(&mut self, expression: &str, context: &CELContext) -> Result<Value> {
        let program = if let Some(prog) = self.program_cache.get(expression) {
            prog
        } else {
            let prog = Program::compile(expression)
                .map_err(|e| anyhow!("Failed to compile CEL expression '{}': {}", expression, e))?;
            self.program_cache.insert(expression.to_string(), prog);
            self.program_cache.get(expression).unwrap()
        };

        let mut cel_context = Context::default();
        for (key, value) in &context.variables {
            let _ = cel_context.add_variable(key.clone(), value.clone());
        }

        program
            .execute(&cel_context)
            .map_err(|e| anyhow!("Failed to execute CEL expression '{}': {}", expression, e))
    }

    /// Type-check a CEL expression without executing it
    pub fn type_check(&mut self, expression: &str) -> Result<()> {
        Program::compile(expression)
            .map_err(|e| anyhow!("Failed to compile CEL expression '{}': {}", expression, e))?;
        Ok(())
    }
}

impl Default for CELEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

/// CELContext holds variables available to CEL expressions
#[derive(Debug, Clone)]
pub struct CELContext {
    pub variables: HashMap<String, Value>,
}

impl CELContext {
    /// Create a new empty CEL context
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    /// Create a CEL context for admission control
    ///
    /// Provides standard variables:
    /// - `object`: The object being created/updated/deleted
    /// - `oldObject`: The existing object (for UPDATE operations)
    /// - `request`: The admission request
    /// - `params`: Parameters from the binding
    pub fn for_admission(
        object: &serde_json::Value,
        old_object: Option<&serde_json::Value>,
        params: Option<&serde_json::Value>,
    ) -> Result<Self> {
        let mut context = Self::new();

        // Add object
        context.add_json_variable("object", object)?;

        // Add oldObject if present
        if let Some(old) = old_object {
            context.add_json_variable("oldObject", old)?;
        }

        // Add params if present
        if let Some(p) = params {
            context.add_json_variable("params", p)?;
        }

        // Add request metadata (simplified for now)
        let request = serde_json::json!({
            "operation": "CREATE", // This should be dynamic
            "userInfo": {},
            "namespace": "",
        });
        context.add_json_variable("request", &request)?;

        Ok(context)
    }

    /// Add a variable to the context
    pub fn add_variable(&mut self, name: String, value: Value) {
        self.variables.insert(name, value);
    }

    /// Add a JSON value as a variable (converts to CEL Value)
    pub fn add_json_variable(&mut self, name: &str, value: &serde_json::Value) -> Result<()> {
        let cel_value = json_to_cel_value(value)?;
        self.variables.insert(name.to_string(), cel_value);
        Ok(())
    }
}

impl Default for CELContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a serde_json::Value to a CEL Value
fn json_to_cel_value(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(u) = n.as_u64() {
                Ok(Value::UInt(u))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(anyhow!("Invalid number: {}", n))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(std::sync::Arc::new(s.clone()))),
        serde_json::Value::Array(arr) => {
            let cel_values: Result<Vec<Value>> = arr.iter().map(json_to_cel_value).collect();
            Ok(Value::List(Arc::new(cel_values?)))
        }
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                let key = Key::String(Arc::new(k.clone()));
                map.insert(key, json_to_cel_value(v)?);
            }
            Ok(Value::Map(Map { map: Arc::new(map) }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cel_evaluator_simple() {
        let mut evaluator = CELEvaluator::new();
        let mut context = CELContext::new();
        context.add_variable("x".to_string(), Value::Int(5));
        context.add_variable("y".to_string(), Value::Int(3));

        let result = evaluator.evaluate("x > y", &context).unwrap();
        assert!(result);

        let result = evaluator.evaluate("x < y", &context).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_cel_evaluator_string() {
        let mut evaluator = CELEvaluator::new();
        let mut context = CELContext::new();
        context.add_variable(
            "name".to_string(),
            Value::String(Arc::new("test".to_string())),
        );

        let result = evaluator
            .evaluate_string("'Hello, ' + name", &context)
            .unwrap();
        assert_eq!(result, "Hello, test");
    }

    #[test]
    fn test_json_to_cel_value() {
        let json = serde_json::json!({
            "name": "test",
            "count": 42,
            "active": true,
            "tags": ["a", "b", "c"]
        });

        let cel_value = json_to_cel_value(&json).unwrap();
        match cel_value {
            Value::Map(map) => {
                let name_key = Key::String(Arc::new("name".to_string()));
                let count_key = Key::String(Arc::new("count".to_string()));
                let active_key = Key::String(Arc::new("active".to_string()));
                assert_eq!(
                    map.get(&name_key),
                    Some(&Value::String(Arc::new("test".to_string())))
                );
                assert_eq!(map.get(&count_key), Some(&Value::Int(42)));
                assert_eq!(map.get(&active_key), Some(&Value::Bool(true)));
            }
            _ => panic!("Expected map"),
        }
    }

    #[test]
    fn test_cel_context_for_admission() {
        let object = serde_json::json!({
            "spec": {
                "replicas": 3
            }
        });

        let context = CELContext::for_admission(&object, None, None).unwrap();
        assert!(context.variables.contains_key("object"));
        assert!(context.variables.contains_key("request"));
    }

    #[test]
    fn test_cel_admission_validation() {
        let mut evaluator = CELEvaluator::new();
        let object = serde_json::json!({
            "spec": {
                "replicas": 10
            }
        });

        let context = CELContext::for_admission(&object, None, None).unwrap();

        // This should fail because replicas > 5
        let result = evaluator
            .evaluate("object.spec.replicas <= 5", &context)
            .unwrap();
        assert!(!result);

        // This should pass
        let result = evaluator
            .evaluate("object.spec.replicas <= 100", &context)
            .unwrap();
        assert!(result);
    }

    #[test]
    fn test_type_check() {
        let mut evaluator = CELEvaluator::new();

        // Valid expression
        assert!(evaluator.type_check("x > 5").is_ok());

        // Note: Invalid expression testing skipped due to CEL library internals
        // The CEL library will catch syntax errors at runtime
    }

    /// Reproduces the K8s conformance test "should allow expressions to refer variables".
    /// A VAP defines:
    ///   variables: [{name: "replicas", expression: "object.spec.replicas"},
    ///               {name: "oddReplicas", expression: "variables.replicas % 2 == 1"}]
    ///   validations: [{expression: "variables.replicas > 1"},
    ///                 {expression: "variables.oddReplicas"}]
    /// With a 3-replica deployment, both validations should pass.
    #[test]
    fn test_cel_vap_variables_refer() {
        let mut evaluator = CELEvaluator::new();

        // Simulate a Deployment with 3 replicas
        let deployment = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "test-deploy", "namespace": "default"},
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c1", "image": "nginx"}]}
                }
            }
        });

        let mut context = CELContext::new();
        context.add_json_variable("object", &deployment).unwrap();

        // Step 1: Evaluate variable "replicas" = object.spec.replicas
        let replicas_val = evaluator
            .evaluate_to_value("object.spec.replicas", &context)
            .expect("should evaluate object.spec.replicas");
        assert_eq!(replicas_val, Value::Int(3));

        // Build the variables map incrementally (like the VAP code does)
        let mut var_map: HashMap<Key, Value> = HashMap::new();
        var_map.insert(Key::String(Arc::new("replicas".to_string())), replicas_val);
        context.add_variable(
            "variables".to_string(),
            Value::Map(Map {
                map: Arc::new(var_map.clone()),
            }),
        );

        // Step 2: Evaluate variable "oddReplicas" = variables.replicas % 2 == 1
        let odd_val = evaluator
            .evaluate_to_value("variables.replicas % 2 == 1", &context)
            .expect("should evaluate variables.replicas % 2 == 1");
        assert_eq!(odd_val, Value::Bool(true));

        var_map.insert(Key::String(Arc::new("oddReplicas".to_string())), odd_val);
        context.add_variable(
            "variables".to_string(),
            Value::Map(Map {
                map: Arc::new(var_map),
            }),
        );

        // Step 3: Evaluate validations
        let v1 = evaluator
            .evaluate("variables.replicas > 1", &context)
            .expect("should evaluate variables.replicas > 1");
        assert!(v1, "3 > 1 should be true");

        let v2 = evaluator
            .evaluate("variables.oddReplicas", &context)
            .expect("should evaluate variables.oddReplicas");
        assert!(v2, "oddReplicas (3 % 2 == 1) should be true");
    }

    /// Test that 1-replica deployment fails variables-based validation.
    #[test]
    fn test_cel_vap_variables_reject_low_replicas() {
        let mut evaluator = CELEvaluator::new();

        let deployment = serde_json::json!({
            "spec": {"replicas": 1}
        });

        let mut context = CELContext::new();
        context.add_json_variable("object", &deployment).unwrap();

        let replicas_val = evaluator
            .evaluate_to_value("object.spec.replicas", &context)
            .unwrap();

        let mut var_map: HashMap<Key, Value> = HashMap::new();
        var_map.insert(Key::String(Arc::new("replicas".to_string())), replicas_val);
        context.add_variable(
            "variables".to_string(),
            Value::Map(Map {
                map: Arc::new(var_map),
            }),
        );

        // 1 > 1 should be false
        let v1 = evaluator
            .evaluate("variables.replicas > 1", &context)
            .unwrap();
        assert!(!v1, "1 > 1 should be false");
    }

    /// Test namespaceObject in CEL context (used by "should validate against a Deployment").
    #[test]
    fn test_cel_namespace_object() {
        let mut evaluator = CELEvaluator::new();

        let ns = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "test-ns"}
        });

        let mut context = CELContext::new();
        context.add_json_variable("namespaceObject", &ns).unwrap();

        let result = evaluator
            .evaluate("namespaceObject.metadata.name == 'test-ns'", &context)
            .unwrap();
        assert!(result);
    }
}
