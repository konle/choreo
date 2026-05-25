use rhai::{AST, Array, Dynamic, Engine, Map, Scope};
use serde_json::Value as JsonValue;

const MAX_OPERATIONS: u64 = 100_000;

pub fn create_engine() -> Engine {
    let mut engine = Engine::new();
    engine.set_max_operations(MAX_OPERATIONS);
    engine
}

pub fn compile_script(engine: &Engine, script: &str) -> anyhow::Result<AST> {
    engine
        .compile(script)
        .map_err(|e| anyhow::anyhow!("Rhai compile error: {}", e))
}

/// Inject a JSON value into the Rhai scope as variable `ctx`.
pub fn inject_context(scope: &mut Scope, ctx: &JsonValue) {
    scope.push("ctx", json_to_dynamic(ctx));
}

/// Inject each top-level key of a JSON object into the Rhai scope as individual variables.
pub fn inject_context_flat(scope: &mut Scope, ctx: &JsonValue) {
    if let Some(obj) = ctx.as_object() {
        for (k, v) in obj {
            scope.push(k.as_str(), json_to_dynamic(v));
        }
    }
}

pub fn json_to_dynamic(val: &JsonValue) -> Dynamic {
    match val {
        JsonValue::Null => Dynamic::UNIT,
        JsonValue::Bool(b) => Dynamic::from(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Dynamic::from(i)
            } else if let Some(f) = n.as_f64() {
                Dynamic::from(f)
            } else {
                Dynamic::UNIT
            }
        }
        JsonValue::String(s) => Dynamic::from(s.clone()),
        JsonValue::Array(arr) => {
            let rhai_arr: Array = arr.iter().map(json_to_dynamic).collect();
            Dynamic::from_array(rhai_arr)
        }
        JsonValue::Object(obj) => {
            let mut rhai_map = Map::new();
            for (k, v) in obj {
                rhai_map.insert(k.clone().into(), json_to_dynamic(v));
            }
            Dynamic::from_map(rhai_map)
        }
    }
}

pub fn dynamic_to_json(val: &Dynamic) -> JsonValue {
    if val.is_unit() {
        JsonValue::Null
    } else if val.is_bool() {
        JsonValue::Bool(val.as_bool().unwrap_or_default())
    } else if val.is_int() {
        serde_json::json!(val.as_int().unwrap_or_default())
    } else if val.is_float() {
        serde_json::json!(val.as_float().unwrap_or_default())
    } else if val.is_string() {
        JsonValue::String(val.clone().into_string().unwrap_or_default())
    } else if val.is_array() {
        let arr = val.clone().into_array().unwrap_or_default();
        JsonValue::Array(arr.iter().map(dynamic_to_json).collect())
    } else if val.is_map() {
        let map = val.clone().into_typed_array::<(String, Dynamic)>().ok();
        if let Some(pairs) = map {
            let mut obj = serde_json::Map::new();
            for (k, v) in pairs {
                obj.insert(k, dynamic_to_json(&v));
            }
            JsonValue::Object(obj)
        } else {
            // Try as rhai::Map directly
            match val.clone().try_cast::<Map>() {
                Some(rhai_map) => {
                    let mut obj = serde_json::Map::new();
                    for (k, v) in rhai_map {
                        obj.insert(k.to_string(), dynamic_to_json(&v));
                    }
                    JsonValue::Object(obj)
                }
                None => JsonValue::String(val.to_string()),
            }
        }
    } else {
        JsonValue::String(val.to_string())
    }
}

/// Convert a Rhai Map result to a serde_json Map.
pub fn rhai_map_to_json(dynamic: Dynamic) -> anyhow::Result<serde_json::Map<String, JsonValue>> {
    let rhai_map: Map = dynamic
        .try_cast()
        .ok_or_else(|| anyhow::anyhow!("script must return a Map (#{{ ... }})"))?;
    let mut result = serde_json::Map::new();
    for (k, v) in rhai_map {
        result.insert(k.to_string(), dynamic_to_json(&v));
    }
    Ok(result)
}
