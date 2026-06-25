use serde_json::Value;

use super::{Defs, Rule};

/// Named types (record/enum/fixed) and record fields that declare `aliases` — the
/// alternate names Avro uses when resolving a writer schema against a reader schema.
pub(super) struct Aliases;

impl Rule for Aliases {
    fn id(&self) -> &'static str {
        "aliases"
    }

    fn check(&self, node: &Value, _ns: &str, _defs: &Defs) -> Option<String> {
        let obj = node.as_object()?;
        let kind = obj.get("type").and_then(Value::as_str);
        let mut parts = Vec::new();
        // A named type may carry its own aliases.
        if matches!(kind, Some("record" | "enum" | "fixed"))
            && let Some(list) = alias_list(node)
        {
            parts.push(format!("[{list}]"));
        }
        // Record fields may carry aliases too; the field wrapper is not itself a
        // visited node, so reach them from the record.
        if kind == Some("record") {
            for field in obj
                .get("fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(list) = alias_list(field) {
                    let name = field.get("name").and_then(Value::as_str).unwrap_or("?");
                    parts.push(format!("{name} [{list}]"));
                }
            }
        }
        (!parts.is_empty()).then(|| parts.join("; "))
    }
}

/// The non-empty `aliases` of a record/enum/fixed or field object, comma-joined.
fn alias_list(node: &Value) -> Option<String> {
    let names: Vec<&str> = node
        .as_object()?
        .get("aliases")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .collect();
    (!names.is_empty()).then(|| names.join(", "))
}
