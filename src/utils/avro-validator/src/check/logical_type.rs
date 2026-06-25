use serde_json::Value;

use super::{Defs, Rule};

/// Any logical type — and, because we read the raw JSON, the physical type it
/// annotates (which apache_avro's `Schema` collapses away).
pub(super) struct LogicalType;

impl Rule for LogicalType {
    fn id(&self) -> &'static str {
        "logical-type"
    }

    fn check(&self, node: &Value, _ns: &str, _defs: &Defs) -> Option<String> {
        let obj = node.as_object()?;
        let logical = obj.get("logicalType")?.as_str()?;
        let physical = obj.get("type").and_then(Value::as_str).unwrap_or("?");
        Some(format!("{logical} (on {physical})"))
    }
}
