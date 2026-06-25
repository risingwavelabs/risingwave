use serde_json::Value;

use super::{Defs, Rule, is_named, resolve, type_label};

/// Record fields that declare a `default`, reported with the field's type. A union
/// whose first branch is `null` is skipped (the unremarkable nullable-to-null case);
/// otherwise the default's type is the union's first branch. A reference is resolved
/// to its definition's kind, and a logical type is shown over its physical type.
pub(super) struct FieldDefault;

impl Rule for FieldDefault {
    fn id(&self) -> &'static str {
        "field-default"
    }

    fn check(&self, node: &Value, ns: &str, defs: &Defs) -> Option<String> {
        let obj = node.as_object()?;
        if obj.get("type").and_then(Value::as_str) != Some("record") {
            return None;
        }
        let mut parts = Vec::new();
        for field in obj
            .get("fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if field.get("default").is_none() {
                continue;
            }
            let Some(ty) = field.get("type") else {
                continue;
            };
            // A union default matches the union's first branch (Avro spec); a null
            // first branch is the unremarkable nullable-to-null case, so skip it.
            let reported = match ty.as_array() {
                Some(branches) => match branches.first() {
                    Some(first) if is_null(first) => continue,
                    Some(first) => first,
                    None => continue,
                },
                None => ty,
            };
            let name = field.get("name").and_then(Value::as_str).unwrap_or("?");
            parts.push(format!("{name}: {}", describe(reported, ns, defs)));
        }
        (!parts.is_empty()).then(|| parts.join("; "))
    }
}

/// Whether a union branch is the null type (`"null"` or `{"type":"null"}`).
fn is_null(branch: &Value) -> bool {
    match branch {
        Value::String(s) => s == "null",
        Value::Object(o) => o.get("type").and_then(Value::as_str) == Some("null"),
        _ => false,
    }
}

/// A label for a default-bearing field's type: a logical type over its physical type,
/// a named type by fullname and kind (references resolved), else the plain type.
fn describe(ty: &Value, ns: &str, defs: &Defs) -> String {
    if let Some(obj) = ty.as_object()
        && let Some(logical) = obj.get("logicalType").and_then(Value::as_str)
    {
        let physical = obj.get("type").and_then(Value::as_str).unwrap_or("?");
        return format!("{logical} (on {physical})");
    }
    if is_named(ty) {
        let label = type_label(ty, ns);
        return match named_kind(ty, ns, defs) {
            Some(kind) => format!("{label} ({kind})"),
            None => label,
        };
    }
    type_label(ty, ns)
}

/// The kind (`record`/`enum`/`fixed`) of a named type: from an inline definition, or
/// the resolved target of a reference.
fn named_kind(ty: &Value, ns: &str, defs: &Defs) -> Option<String> {
    if let Value::Object(obj) = ty
        && let Some(kind @ ("record" | "enum" | "fixed")) = obj.get("type").and_then(Value::as_str)
    {
        return Some(kind.to_owned());
    }
    let def = resolve(ty, ns, defs)?;
    Some(def.as_object()?.get("type")?.as_str()?.to_owned())
}
