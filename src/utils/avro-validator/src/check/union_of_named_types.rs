use serde_json::Value;

use super::{Defs, Rule, is_named, resolve, type_label};

/// Unions whose variants include two or more named types (record/enum/fixed/ref).
/// Each is reported with its kind, resolving a reference to its definition to find
/// it. RisingWave rejects named types inside a union (issue 17632).
pub(super) struct UnionOfNamedTypes;

impl Rule for UnionOfNamedTypes {
    fn id(&self) -> &'static str {
        "union-of-named-types"
    }

    fn check(&self, node: &Value, ns: &str, defs: &Defs) -> Option<String> {
        let variants = node.as_array()?;
        let named: Vec<String> = variants
            .iter()
            .filter(|v| is_named(v))
            .map(|v| {
                let label = type_label(v, ns);
                match named_kind(v, ns, defs) {
                    Some(kind) => format!("{label} ({kind})"),
                    None => label,
                }
            })
            .collect();
        (named.len() >= 2).then(|| format!("{} named: [{}]", named.len(), named.join(", ")))
    }
}

/// The kind (`record`/`enum`/`fixed`) of a named variant: read straight from an inline
/// definition, or via the resolved target for a reference.
fn named_kind(variant: &Value, ns: &str, defs: &Defs) -> Option<String> {
    if let Value::Object(obj) = variant
        && let Some(kind @ ("record" | "enum" | "fixed")) = obj.get("type").and_then(Value::as_str)
    {
        return Some(kind.to_owned());
    }
    let def = resolve(variant, ns, defs)?;
    Some(def.as_object()?.get("type")?.as_str()?.to_owned())
}
