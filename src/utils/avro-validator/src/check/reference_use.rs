use serde_json::Value;

use super::{Defs, Rule, resolve, type_label};

/// Every use of a named-type reference — a name string, or its object form
/// (`{"type":"Foo"}`) — reported with the kind of the definition it resolves to.
pub(super) struct ReferenceUse;

impl Rule for ReferenceUse {
    fn id(&self) -> &'static str {
        "reference-use"
    }

    fn check(&self, node: &Value, ns: &str, defs: &Defs) -> Option<String> {
        // `resolve` returns a definition only for a reference node (not a primitive
        // or an inline definition), so it doubles as the "is this a use?" gate.
        let def = resolve(node, ns, defs)?;
        let kind = def.get("type").and_then(Value::as_str)?;
        Some(format!("{} ({kind})", type_label(node, ns)))
    }
}
