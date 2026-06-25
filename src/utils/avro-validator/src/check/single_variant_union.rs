use serde_json::Value;

use super::{Defs, Rule, variant_labels};

/// Unions with exactly one variant (a single-element JSON array).
pub(super) struct SingleVariantUnion;

impl Rule for SingleVariantUnion {
    fn id(&self) -> &'static str {
        "single-variant-union"
    }

    fn check(&self, node: &Value, ns: &str, _defs: &Defs) -> Option<String> {
        let variants = node.as_array()?;
        (variants.len() == 1).then(|| format!("[{}]", variant_labels(variants, ns)))
    }
}
