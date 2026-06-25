//! A small visitor framework for running structural rules over an Avro schema's
//! raw JSON ([`serde_json::Value`]).
//!
//! Walking the JSON as written — rather than apache_avro's parsed `Schema` — lets
//! rules see what the parser hides: the physical type beneath a logical type, and
//! unknown attributes.
//!
//! [`run`] walks every schema node depth-first and offers each to every [`Rule`],
//! threading the in-scope namespace and a table of named definitions so a rule can
//! resolve any reference it meets. A rule returns `Some(detail)` for a node it
//! matches, producing a [`Finding`] tagged with the node's path. Each concrete rule
//! lives in its own submodule; to add one, implement [`Rule`] there and list it in
//! [`default_rules`].

mod aliases;
mod field_default;
mod logical_type;
mod reference_use;
mod single_variant_union;
mod union_of_named_types;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;

/// Named-type definitions (record/enum/fixed) indexed by fullname, for resolving
/// references a rule encounters.
type Defs<'a> = HashMap<String, &'a Value>;

/// One rule match, located within the schema.
pub struct Finding {
    /// The matching rule's [`Rule::id`].
    pub rule: &'static str,
    /// Dotted path to the node, e.g. `User.address` or `payload.|1` (union variant 1).
    pub path: String,
    /// The rule's detail message for this match.
    pub detail: String,
}

/// A structural check applied to each schema node (a [`Value`] holding an Avro type).
pub trait Rule {
    /// Stable identifier shown in output (e.g. `wide-union`).
    fn id(&self) -> &'static str;
    /// Inspect a single node; return a detail message when it matches. `defs` resolves
    /// any named reference the node carries to its definition.
    fn check(&self, node: &Value, ns: &str, defs: &Defs) -> Option<String>;
}

/// The rules run by default. Extend this to add a check.
pub fn default_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(single_variant_union::SingleVariantUnion),
        Box::new(logical_type::LogicalType),
        Box::new(union_of_named_types::UnionOfNamedTypes),
        Box::new(aliases::Aliases),
        Box::new(field_default::FieldDefault),
        Box::new(reference_use::ReferenceUse),
    ]
}

/// Walk `schema` with the default rule set.
pub fn run_default(schema: &Value) -> Vec<Finding> {
    run(schema, &default_rules())
}

/// The ids of every check, in run order — used to group output by rule.
pub fn rule_ids() -> Vec<&'static str> {
    let mut ids: Vec<&'static str> = default_rules().iter().map(|r| r.id()).collect();
    ids.push(NAMESPACE_SIGNIFICANT);
    ids
}

/// Walk `schema` depth-first, collecting every rule match.
pub fn run(schema: &Value, rules: &[Box<dyn Rule>]) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut defs = Defs::new();
    visit(
        schema,
        &mut String::new(),
        "",
        &mut defs,
        rules,
        &mut findings,
    );
    // The per-node rules above are local; namespace significance is a whole-schema
    // property, so check it once over the now-complete definition set.
    namespace_collisions(&defs, &mut findings);
    findings
}

/// Rule id for the whole-schema namespace check below.
const NAMESPACE_SIGNIFICANT: &str = "namespace-significant";

/// Flag any simple name defined under more than one namespace — once namespaces are
/// dropped such a name is ambiguous, so the namespace is load-bearing. A whole-schema
/// property, so it scans the complete definition set in one pass.
fn namespace_collisions(defs: &Defs, out: &mut Vec<Finding>) {
    let mut by_name: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for fullname in defs.keys() {
        let (namespace, simple) = match fullname.rsplit_once('.') {
            Some(parts) => parts,
            None => ("", fullname.as_str()),
        };
        by_name.entry(simple).or_default().insert(namespace);
    }
    for (simple, namespaces) in by_name {
        if namespaces.len() < 2 {
            continue;
        }
        let list = namespaces
            .iter()
            .map(|ns| if ns.is_empty() { "(none)" } else { ns })
            .collect::<Vec<_>>()
            .join(", ");
        out.push(Finding {
            rule: NAMESPACE_SIGNIFICANT,
            path: "<root>".to_owned(),
            detail: format!("{simple}: {list}"),
        });
    }
}

fn visit<'a>(
    node: &'a Value,
    path: &mut String,
    ns: &'a str,
    defs: &mut Defs<'a>,
    rules: &[Box<dyn Rule>],
    out: &mut Vec<Finding>,
) {
    // Index a named definition on the way in, so it is available to resolve a
    // reference anywhere in its own subtree (e.g. recursive `record Node { next: Node }`).
    if let Value::Object(obj) = node
        && let Some("record" | "enum" | "fixed") = obj.get("type").and_then(Value::as_str)
    {
        let name = obj.get("name").and_then(Value::as_str).unwrap_or("?");
        defs.insert(
            fullname(name, obj.get("namespace").and_then(Value::as_str), ns),
            node,
        );
    }

    // Recurse following Avro's JSON shape. A schema is a string (primitive name or
    // named reference — both leaves), an array (union), or an object (a complex type
    // keyed by `"type"`). The walk does not follow references — rules resolve them
    // via the definitions table — which keeps it finite on recursive schemas.
    match node {
        Value::Array(variants) => {
            for (i, variant) in variants.iter().enumerate() {
                descend(path, &format!("|{i}"), |p| {
                    visit(variant, p, ns, defs, rules, out)
                });
            }
        }
        Value::Object(obj) => match obj.get("type").and_then(Value::as_str) {
            Some("record") => {
                // Nested named types inherit this record's namespace: a dotted name
                // sets it, else an explicit `namespace`, else the enclosing one.
                let child_ns = obj
                    .get("name")
                    .and_then(Value::as_str)
                    .and_then(|n| n.rfind('.').map(|i| &n[..i]))
                    .or_else(|| obj.get("namespace").and_then(Value::as_str))
                    .unwrap_or(ns);
                for field in obj
                    .get("fields")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let name = field.get("name").and_then(Value::as_str).unwrap_or("?");
                    if let Some(field_type) = field.get("type") {
                        descend(path, name, |p| {
                            visit(field_type, p, child_ns, defs, rules, out)
                        });
                    }
                }
            }
            Some("array") => {
                if let Some(items) = obj.get("items") {
                    descend(path, "[]", |p| visit(items, p, ns, defs, rules, out));
                }
            }
            Some("map") => {
                if let Some(values) = obj.get("values") {
                    descend(path, "{}", |p| visit(values, p, ns, defs, rules, out));
                }
            }
            // Primitive (incl. logical types), enum, fixed, or a reference — leaf.
            _ => {}
        },
        _ => {}
    }

    // Run rules after recursing, so a rule on this node sees its whole subtree already
    // indexed — letting it resolve a reference to a type defined in, say, a sibling
    // field, which is only reached during the recursion above.
    for rule in rules {
        if let Some(detail) = rule.check(node, ns, defs) {
            out.push(Finding {
                rule: rule.id(),
                path: if path.is_empty() {
                    "<root>".to_owned()
                } else {
                    path.clone()
                },
                detail,
            });
        }
    }
}

/// Append `segment` to `path`, run `f`, then restore `path`.
fn descend(path: &mut String, segment: &str, f: impl FnOnce(&mut String)) {
    let restore_to = path.len();
    if !path.is_empty() {
        path.push('.');
    }
    path.push_str(segment);
    f(path);
    path.truncate(restore_to);
}

// ---------------------------------------------------------------------------
// Schema-node helpers (shared by the rule submodules)
// ---------------------------------------------------------------------------

/// Whether `name` is one of the eight Avro primitive type names.
fn is_primitive_name(name: &str) -> bool {
    matches!(
        name,
        "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string"
    )
}

/// Whether a union variant is a named type: a record/enum/fixed definition, or a
/// reference to one — a non-primitive name, as a bare string or in object form
/// (`{"type":"Foo"}`).
fn is_named(variant: &Value) -> bool {
    match variant {
        Value::String(name) => !is_primitive_name(name),
        Value::Object(obj) => match obj.get("type").and_then(Value::as_str) {
            Some("record" | "enum" | "fixed") => true,
            // Object form of a reference (`{"type":"Foo"}` ≡ `"Foo"`): any name that
            // isn't a primitive or the array/map keyword references a named type.
            Some(other) => !is_primitive_name(other) && !matches!(other, "array" | "map"),
            None => false,
        },
        _ => false,
    }
}

/// A short label identifying a union variant the way Avro does: a named type
/// (record/enum/fixed) by its fullname — the spec distinguishes such variants by
/// name, allowing several in one union — and everything else, including logical
/// types, by its underlying Avro type (a `long` with `logicalType` is still the
/// union's `long`).
fn type_label(variant: &Value, ns: &str) -> String {
    match variant {
        Value::String(name) if is_primitive_name(name) => name.to_owned(),
        // A non-primitive string is a named-type reference.
        Value::String(name) => fullname(name, None, ns),
        Value::Object(obj) => match obj.get("type").and_then(Value::as_str) {
            Some("record" | "enum" | "fixed") => fullname(
                obj.get("name").and_then(Value::as_str).unwrap_or("?"),
                obj.get("namespace").and_then(Value::as_str),
                ns,
            ),
            // Object form of a reference (`{"type":"Foo"}` ≡ `"Foo"`).
            Some(name) if !is_primitive_name(name) && !matches!(name, "array" | "map") => {
                fullname(name, None, ns)
            }
            Some(underlying) => underlying.to_owned(),
            None => "?".to_owned(),
        },
        // A bare-array variant is a nested union and a JSON scalar is no schema at all;
        // neither is valid Avro, so flag it rather than invent a spec-looking name.
        _ => "?".to_owned(),
    }
}

/// An Avro fullname: a dotted `name` already is one; otherwise the explicit or
/// inherited namespace, when non-empty, qualifies it.
fn fullname(name: &str, explicit_ns: Option<&str>, inherited_ns: &str) -> String {
    if name.contains('.') {
        return name.to_owned();
    }
    match explicit_ns.unwrap_or(inherited_ns) {
        "" => name.to_owned(),
        ns => format!("{ns}.{name}"),
    }
}

/// Resolve a reference variant — a name string, or its object form `{"type":"Foo"}` —
/// to the referenced definition, qualifying an unqualified name with `ns`. Returns
/// `None` for anything that is not a reference (a primitive, or an inline definition).
fn resolve<'a>(reference: &Value, ns: &str, defs: &Defs<'a>) -> Option<&'a Value> {
    let name = match reference {
        Value::String(name) if !is_primitive_name(name) => name.as_str(),
        Value::Object(obj) => match obj.get("type").and_then(Value::as_str) {
            Some(name)
                if !is_primitive_name(name)
                    && !matches!(name, "record" | "enum" | "fixed" | "array" | "map") =>
            {
                name
            }
            _ => return None,
        },
        _ => return None,
    };
    defs.get(&fullname(name, None, ns)).copied()
}

fn variant_labels(variants: &[Value], ns: &str) -> String {
    variants
        .iter()
        .map(|v| type_label(v, ns))
        .collect::<Vec<_>>()
        .join(", ")
}
