// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::{env, fs};

use itertools::Itertools;
use proc_macro2::TokenTree;
use quote::ToTokens;
use serde::Serialize;
use syn::{Attribute, Field, Item, ItemFn, Lit, LitStr, Meta, Type, parse_file};
use thiserror_ext::AsReport;
use walkdir::{DirEntry, WalkDir};

fn connector_crate_path() -> PathBuf {
    let connector_crate_path = env::var("CARGO_MANIFEST_DIR").unwrap();
    Path::new(&connector_crate_path).to_path_buf()
}

fn common_files() -> impl IntoIterator<Item = walkdir::Result<DirEntry>> {
    WalkDir::new(
        connector_crate_path()
            .join("src")
            .join("connector_common")
            .join("common.rs"),
    )
    .into_iter()
    .chain(WalkDir::new(
        connector_crate_path()
            .join("src")
            .join("connector_common")
            .join("mqtt_common.rs"),
    ))
    .chain(WalkDir::new(
        connector_crate_path()
            .join("src")
            .join("connector_common")
            .join("iceberg")
            .join("mod.rs"),
    ))
}

pub fn generate_with_options_yaml_source() -> String {
    generate_with_options_yaml_inner(&connector_crate_path().join("src").join("source"))
}

pub fn generate_with_options_yaml_sink() -> String {
    generate_with_options_yaml_inner(&connector_crate_path().join("src").join("sink"))
}

/// Collect all structs with `#[derive(WithOptions)]` in the `.rs` files in `path` (plus `common.rs`),
/// and generate a YAML file.
///
/// Note: here we assumes the struct is parsed by `serde`. If it's not the case,
/// the generated `yaml` might be inconsistent with the actual parsing logic.
/// TODO: improve the test to check whether serde is used.
///
/// - For sources, the parsing logic is in `TryFromBTreeMap`.
/// - For sinks, the parsing logic is in `TryFrom<SinkParam>`.
fn generate_with_options_yaml_inner(path: &Path) -> String {
    let mut structs = vec![];
    let mut functions = BTreeMap::<String, FunctionInfo>::new();

    // Recursively list all the .rs files
    for entry in walkdir::WalkDir::new(path)
        .into_iter()
        .chain(common_files())
    {
        let entry = entry.expect("Failed to read directory entry");
        if entry.path().extension() == Some("rs".as_ref()) {
            // Parse the content of the .rs file
            let content = fs::read_to_string(entry.path()).expect("Failed to read file");
            let file = parse_file(&content).expect("Failed to parse file");

            // Process each item in the file
            for item in file.items {
                if let Item::Struct(struct_item) = item {
                    // Check if the struct has the #[with_options] attribute
                    if has_with_options_attribute(&struct_item.attrs) {
                        structs.push(struct_item);
                    }
                } else if let Item::Fn(func_item) = item {
                    let (func_name, func_body) = extract_function_body(func_item);
                    functions.insert(func_name, func_body);
                }
            }
        }
    }

    let mut struct_infos: BTreeMap<String, StructInfo> = BTreeMap::new();

    // Process each struct
    for struct_item in structs {
        let struct_name = struct_item.ident.to_string();

        let mut struct_info = StructInfo::default();
        for field in struct_item.fields {
            // Process each field
            if let Some(field_name) = &field.ident {
                if field_name == "unknown_fields" {
                    continue;
                }

                let SerdeProperties {
                    default_func,
                    rename,
                    alias,
                } = extract_serde_properties(&field);

                let field_type = field.ty;
                let mut required = match extract_type_name(&field_type).as_str() {
                    // Fields of type Option<T> or HashMap<K, V> are always considered optional.
                    "HashMap" | "Option" => false,
                    _ => true,
                };
                let mut field_type = quote::quote!(#field_type)
                    .to_token_stream()
                    .into_iter()
                    .join("");
                // Option<T> -> T
                if field_type.starts_with("Option") {
                    field_type = field_type[7..field_type.len() - 1].to_string();
                }
                let comments = extract_comments(&field.attrs);

                // Replace the function name with the function body.
                let mut default = default_func.clone();
                if let Some(default_func) = default_func {
                    if let Some(fn_info) = functions.get(&default_func) {
                        default = Some(fn_info.body.clone());
                    }
                    // If the field has a default value, it must be optional.
                    required = false;
                }

                let name = rename.unwrap_or_else(|| field_name.to_string());

                // Assemble the information
                struct_info.fields.push(FieldInfo {
                    name,
                    field_type,
                    comments,
                    required,
                    default,
                    alias,
                });
            } else {
                panic!("Unexpected tuple struct: {}", struct_name);
            }
        }
        if struct_infos
            .insert(struct_name.clone(), struct_info)
            .is_some()
        {
            panic!("Duplicate struct: {}", struct_name);
        };
    }

    // Flatten the nested options.
    let struct_infos = flatten_nested_options(struct_infos);

    // Generate the output
    format!(
        "# THIS FILE IS AUTO_GENERATED. DO NOT EDIT\n# UPDATE WITH: ./risedev generate-with-options\n\n{}",
        serde_yaml::to_string(&struct_infos).unwrap()
    )
}

#[derive(Debug, Serialize, Clone)]
struct FieldInfo {
    // If specified with serde(rename), use the renamed name instead of the Rust name.
    name: String,

    /// For `Option<T>`, it'll be the T.
    field_type: String,

    #[serde(skip_serializing_if = "String::is_empty")]
    comments: String,

    required: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    alias: Vec<String>,
}

#[derive(Default)]
struct SerdeProperties {
    default_func: Option<String>,
    rename: Option<String>,
    alias: Vec<String>,
}

#[derive(Debug, Serialize, Default)]
struct StructInfo {
    fields: Vec<FieldInfo>,
}

#[derive(Debug)]
struct FunctionInfo {
    body: String,
}

/// Has `#[derive(WithOptions)]`
fn has_with_options_attribute(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if let Meta::List(meta_list) = &attr.meta {
            return meta_list.path.is_ident("derive")
                && meta_list.tokens.clone().into_iter().any(|token| {
                    if let TokenTree::Ident(ident) = token {
                        ident == "WithOptions"
                    } else {
                        false
                    }
                });
        }
        false
    })
}

fn extract_comments(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter_map(|attr| {
            if let Meta::NameValue(mnv) = &attr.meta
                && mnv.path.is_ident("doc")
                && let syn::Expr::Lit(syn::ExprLit {
                    lit: Lit::Str(lit_str),
                    ..
                }) = &mnv.value
            {
                return Some(lit_str.value().trim().to_owned());
            }
            None
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

fn extract_serde_properties(field: &Field) -> SerdeProperties {
    for attr in &field.attrs {
        if let Meta::List(meta_list) = &attr.meta
            && meta_list.path.is_ident("serde")
        {
            // Initialize the values to be extracted
            let mut serde_props = SerdeProperties::default();

            // Iterate over nested meta items (e.g., rename = "abc")
            meta_list
                .parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename") {
                        if let Ok(value) = meta.value()?.parse::<LitStr>() {
                            serde_props.rename = Some(value.value());
                        }
                    } else if meta.path.is_ident("alias") {
                        if let Ok(value) = meta.value()?.parse::<LitStr>() {
                            serde_props.alias.push(value.value());
                        }
                    } else if meta.path.is_ident("default") {
                        if let Ok(value) = meta.value().and_then(|v| v.parse::<LitStr>()) {
                            serde_props.default_func = Some(value.value());
                        } else {
                            serde_props.default_func = Some("Default::default".to_owned());
                        }
                    }
                    // drain the remaining meta. Otherwise parse_nested_meta returns err
                    // <https://github.com/dtolnay/syn/issues/1426>
                    _ = meta.value();
                    _ = meta.input.parse::<LitStr>();
                    Ok(())
                })
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to parse serde properties for field: {:?}, err: {}",
                        field.ident,
                        err.to_report_string(),
                    )
                });

            // Return the extracted values
            return serde_props;
        }
    }
    SerdeProperties::default()
}

/// Flatten the nested options, e.g.,
/// ```ignore
/// pub struct KafkaConfig {
///     #[serde(flatten)]
///     pub common: KafkaCommon,
///     #[serde(flatten)]
///     pub rdkafka_properties: RdKafkaPropertiesProducer,
///     // ...
/// }
/// ```
///
/// Note: here we assumes `#[serde(flatten)]` is used for struct fields. If it's not the case,
/// the generated `yaml` might be inconsistent with the actual parsing logic.
fn flatten_nested_options(options: BTreeMap<String, StructInfo>) -> BTreeMap<String, StructInfo> {
    let mut deleted_keys = HashSet::new();

    let mut new_options: BTreeMap<String, StructInfo> = options
        .iter()
        .map(|(name, struct_info)| {
            (
                name.clone(),
                StructInfo {
                    fields: flatten_struct(struct_info, &options, &mut deleted_keys),
                },
            )
        })
        .collect();

    for key in deleted_keys {
        new_options.remove(&key);
    }

    new_options
}

// Get all fields recursively from a WithOptions struct.
fn flatten_struct(
    struct_info: &StructInfo,
    options: &BTreeMap<String, StructInfo>,
    deleted_keys: &mut HashSet<String>,
) -> Vec<FieldInfo> {
    let mut fields = Vec::new();
    for field in &struct_info.fields {
        if let Some(nested_struct_info) = options.get(&field.field_type) {
            fields.append(&mut flatten_struct(
                nested_struct_info,
                options,
                deleted_keys,
            ));
            deleted_keys.insert(field.field_type.clone());
        } else {
            fields.push(field.clone());
        }
    }
    fields
}

// If the type is Option<T>, return Option.
// For HashMap<K, V>, return HashMap.
fn extract_type_name(ty: &Type) -> String {
    if let Type::Path(typepath) = ty
        && let Some(segment) = typepath.path.segments.last()
    {
        return segment.ident.to_string();
    }
    panic!("Failed to extract type name: {}", quote::quote!(#ty));
}

/// Extract the return expression from the body of a single-expression function,
/// like `123` from `fn default_func() { 123 }`
/// or `u64::MAX` from `fn default_func() -> u64 { u64::MAX }`
fn extract_function_body(func: ItemFn) -> (String, FunctionInfo) {
    // The function body is a Block, which contains a vector of Stmts (statements)
    let body = func.block;
    let body = quote::quote!(#body)
        .to_string()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim()
        .to_owned();

    (func.sig.ident.to_string(), FunctionInfo { body })
}
