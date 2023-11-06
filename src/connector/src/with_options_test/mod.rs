// Copyright 2023 RisingWave Labs
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
use std::path::PathBuf;
use std::{env, fs};

use serde::Serialize;
use syn::{parse_file, Attribute, Field, Item, ItemFn, Lit, Meta, MetaNameValue, NestedMeta, Type};

fn connector_crate_path() -> Option<PathBuf> {
    let mut current_dir = env::current_dir().ok()?;
    loop {
        if current_dir.join("Cargo.lock").exists() {
            let connector_path: PathBuf = current_dir.join("./src/connector");
            return Some(connector_path);
        }
        if !current_dir.pop() {
            break;
        }
    }
    None
}

pub fn update_with_options_yaml() -> String {
    let mut structs = vec![];
    let mut functions = BTreeMap::<String, FunctionInfo>::new();

    // Recursively list all the .rs files
    for entry in walkdir::WalkDir::new(connector_crate_path().unwrap().join("src")) {
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
                let serde_props = extract_serde_properties(&field);

                let field_type = field.ty;
                let mut required = match extract_type_name(&field_type).as_str() {
                    // Fields of type Option<T> or HashMap<K, V> are always considered optional.
                    "HashMap" | "Option" => false,
                    _ => true,
                };
                let field_type = quote::quote!(#field_type).to_string();
                let comments = extract_comments(&field.attrs);
                let alias = serde_props.alias;
                let rename = serde_props.rename;
                let default_func = serde_props.default_func;

                // Replace the function name with the function body.
                let mut default = default_func.clone();
                if let Some(default_func) = default_func {
                    if let Some(fn_info) = functions.get(&default_func) {
                        default = Some(fn_info.body.clone());
                    }
                    // If the field has a default value, it must be optional.
                    required = false;
                }

                let name = rename.unwrap_or_else(|| field_name.to_string()).to_string();

                // Assemble the information
                struct_info.fields.push(FieldInfo {
                    name,
                    field_type,
                    comments,
                    required,
                    default,
                    alias,
                });
            }
        }
        struct_infos.insert(struct_name, struct_info);
    }

    // Flatten the nested options.
    let struct_infos = flatten_nested_options(struct_infos);

    // Generate the output
    "# THIS FILE IS AUTO_GENERATED. DO NOT EDIT\n\n".to_string()
        + &serde_yaml::to_string(&struct_infos).unwrap()
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

    #[serde(skip_serializing_if = "Option::is_none")]
    alias: Option<String>,
}

#[derive(Default)]
struct SerdeProperties {
    default_func: Option<String>,
    rename: Option<String>,
    alias: Option<String>,
}

#[derive(Debug, Serialize, Default)]
struct StructInfo {
    fields: Vec<FieldInfo>,
}

#[derive(Debug)]
struct FunctionInfo {
    body: String,
}

fn has_with_options_attribute(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
            return meta_list.path.is_ident("derive")
                && meta_list.nested.iter().any(|nested| match nested {
                    syn::NestedMeta::Meta(Meta::Path(path)) => path.is_ident("WithOptions"),
                    _ => false,
                });
        }
        false
    })
}

fn extract_comments(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter_map(|attr| {
            if let Ok(Meta::NameValue(mnv)) = attr.parse_meta() {
                if mnv.path.is_ident("doc") {
                    if let syn::Lit::Str(lit_str) = mnv.lit {
                        return Some(lit_str.value());
                    }
                }
            }
            None
        })
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn extract_serde_properties(field: &Field) -> SerdeProperties {
    for attr in &field.attrs {
        if let Ok(meta) = attr.parse_meta() {
            if meta.path().is_ident("serde") {
                // Initialize the values to be extracted
                let mut serde_props = SerdeProperties::default();

                if let Meta::List(meta_list) = meta {
                    // Iterate over nested meta items (e.g., rename = "abc")
                    for nested_meta in meta_list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(MetaNameValue {
                            path, lit, ..
                        })) = nested_meta
                        {
                            if path.is_ident("rename") {
                                if let Lit::Str(lit_str) = lit {
                                    serde_props.rename = Some(lit_str.value());
                                }
                            } else if path.is_ident("alias") {
                                if let Lit::Str(lit_str) = lit {
                                    serde_props.alias = Some(lit_str.value());
                                }
                            } else if path.is_ident("default") {
                                if let Lit::Str(lit_str) = lit {
                                    serde_props.default_func = Some(lit_str.value());
                                }
                            }
                        } else if let NestedMeta::Meta(Meta::Path(path)) = nested_meta {
                            if path.is_ident("default") {
                                serde_props.default_func = Some("Default::default".to_string());
                            }
                        }
                    }
                }
                // Return the extracted values
                return serde_props;
            }
        }
    }
    SerdeProperties::default()
}

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
    if let Type::Path(typepath) = ty {
        if let Some(segment) = typepath.path.segments.last() {
            return segment.ident.to_string();
        }
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
        .to_string();

    (func.sig.ident.to_string(), FunctionInfo { body })
}
