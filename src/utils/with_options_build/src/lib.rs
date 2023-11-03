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
use std::io::Write;
use std::path::PathBuf;
use std::{env, fs};

use serde::Serialize;
use syn::{parse_file, Attribute, Expr, Field, Item, LitStr, Meta};

fn find_project_root() -> Option<PathBuf> {
    let mut current_dir = env::current_dir().ok()?;
    loop {
        if current_dir.join("Cargo.lock").exists() {
            return Some(current_dir);
        }
        if !current_dir.pop() {
            break;
        }
    }
    None
}

pub fn update_with_options_yaml() {
    let connector_path = find_project_root().unwrap().join("./src/connector");

    let mut structs = vec![];

    // Step 1: Recursively list all the .rs files
    for entry in walkdir::WalkDir::new(connector_path.join("src")) {
        let entry = entry.expect("Failed to read directory entry");
        if entry.path().extension() == Some("rs".as_ref()) {
            // Step 2: Parse the content of the .rs file
            let content = fs::read_to_string(entry.path()).expect("Failed to read file");
            let file = parse_file(&content).expect("Failed to parse file");

            // Step 3: Process each item in the file
            for item in file.items {
                if let Item::Struct(struct_item) = item {
                    // Check if the struct has the #[with_options] attribute
                    if has_with_options_attribute(&struct_item.attrs) {
                        structs.push(struct_item);
                    }
                }
            }
        }
    }

    let mut struct_infos: BTreeMap<String, StructInfo> = BTreeMap::new();

    // Step 4: Process each struct
    for struct_item in structs {
        let struct_name = struct_item.ident.to_string();

        let mut struct_info = StructInfo::default();
        for field in struct_item.fields {
            // Step 5: Process each field
            if let Some(field_name) = &field.ident {
                // If specified with serde(rename), use the renamed name instead of the Rust name.
                let field_name =
                    extract_serde_rename_value(&field).unwrap_or_else(|| field_name.to_string());

                let field_type = &field.ty;
                let comments = extract_comments(&field.attrs);
                let required = has_required_attribute(&field.attrs);
                let default = extract_default_attribute(&field.attrs);

                // Step 6: Assemble the information
                struct_info.fields.push(FieldInfo {
                    name: field_name.to_string(),
                    field_type: quote::quote!(#field_type).to_string(),
                    comments,
                    required,
                    default,
                });
            }
        }
        struct_infos.insert(struct_name, struct_info);
    }

    // Step 7: Flatten the nested options.
    let struct_infos = flatten_nested_options(struct_infos);
    for (key, _) in struct_infos.iter() {
        println!("Struct {}", key);
    }

    // Step 8: Generate the output
    let yaml_str = serde_yaml::to_string(&struct_infos).unwrap();
    let mut file = std::fs::File::create(connector_path.join("with_options.yaml")).unwrap();
    file.write_all(yaml_str.as_bytes()).unwrap();
}

#[derive(Debug, Serialize, Clone)]
struct FieldInfo {
    name: String,
    field_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    comments: String,
    required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<String>,
}

#[derive(Debug, Serialize, Default)]
struct StructInfo {
    fields: Vec<FieldInfo>,
}

fn has_with_options_attribute(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if attr.meta.path().is_ident("derive") {
            if let Meta::List(meta_list) = &attr.meta {
                let mut has_with_options = false;
                let _ = meta_list.parse_nested_meta(|nested| {
                    if nested.path.is_ident("WithOptions") {
                        has_with_options = true;
                        return Ok(());
                    }
                    Ok(())
                });
                return has_with_options;
            }
        }
        false
    })
}

fn extract_comments(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter_map(|attr| {
            if let Meta::NameValue(mnv) = &attr.meta {
                if mnv.path.is_ident("doc") {
                    if let Expr::Lit(lit) = &mnv.value {
                        if let syn::Lit::Str(lit_str) = &lit.lit {
                            return Some(lit_str.value());
                        }
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

fn extract_serde_rename_value(field: &Field) -> Option<String> {
    for attr in &field.attrs {
        if let Meta::List(meta_list) = &attr.meta {
            if meta_list.path.is_ident("serde") {
                let mut rename = None;
                let _ = meta_list.parse_nested_meta(|nested| {
                    if nested.path.is_ident("rename") {
                        let value = nested.value()?;
                        let lit_str: LitStr = value.parse()?;
                        rename = Some(lit_str.value().to_string());
                    }
                    Ok(())
                });
                return rename;
            }
        }
    }
    None
}

fn has_required_attribute(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if let Meta::List(meta_list) = &attr.meta {
            if meta_list.path.is_ident("with_option") {
                let mut required = false;
                let _ = meta_list.parse_nested_meta(|nested| {
                    if nested.path.is_ident("required") {
                        required = true;
                    }
                    Ok(())
                });
                return required;
            }
        }
        false
    })
}

fn extract_default_attribute(attrs: &[Attribute]) -> Option<String> {
    attrs.iter().find_map(|attr| {
        if let Meta::List(meta_list) = &attr.meta {
            if meta_list.path.is_ident("with_option") {
                let mut default: Option<String> = None;
                let _ = meta_list.parse_nested_meta(|nested| {
                    if nested.path.is_ident("default") {
                        let value = nested.value()?;
                        let lit_str: LitStr = value.parse()?;
                        default = Some(lit_str.value().to_string());
                    }
                    Ok(())
                });
                return default;
            }
        }
        None
    })
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
    for field in struct_info.fields.iter() {
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
