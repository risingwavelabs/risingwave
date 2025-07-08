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

use itertools::Itertools;
use quote::quote;
use syn::{Attribute, Data, DataStruct, DeriveInput, Field, Fields};

pub fn generate_config_doc_fn(input: DeriveInput) -> proc_macro2::TokenStream {
    let mut doc = StructFieldDocs::new();

    let struct_name = input.ident;
    match input.data {
        Data::Struct(ref data) => doc.extract_field_docs(data),
        _ => panic!("This macro only supports structs"),
    };

    let vec_fields = doc.token_vec_fields();
    let call_nested_fields = doc.token_call_nested_fields();
    quote! {
        impl #struct_name {
            pub fn config_docs(name: String, docs: &mut std::collections::BTreeMap<String, Vec<(String, String)>>) {
                docs.insert(name.clone(), #vec_fields);
                #call_nested_fields;
            }
        }
    }
}

fn extract_comment(attrs: &Vec<Attribute>) -> String {
    attrs
        .iter()
        .filter_map(|attr| {
            if let Ok(meta) = attr.parse_meta()
                && meta.path().is_ident("doc")
                && let syn::Meta::NameValue(syn::MetaNameValue {
                    lit: syn::Lit::Str(comment),
                    ..
                }) = meta
            {
                Some(comment.value())
            } else {
                None
            }
        })
        .filter_map(|comment| {
            let trimmed = comment.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        })
        .join(" ")
}

fn is_nested_config_field(field: &Field) -> bool {
    field.attrs.iter().any(|attr| {
        if let Some(attr_name) = attr.path.get_ident() {
            attr_name == "config_doc" && attr.tokens.to_string() == "(nested)"
        } else {
            false
        }
    })
}

fn is_omitted_config_field(field: &Field) -> bool {
    field.attrs.iter().any(|attr| {
        if let Some(attr_name) = attr.path.get_ident() {
            attr_name == "config_doc" && attr.tokens.to_string() == "(omitted)"
        } else {
            false
        }
    })
}

fn field_name(f: &Field) -> String {
    f.ident
        .as_ref()
        .expect("field name should not be empty")
        .to_string()
}

struct StructFieldDocs {
    // Fields that require recursively retrieving their field docs.
    nested_fields: Vec<(String, syn::Type)>,

    fields: Vec<(String, String)>,
}

impl StructFieldDocs {
    fn new() -> Self {
        Self {
            nested_fields: vec![],
            fields: vec![],
        }
    }

    fn extract_field_docs(&mut self, data: &DataStruct) {
        match &data.fields {
            Fields::Named(fields) => {
                self.fields = fields
                    .named
                    .iter()
                    .filter_map(|field| {
                        if is_omitted_config_field(field) {
                            return None;
                        }
                        if is_nested_config_field(field) {
                            self.nested_fields
                                .push((field_name(field), field.ty.clone()));
                            return None;
                        }
                        let field_name = field.ident.as_ref()?.to_string();
                        let rustdoc = extract_comment(&field.attrs);
                        Some((field_name, rustdoc))
                    })
                    .collect_vec();
            }
            _ => unreachable!("field should be named"),
        }
    }

    fn token_vec_fields(&self) -> proc_macro2::TokenStream {
        let token_fields: Vec<proc_macro2::TokenStream> = self
            .fields
            .iter()
            .map(|(name, doc)| {
                quote! { (#name.to_string(), #doc.to_string()) }
            })
            .collect();

        quote! {
            vec![#(#token_fields),*]
        }
    }

    fn token_call_nested_fields(&self) -> proc_macro2::TokenStream {
        let tokens: Vec<proc_macro2::TokenStream> = self
            .nested_fields
            .iter()
            .map(|(ident, ty)| {
                quote! {
                    if name.is_empty() {
                        #ty::config_docs(#ident.to_string(), docs);
                    } else {
                        #ty::config_docs(format!("{}.{}", name, #ident), docs);
                    }
                }
            })
            .collect();
        quote! { #(#tokens)* }
    }
}
