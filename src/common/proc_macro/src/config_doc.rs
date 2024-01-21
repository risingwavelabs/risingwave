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
            pub fn config_docs(name: String, docs: &mut std::collections::HashMap<String, Vec<(String, String)>>) {
                docs.insert(name, #vec_fields);
                #call_nested_fields;
            }
        }
    }
}

fn extract_comment(attrs: &Vec<Attribute>) -> String {
    attrs
        .iter()
        .filter_map(|attr| {
            if let Ok(meta) = attr.parse_meta() {
                if meta.path().is_ident("doc") {
                    if let syn::Meta::NameValue(syn::MetaNameValue {
                        lit: syn::Lit::Str(comment),
                        ..
                    }) = meta
                    {
                        return Some(comment.value());
                    }
                }
            }
            None
        })
        .join("\n")
}

fn is_nested_config_field(field: &Field) -> bool {
    field
        .attrs
        .iter()
        .any(|attr| attr.path.is_ident("nested_config"))
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
                quote! { (String::from(#name), String::from(#doc)) }
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
                        #ty::config_docs(#ident, docs);
                    } else {
                        #ty::config_docs(format!("{}.{}", name, #ident), docs);
                    }
                }
            })
            .collect();
        quote! { #(#tokens)* }
    }
}
