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

#![cfg_attr(coverage, feature(no_coverage))]

use estimate_size::{
    add_trait_bounds, extract_ignored_generics_list, has_nested_flag_attribute_list,
};
use proc_macro::TokenStream;
use proc_macro_error::proc_macro_error;
use quote::quote;
use syn::parse_macro_input;

mod config;
mod estimate_size;

/// Sections in the configuration file can use `#[derive(OverrideConfig)]` to generate the
/// implementation of overwriting configs from the file.
///
/// In the struct definition, use #[override_opts(path = ...)] on a field to indicate the field in
/// `RwConfig` to override.
///
/// An example:
///
/// ```ignore
/// #[derive(OverrideConfig)]
/// struct Opts {
///     #[override_opts(path = meta.listen_addr)]
///     listen_addr: Option<String>,
/// }
/// ```
///
/// will generate
///
/// impl OverrideConfig for Opts {
///     fn r#override(self, config: &mut RwConfig) {
///         if let Some(v) = self.required_str {
///             config.meta.listen_addr = v;
///         }
///     }
/// }
/// ```
#[cfg_attr(coverage, no_coverage)]
#[proc_macro_derive(OverrideConfig, attributes(override_opts))]
#[proc_macro_error]
pub fn override_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input);

    let gen = config::produce_override_config(input);

    gen.into()
}

#[proc_macro_derive(EstimateSize)]
pub fn derive_estimate_size(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast: syn::DeriveInput = syn::parse(input).unwrap();

    // The name of the sruct.
    let name = &ast.ident;

    // Extract all generics we shall ignore.
    let ignored = extract_ignored_generics_list(&ast.attrs);

    // Add a bound `T: EstimateSize` to every type parameter T.
    let generics = add_trait_bounds(ast.generics, &ignored);

    // Extract the generics of the struct/enum.
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Traverse the parsed data to generate the individual parts of the function.
    match ast.data {
        syn::Data::Enum(data_enum) => {
            if data_enum.variants.is_empty() {
                // Empty enums are easy to implement.
                let gen = quote! {
                    impl EstimateSize for #name {
                        fn estimated_heap_size(&self) -> usize {
                            0
                        }
                    }
                };
                return gen.into();
            }

            let mut cmds = Vec::with_capacity(data_enum.variants.len());

            for variant in data_enum.variants.iter() {
                let ident = &variant.ident;

                match &variant.fields {
                    syn::Fields::Unnamed(unnamed_fields) => {
                        let num_fields = unnamed_fields.unnamed.len();

                        let mut field_idents = Vec::with_capacity(num_fields);
                        for i in 0..num_fields {
                            let field_ident = String::from("v") + &i.to_string();
                            let field_ident = syn::parse_str::<syn::Ident>(&field_ident).unwrap();

                            field_idents.push(field_ident);
                        }

                        let mut field_cmds = Vec::with_capacity(num_fields);

                        for (i, _field) in unnamed_fields.unnamed.iter().enumerate() {
                            let field_ident = String::from("v") + &i.to_string();
                            let field_ident = syn::parse_str::<syn::Ident>(&field_ident).unwrap();

                            field_cmds.push(quote! {
                                total += EstimateSize::estimated_heap_size(#field_ident);
                            })
                        }

                        cmds.push(quote! {
                            Self::#ident(#(#field_idents,)*) => {
                                let mut total = 0;

                                #(#field_cmds)*;

                                total
                            }
                        });
                    }
                    syn::Fields::Named(named_fields) => {
                        let num_fields = named_fields.named.len();

                        let mut field_idents = Vec::with_capacity(num_fields);

                        let mut field_cmds = Vec::with_capacity(num_fields);

                        for field in named_fields.named.iter() {
                            let field_ident = field.ident.as_ref().unwrap();

                            field_idents.push(field_ident);

                            field_cmds.push(quote! {
                                total += EstimateSize::estimated_heap_size(#field_ident);
                            })
                        }

                        cmds.push(quote! {
                            Self::#ident{#(#field_idents,)*} => {
                                let mut total = 0;

                                #(#field_cmds)*;

                                total
                            }
                        });
                    }
                    syn::Fields::Unit => {
                        cmds.push(quote! {
                            Self::#ident => 0,
                        });
                    }
                }
            }

            // Build the trait implementation
            let gen = quote! {
                impl #impl_generics EstimateSize for #name #ty_generics #where_clause {
                    fn estimated_heap_size(&self) -> usize {
                        match self {
                            #(#cmds)*
                        }
                    }
                }
            };
            gen.into()
        }
        syn::Data::Union(_data_union) => {
            panic!("Deriving EstimateSize for unions is currently not supported.")
        }
        syn::Data::Struct(data_struct) => {
            if data_struct.fields.is_empty() {
                // Empty structs are easy to implement.
                let gen = quote! {
                    impl EstimateSize for #name {}
                };
                return gen.into();
            }

            let mut cmds = Vec::with_capacity(data_struct.fields.len());

            for field in data_struct.fields.iter() {
                // Check if the value should be ignored. If so skip it.
                if has_nested_flag_attribute_list(&field.attrs, "estimate_size", "ignore") {
                    continue;
                }

                let ident = field.ident.as_ref().unwrap();

                cmds.push(quote! {
                    total += EstimateSize::estimated_heap_size(&self.#ident);
                })
            }

            // Build the trait implementation
            let gen = quote! {
                impl #impl_generics EstimateSize for #name #ty_generics #where_clause {
                    fn estimated_heap_size(&self) -> usize {
                        let mut total = 0;

                        #(#cmds)*;

                        total
                    }
                }
            };
            gen.into()
        }
    }
}
