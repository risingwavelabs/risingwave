// Copyright 2022 RisingWave Labs
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

#![cfg_attr(coverage, feature(coverage_attribute))]
#![feature(iterator_try_collect)]

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{Data, DataEnum, DataStruct, DeriveInput, Result, parse_macro_input};

mod generate;

/// This attribute will be placed before any pb types, including messages and enums.
/// See `prost/helpers/README.md` for more details.
#[proc_macro_derive(AnyPB)]
pub fn any_pb(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    match produce(&ast) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(StreamNodeBodyVariants)]
pub fn stream_node_body_variants(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    match produce_stream_node_body_variants(&ast) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn produce_stream_node_body_variants(ast: &DeriveInput) -> Result<TokenStream2> {
    if ast.ident != "NodeBody" {
        return Err(syn::Error::new_spanned(
            &ast.ident,
            "StreamNodeBodyVariants can only be derived for stream_plan::stream_node::NodeBody",
        ));
    }

    let Data::Enum(DataEnum { variants, .. }) = &ast.data else {
        return Err(syn::Error::new_spanned(
            ast,
            "StreamNodeBodyVariants can only be derived for enums",
        ));
    };

    let variants: Vec<_> = variants.iter().map(|variant| &variant.ident).collect();
    let marker_types: Vec<_> = variants
        .iter()
        .map(|variant| format_ident!("{variant}Variant"))
        .collect();
    let dollar = quote!($);

    Ok(quote! {
        #(
            #[derive(Debug, Clone, Copy)]
            pub struct #marker_types;
        )*

        #[macro_export]
        #[doc(hidden)]
        macro_rules! __dispatch_stream_node_body {
            (#dollar body:expr, #dollar node_body:ident, #dollar node:ident => #dollar call:expr) => {
                match #dollar body {
                    #(
                        ::risingwave_pb::stream_plan::stream_node::NodeBody::#variants(#dollar node) => {
                            type #dollar node_body = ::risingwave_pb::stream_plan::stream_node::#marker_types;
                            #dollar call
                        }
                    )*
                }
            };
        }
    })
}

// Procedure macros can not be tested from the same crate.
fn produce(ast: &DeriveInput) -> Result<TokenStream2> {
    let name = &ast.ident;

    // Is it a struct?
    let struct_get = if let syn::Data::Struct(DataStruct { ref fields, .. }) = ast.data {
        let generated: Vec<_> = fields.iter().map(generate::implement).try_collect()?;
        quote! {
            impl #name {
                #(#generated)*
            }
        }
    } else {
        // Do nothing.
        quote! {}
    };

    // Add a `Pb`-prefixed alias for all types.
    // No need to add docs for this alias as rust-analyzer will forward the docs to the original type.
    let pb_alias = {
        let pb_name = format_ident!("Pb{name}");
        quote! {
            pub type #pb_name = #name;
        }
    };

    Ok(quote! {
        #pb_alias
        #struct_get
    })
}

#[proc_macro_derive(Version)]
pub fn version(input: TokenStream) -> TokenStream {
    fn version_inner(ast: &DeriveInput) -> syn::Result<TokenStream2> {
        let last_variant = match &ast.data {
            Data::Enum(v) => v.variants.iter().next_back().ok_or_else(|| {
                syn::Error::new(
                    Span::call_site(),
                    "This macro requires at least one variant in the enum.",
                )
            })?,
            _ => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    "This macro only supports enums.",
                ));
            }
        };

        let enum_name = &ast.ident;
        let last_variant_name = &last_variant.ident;

        Ok(quote! {
            impl #enum_name {
                pub const LATEST: Self = Self::#last_variant_name;
            }
        })
    }

    let ast = parse_macro_input!(input as DeriveInput);

    match version_inner(&ast) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::parse_quote;

    use super::*;

    fn normalize(tokens: TokenStream2) -> String {
        tokens
            .to_string()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn stream_node_body_variants_generates_markers_and_dispatch() {
        let input = parse_quote! {
            enum NodeBody {
                Source(Box<SourceNode>),
                Project(Box<ProjectNode>),
            }
        };

        let dollar = quote!($);
        let expected_code = quote! {
            #[derive(Debug, Clone, Copy)]
            pub struct SourceVariant;

            #[derive(Debug, Clone, Copy)]
            pub struct ProjectVariant;

            #[macro_export]
            #[doc(hidden)]
            macro_rules! __dispatch_stream_node_body {
                (#dollar body:expr, #dollar node_body:ident, #dollar node:ident => #dollar call:expr) => {
                    match #dollar body {
                        ::risingwave_pb::stream_plan::stream_node::NodeBody::Source(#dollar node) => {
                            type #dollar node_body = ::risingwave_pb::stream_plan::stream_node::SourceVariant;
                            #dollar call
                        }
                        ::risingwave_pb::stream_plan::stream_node::NodeBody::Project(#dollar node) => {
                            type #dollar node_body = ::risingwave_pb::stream_plan::stream_node::ProjectVariant;
                            #dollar call
                        }
                    }
                };
            }
        };

        assert_eq!(
            normalize(produce_stream_node_body_variants(&input).unwrap()),
            normalize(expected_code)
        );
    }

    #[test]
    fn stream_node_body_variants_rejects_unexpected_enum_name() {
        let input = parse_quote! {
            enum OtherBody {
                Source(Box<SourceNode>),
            }
        };

        let err = produce_stream_node_body_variants(&input).unwrap_err();
        assert_eq!(
            err.to_string(),
            "StreamNodeBodyVariants can only be derived for stream_plan::stream_node::NodeBody"
        );
    }
}
