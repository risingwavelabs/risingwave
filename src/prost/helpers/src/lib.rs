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

#![cfg_attr(coverage, feature(coverage_attribute))]
#![feature(iterator_try_collect)]

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{Data, DataStruct, DeriveInput, Result, parse_macro_input};

mod generate;

/// This attribute will be placed before any pb types, including messages and enums.
/// See `prost/helpers/README.md` for more details.
#[cfg_attr(coverage, coverage(off))]
#[proc_macro_derive(AnyPB)]
pub fn any_pb(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    match produce(&ast) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// Procedure macros can not be tested from the same crate.
#[cfg_attr(coverage, coverage(off))]
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

#[cfg_attr(coverage, coverage(off))]
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
