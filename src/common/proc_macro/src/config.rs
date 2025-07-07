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

use bae::FromAttributes;
use proc_macro_error::{ResultExt, abort};
use proc_macro2::TokenStream;
use quote::quote;
use syn::DeriveInput;

#[derive(FromAttributes)]
pub struct OverrideOpts {
    /// The path to the field to override.
    pub path: syn::Expr,

    /// Whether to override the field only if it is absent in the config.
    ///
    /// This requires the field to be an `Option`.
    pub if_absent: Option<()>,
}

// TODO(bugen): the implementation is not robust but it works for now.
fn type_is_option(ty: &syn::Type) -> bool {
    if let syn::Type::Path(syn::TypePath { path, .. }) = ty
        && let Some(segment) = path.segments.last()
        && segment.ident == "Option"
    {
        true
    } else {
        false
    }
}

pub fn produce_override_config(input: DeriveInput) -> TokenStream {
    let syn::Data::Struct(syn::DataStruct { fields, .. }) = input.data else {
        abort!(input, "Only struct is supported");
    };

    let mut override_stmts = Vec::new();

    for field in fields {
        let field_type_is_option = type_is_option(&field.ty);
        let field_ident = field.ident;

        // Allow multiple `override_opts` attributes on a field.
        for attr in &field.attrs {
            let attributes = OverrideOpts::try_from_attributes(std::slice::from_ref(attr))
                .expect_or_abort("Failed to parse attribute");
            let Some(OverrideOpts { path, if_absent }) = attributes else {
                // Ignore attributes that are not `override_opts`.
                continue;
            };

            // Use `into` to support `Option` target fields.
            let mut override_stmt = if field_type_is_option {
                quote! {
                    if let Some(v) = self.#field_ident.clone() {
                        config.#path = v.into();
                    }
                }
            } else {
                quote! {
                    config.#path = self.#field_ident.clone().into();
                }
            };

            if if_absent.is_some() {
                override_stmt = quote! {
                    if config.#path.is_none() {
                        #override_stmt
                    }
                }
            }

            override_stmts.push(override_stmt);
        }
    }

    let struct_ident = input.ident;

    quote! {
        impl ::risingwave_common::config::OverrideConfig for #struct_ident {
            fn r#override(&self, config: &mut ::risingwave_common::config::RwConfig) {
                #(#override_stmts)*
            }
        }
    }
}
