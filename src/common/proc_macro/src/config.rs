// Copyright 2023 Singularity Data
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
use proc_macro2::TokenStream;
use proc_macro_error::ResultExt;
use quote::quote;
use syn::DeriveInput;

#[derive(FromAttributes)]
pub struct Overwrite {
    pub path: Option<syn::Expr>,
    pub optional_in_config: Option<()>,
}

#[cfg_attr(coverage, no_coverage)]
pub fn produce_overwrite_config(input: DeriveInput) -> TokenStream {
    let struct_ident = input.ident;
    let mut overwrite_stmts = Vec::new();

    if let syn::Data::Struct(syn::DataStruct { fields, .. }) = input.data {
        for field in fields {
            let overwrite_opts = Overwrite::from_attributes(&field.attrs)
                .expect_or_abort("Failed to parse `overwrite` attribute");
            let path = overwrite_opts.path.expect("`path` must exist");
            let field_ident = field.ident;

            let overwrite_stmt = if overwrite_opts.optional_in_config.is_some() {
                quote! {
                    if self.#field_ident.is_some() {
                        config.#path = self.#field_ident;
                    }
                }
            } else {
                quote! {
                    if let Some(v) = self.#field_ident {
                        config.#path = v;
                    }
                }
            };

            overwrite_stmts.push(overwrite_stmt);
        }
    }

    quote! {
        impl risingwave_common::config::OverwriteConfig for #struct_ident {
            fn overwrite(self, config: &mut risingwave_common::config::RwConfig) {
                #(#overwrite_stmts)*
            }
        }
    }
}
