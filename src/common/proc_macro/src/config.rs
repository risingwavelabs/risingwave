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

// Copyright (c) 2022 Denis Kerp
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use bae::FromAttributes;
use proc_macro2::TokenStream;
use proc_macro_error::ResultExt;
use quote::quote;
use syn::DeriveInput;

#[derive(FromAttributes)]
pub struct OverrideOpts {
    pub path: Option<syn::Expr>,
    pub optional_in_config: Option<()>,
}

#[cfg_attr(coverage, no_coverage)]
pub fn produce_override_config(input: DeriveInput) -> TokenStream {
    let struct_ident = input.ident;
    let mut override_stmts = Vec::new();

    if let syn::Data::Struct(syn::DataStruct { fields, .. }) = input.data {
        for field in fields {
            let override_opts = OverrideOpts::from_attributes(&field.attrs)
                .expect_or_abort("Failed to parse `override_opts` attribute");
            let path = override_opts.path.expect("`path` must exist");
            let field_ident = field.ident;

            let override_stmt = if override_opts.optional_in_config.is_some() {
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

            override_stmts.push(override_stmt);
        }
    }

    quote! {
        impl risingwave_common::config::OverrideConfig for #struct_ident {
            fn r#override(self, config: &mut risingwave_common::config::RwConfig) {
                #(#override_stmts)*
            }
        }
    }
}
