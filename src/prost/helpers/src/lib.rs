// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg_attr(coverage, feature(no_coverage))]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro_error::{proc_macro_error, ResultExt};
use quote::quote;
use syn::{DataStruct, DeriveInput};

mod generate;

/// This attribute will be placed before any pb types, including messages and enums.
/// See `prost/helpers/README.md` for more details.
#[cfg_attr(coverage, no_coverage)]
#[proc_macro_derive(AnyPB)]
#[proc_macro_error]
pub fn any_pb(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast: DeriveInput = syn::parse(input).expect_or_abort("Couldn't parse for getters");

    // Build the impl
    let gen = produce(&ast);

    // Return the generated impl
    gen.into()
}

// Procedure macros can not be tested from the same crate.
#[cfg_attr(coverage, no_coverage)]
fn produce(ast: &DeriveInput) -> TokenStream2 {
    let name = &ast.ident;

    // Is it a struct?
    if let syn::Data::Struct(DataStruct { ref fields, .. }) = ast.data {
        let generated = fields.iter().map(generate::implement);
        quote! {
            impl #name {
                #(#generated)*
            }
        }
    } else {
        // Do nothing.
        quote! {}
    }
}
