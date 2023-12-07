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

use proc_macro2::TokenStream;
use quote::quote;
use syn::Data;
use syn::DeriveInput;
use syn::Field;
use syn::Result;

#[proc_macro_derive(Fields)]
pub fn fields(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = syn::parse_macro_input! {tokens};

    match gen(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn gen(input: DeriveInput) -> Result<TokenStream> {
    let DeriveInput {
        attrs: _attrs,
        vis: _vis,
        ident,
        generics,
        data,
    } = input;
    if !generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            generics,
            "generics are not supported",
        ));
    }

    let Data::Struct(r#struct) = data else {
        return Err(syn::Error::new_spanned(ident, "only structs are supported"));
    };

    let fields_rs = r#struct.fields;
    let fields_rw: Vec<TokenStream> = fields_rs
        .into_iter()
        .map(|field_rs| {
            let Field {
                // We can support #[field(ignore)] or other useful attributes here.
                attrs: _attrs,
                ident: name,
                ty,
                ..
            } = field_rs;
            let name = name.map_or("".to_string(), |name| name.to_string());
            quote! {
                (#name, <#ty as ::risingwave_common::types::WithDataType>::default_data_type())
            }
        })
        .collect();

    Ok(quote! {
        impl ::risingwave_common::types::Fields for #ident {
            fn fields() -> Vec<(&'static str, ::risingwave_common::types::DataType)> {
                vec![#(#fields_rw),*]
            }
        }
    })
}
