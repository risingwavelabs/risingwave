// Copyright 2024 RisingWave Labs
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
use syn::{Data, DeriveInput, Result};

#[proc_macro_derive(Fields, attributes(primary_key))]
pub fn fields(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    inner(tokens.into()).into()
}

fn inner(tokens: TokenStream) -> TokenStream {
    match gen(tokens) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

fn gen(tokens: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(tokens)?;

    let ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "generics are not supported",
        ));
    }

    let Data::Struct(struct_) = &input.data else {
        return Err(syn::Error::new_spanned(
            input.ident,
            "only structs are supported",
        ));
    };

    let fields_rw: Vec<TokenStream> = struct_
        .fields
        .iter()
        .map(|field| {
            let name = field.ident.as_ref().expect("field no name").to_string();
            let ty = &field.ty;
            quote! {
                (#name, <#ty as ::risingwave_common::types::WithDataType>::default_data_type())
            }
        })
        .collect();
    let names = struct_
        .fields
        .iter()
        .map(|field| field.ident.as_ref().expect("field no name"))
        .collect::<Vec<_>>();
    let primary_key = get_primary_key(&input).map(|indices| {
        quote! {
            fn primary_key() -> &'static [usize] {
                &[#(#indices),*]
            }
        }
    });

    Ok(quote! {
        impl ::risingwave_common::types::Fields for #ident {
            fn fields() -> Vec<(&'static str, ::risingwave_common::types::DataType)> {
                vec![#(#fields_rw),*]
            }
            fn into_owned_row(self) -> ::risingwave_common::row::OwnedRow {
                ::risingwave_common::row::OwnedRow::new(vec![#(
                    ::risingwave_common::types::ToOwnedDatum::to_owned_datum(self.#names)
                ),*])
            }
            #primary_key
        }
        impl From<#ident> for ::risingwave_common::types::ScalarImpl {
            fn from(v: #ident) -> Self {
                ::risingwave_common::types::StructValue::new(vec![#(
                    ::risingwave_common::types::ToOwnedDatum::to_owned_datum(v.#names)
                ),*]).into()
            }
        }
    })
}

/// Get primary key indices from `#[primary_key]` attribute.
fn get_primary_key(input: &syn::DeriveInput) -> Option<Vec<usize>> {
    let syn::Data::Struct(struct_) = &input.data else {
        return None;
    };
    // find `#[primary_key(k1, k2, ...)]` on struct
    let composite = input.attrs.iter().find_map(|attr| match &attr.meta {
        syn::Meta::List(list) if list.path.is_ident("primary_key") => Some(&list.tokens),
        _ => None,
    });
    if let Some(keys) = composite {
        let index = |name: &str| {
            struct_
                .fields
                .iter()
                .position(|f| f.ident.as_ref().map_or(false, |i| i == name))
                .expect("primary key not found")
        };
        return Some(
            keys.to_string()
                .split(',')
                .map(|s| index(s.trim()))
                .collect(),
        );
    }
    // find `#[primary_key]` on fields
    for (i, field) in struct_.fields.iter().enumerate() {
        for attr in &field.attrs {
            if matches!(&attr.meta, syn::Meta::Path(path) if path.is_ident("primary_key")) {
                return Some(vec![i]);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use proc_macro2::TokenStream;
    use syn::File;

    fn pretty_print(output: TokenStream) -> String {
        let output: File = syn::parse2(output).unwrap();
        prettyplease::unparse(&output)
    }

    #[test]
    fn test_gen() {
        let code = indoc! {r#"
            #[derive(Fields)]
            #[primary_key(v2, v1)]
            struct Data {
                v1: i16,
                v2: std::primitive::i32,
                v3: bool,
                v4: Serial,
            }
        "#};

        let input: TokenStream = str::parse(code).unwrap();

        let output = super::gen(input).unwrap();

        let output = pretty_print(output);

        let expected = expect_test::expect_file!["gen/test_output.rs"];

        expected.assert_eq(&output);
    }
}
