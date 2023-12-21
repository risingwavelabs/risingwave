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
use syn::{Data, DeriveInput, Field, Result};

#[proc_macro_derive(Fields)]
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
