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

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Result};

#[proc_macro_derive(Fields, attributes(primary_key, fields))]
pub fn fields(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    inner(tokens.into()).into()
}

fn inner(tokens: TokenStream) -> TokenStream {
    match r#gen(tokens) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

fn r#gen(tokens: TokenStream) -> Result<TokenStream> {
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

    let style = get_style(&input);
    if let Some(style) = &style {
        if !["Title Case", "TITLE CASE", "snake_case"].contains(&style.value().as_str()) {
            return Err(syn::Error::new_spanned(
                style,
                "only `Title Case`, `TITLE CASE`, and `snake_case` are supported",
            ));
        }
    }

    let fields_rw: Vec<TokenStream> = struct_
        .fields
        .iter()
        .map(|field| {
            let mut name = field.ident.as_ref().expect("field no name").to_string();
            // strip leading `r#`
            if name.starts_with("r#") {
                name = name[2..].to_string();
            }
            // cast style
            match style.as_ref().map_or(String::new(), |f| f.value()).as_str() {
                "Title Case" => name = to_title_case(&name),
                "TITLE CASE" => name = to_title_case(&name).to_uppercase(),
                _ => {}
            }
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
    let primary_key = get_primary_key(&input).map_or_else(
        || quote! { None },
        |indices| {
            quote! { Some(&[#(#indices),*]) }
        },
    );

    Ok(quote! {
        impl ::risingwave_common::types::Fields for #ident {
            const PRIMARY_KEY: Option<&'static [usize]> = #primary_key;

            fn fields() -> Vec<(&'static str, ::risingwave_common::types::DataType)> {
                vec![#(#fields_rw),*]
            }
            fn into_owned_row(self) -> ::risingwave_common::row::OwnedRow {
                ::risingwave_common::row::OwnedRow::new(vec![#(
                    ::risingwave_common::types::ToOwnedDatum::to_owned_datum(self.#names)
                ),*])
            }
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
                .position(|f| f.ident.as_ref().is_some_and(|i| i == name))
                .expect("primary key not found")
        };
        return Some(
            keys.to_string()
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(index)
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

/// Get name style from `#[fields(style = "xxx")]` attribute.
fn get_style(input: &syn::DeriveInput) -> Option<syn::LitStr> {
    let style = input.attrs.iter().find_map(|attr| match &attr.meta {
        syn::Meta::List(list) if list.path.is_ident("fields") => {
            let name_value: syn::MetaNameValue = syn::parse2(list.tokens.clone()).ok()?;
            if name_value.path.is_ident("style") {
                Some(name_value.value)
            } else {
                None
            }
        }
        _ => None,
    })?;
    match style {
        syn::Expr::Lit(lit) => match lit.lit {
            syn::Lit::Str(s) => Some(s),
            _ => None,
        },
        _ => None,
    }
}

/// Convert `snake_case` to `Title Case`.
fn to_title_case(s: &str) -> String {
    let mut title = String::new();
    let mut next_upper = true;
    for c in s.chars() {
        if c == '_' {
            title.push(' ');
            next_upper = true;
        } else if next_upper {
            title.push(c.to_uppercase().next().unwrap());
            next_upper = false;
        } else {
            title.push(c);
        }
    }
    title
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

    fn do_test(code: &str, expected_path: &str) {
        let input: TokenStream = str::parse(code).unwrap();

        let output = super::r#gen(input).unwrap();

        let output = pretty_print(output);

        let expected = expect_test::expect_file![expected_path];

        expected.assert_eq(&output);
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
                r#type: i32,
            }
        "#};

        do_test(code, "gen/test_output.rs");
    }

    #[test]
    fn test_no_pk() {
        let code = indoc! {r#"
            #[derive(Fields)]
            struct Data {
                v1: i16,
                v2: String,
            }
        "#};

        do_test(code, "gen/test_no_pk.rs");
    }

    #[test]
    fn test_empty_pk() {
        let code = indoc! {r#"
            #[derive(Fields)]
            #[primary_key()]
            struct Data {
                v1: i16,
                v2: String,
            }
        "#};

        do_test(code, "gen/test_empty_pk.rs");
    }
}
