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

#![feature(let_chains)]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Data, DeriveInput, Fields, Meta, NestedMeta};

enum FieldRedactionAction {
    NoAction,
    Rename(String),
    UseRustName,
    Flatten,
}

/// This derive macro generates implementations of the `MarkRedaction` trait for structs with named fields.
///
/// # Usage
///
/// 1. Annotate the struct with `#[derive(MarkRedaction)]`.
///
/// 2. By default, no field of the struct is marked for redaction.
///
/// 3. To mark a field for redaction, annotate it with the `mark_redaction` helper attribute, in one of the following forms:
///
///    - `#[mark_redaction]`. It includes the Rust name of the field.
///
///    - `#[mark_redaction(alias="name_1", alias="name_2")]`. It includes the Rust name of the field, and the alias names. Multiple aliases can be specified.
///
///    - `#[mark_redaction(rename="new_name")]`. It includes the renamed name, and optional alias names if any.
///
///    - `#[mark_redaction(flatten)]`. It invokes `MarkRedaction::marks` on the field's type, and includes the resulted marks.
///
/// # Example
///
/// ```
/// #[derive(MarkRedaction)]
/// struct Bar {
///     #[mark_redaction(rename("bar.a"))]
///     a: i32,
///     b: i32,
/// }
///
/// #[derive(MarkRedaction)]
/// struct Foo {
///     #[mark_redaction]
///     a: i32,
///     #[mark_redaction(rename("foo.b"), alias = "foo.b_2")]
///     b: i32,
///     #[mark_redaction(alias = "foo.c")]
///     c: i32,
///     d: i32,
///     #[mark_redaction(flatten)]
///     bar: Bar,
/// }
///
/// // Bar::remarks will return ["bar.a"].
/// // Foo::remarks will return ["a", "foo.b", "foo.b_2", "c", "foo.c", "bar.a"].
/// ```
#[proc_macro_derive(MarkRedaction, attributes(mark_redaction))]
pub fn mark_redaction(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);
    match mark_redaction_impl(input) {
        Ok(token_stream) => token_stream.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn mark_redaction_impl(input: DeriveInput) -> Result<TokenStream2, syn::Error> {
    let fields = match &input.data {
        Data::Struct(data) => &data.fields,
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "MarkRedaction can only annotate struct",
            ));
        }
    };
    let Fields::Named(fields) = fields else {
        return Err(syn::Error::new_spanned(
            fields,
            "MarkRedaction can only annotate struct with named fields",
        ));
    };
    let struct_name = input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let mut marked_names = vec![];
    let mut field_types_to_expand = vec![];
    let helper_attr = "mark_redaction";
    let flatten_ident = "flatten";
    let alias_ident = "alias";
    let rename_ident = "rename";
    for field in &fields.named {
        let mut action = FieldRedactionAction::NoAction;
        let field_name = field
            .ident
            .clone()
            .unwrap_or_else(|| panic!("named field must have name"))
            .to_string();
        for attr in &field.attrs {
            let meta = match attr.parse_meta() {
                Ok(m) => m,
                Err(e) => {
                    return Err(syn::Error::new_spanned(
                        attr,
                        format!("failed to parse attrs: {e}"),
                    ));
                }
            };
            match meta {
                Meta::Path(path) => {
                    if !path.is_ident(helper_attr) {
                        continue;
                    }
                    // #[mark_redaction]
                    try_set_action(&mut action, FieldRedactionAction::UseRustName, &path)?;
                }
                Meta::List(list) => {
                    if !list.path.is_ident(helper_attr) {
                        continue;
                    }
                    // action may be override
                    try_set_action(&mut action, FieldRedactionAction::UseRustName, &list)?;
                    for nested_meta in list.nested {
                        let attr_meta = match nested_meta {
                            NestedMeta::Meta(m) => m,
                            NestedMeta::Lit(l) => {
                                return Err(syn::Error::new_spanned(&l, "unexpected attr literal"));
                            }
                        };
                        let mut accepted = false;
                        match &attr_meta {
                            Meta::Path(path) => {
                                if path.is_ident(flatten_ident) {
                                    // #[mark_redaction(flatten)]
                                    try_set_action(
                                        &mut action,
                                        FieldRedactionAction::Flatten,
                                        path,
                                    )?;
                                    field_types_to_expand.push(field.ty.clone());
                                    accepted = true;
                                }
                            }
                            Meta::NameValue(name_value) => {
                                if name_value.path.is_ident(alias_ident) {
                                    if let syn::Lit::Str(l) = &name_value.lit {
                                        // #[mark_redaction(alias="xxx")]
                                        marked_names.push(l.value());
                                        accepted = true;
                                    }
                                } else if name_value.path.is_ident(rename_ident) {
                                    if let syn::Lit::Str(l) = &name_value.lit {
                                        // #[mark_redaction(rename="xxx")]
                                        try_set_action(
                                            &mut action,
                                            FieldRedactionAction::Rename(l.value()),
                                            name_value,
                                        )?;
                                        accepted = true;
                                    }
                                }
                            }
                            Meta::List(_) => {}
                        }
                        if !accepted {
                            return Err(syn::Error::new_spanned(
                                &attr_meta,
                                "unexpected attr meta",
                            ));
                        }
                    }
                }
                Meta::NameValue(_) => {}
            }
        }
        match action {
            FieldRedactionAction::Rename(rename) => {
                marked_names.push(rename);
            }
            FieldRedactionAction::UseRustName => {
                marked_names.push(field_name);
            }
            _ => {}
        }
    }

    let output = quote! {
        impl #impl_generics mark_redaction::MarkRedaction for #struct_name #type_generics #where_clause {
            fn marks() -> std::collections::HashSet<String> {
                let mut ret: std::collections::HashSet<String> = Default::default();
                let ident: Vec<String> = vec![#(#marked_names.to_string(),)*];
                ret.extend(ident);
                let expand: Vec<std::collections::HashSet<String>> = vec![#(#field_types_to_expand::marks(),)*];
                ret.extend(expand.into_iter().flatten());
                ret
            }
        }
    };

    Ok(output)
}

fn try_set_action(
    current_action: &mut FieldRedactionAction,
    new_action: FieldRedactionAction,
    tokens: impl ToTokens,
) -> Result<(), syn::Error> {
    let mut reject = false;
    match current_action {
        FieldRedactionAction::NoAction => {
            *current_action = new_action;
        }
        FieldRedactionAction::Rename(_) => {
            // rename is incompatible with all other actions.
            reject = true;
        }
        FieldRedactionAction::UseRustName => {
            if matches!(new_action, FieldRedactionAction::NoAction) {
                // unset is not allowed
                reject = true;
            } else {
                *current_action = new_action;
            }
        }
        FieldRedactionAction::Flatten => {
            // flatten is incompatible with all other actions.
            reject = true;
        }
    }
    if reject {
        return Err(syn::Error::new_spanned(
            tokens,
            "incompatible redaction actions",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use syn::{parse_quote, DeriveInput};

    use crate::mark_redaction_impl;

    #[test]
    fn test_basic() {
        let input: DeriveInput = parse_quote!(
            struct Foo {}
        );
        mark_redaction_impl(input).unwrap();

        let input: DeriveInput = parse_quote!(
            struct Foo(i32);
        );
        mark_redaction_impl(input).unwrap_err();

        let input: DeriveInput = parse_quote!(
            enum Foo {
                A,
                B,
            }
        );
        mark_redaction_impl(input).unwrap_err();

        let input: DeriveInput = parse_quote!(
            union Foo {
                a: i32,
                b: i32,
            }
        );
        mark_redaction_impl(input).unwrap_err();
    }

    #[test]
    fn test_attr() {
        let input: DeriveInput = parse_quote!(
            struct Foo {
                #[mark_redaction]
                a: i32,
                b: i32,
                #[mark_redaction(rename = "c_rename", alias = "c_1", alias = "c_2")]
                c: i32,
                #[mark_redaction(alias = "d_1")]
                d: i32,
                #[mark_redaction(flatten)]
                e: Bar<i32>,
                #[mark_redaction(flatten)]
                f: HashSet<String, String>,
            }
        );
        mark_redaction_impl(input).unwrap();
        let input: DeriveInput = parse_quote!(
            struct Foo<T> {
                #[mark_redaction]
                a: i32,
                #[mark_redaction]
                b: T,
                #[mark_redaction(flatten)]
                c: T,
            }
        );
        mark_redaction_impl(input).unwrap();

        let input: DeriveInput = parse_quote!(
            struct Foo {
                #[mark_redaction(rename = "c_rename", rename = "c_rename_2")]
                a: i32,
            }
        );
        mark_redaction_impl(input).unwrap_err();

        let input: DeriveInput = parse_quote!(
            struct Foo {
                #[mark_redaction(flatten, rename = "c_rename_2")]
                a: Bar,
            }
        );
        mark_redaction_impl(input).unwrap_err();

        let input: DeriveInput = parse_quote!(
            struct Foo {
                #[mark_redaction(flatten, flatten)]
                a: Bar,
            }
        );
        mark_redaction_impl(input).unwrap_err();

        let input: DeriveInput = parse_quote!(
            struct Foo {
                #[mark_redaction(non_sense)]
                a: Bar,
            }
        );
        mark_redaction_impl(input).unwrap_err();

        let input: DeriveInput = parse_quote!(
            struct Foo {
                #[mark_redaction(non_sense = "123")]
                a: Bar,
            }
        );
        mark_redaction_impl(input).unwrap_err();

        let input: DeriveInput = parse_quote!(
            struct Foo {
                #[mark_redaction(alias = 123)]
                a: Bar,
            }
        );
        mark_redaction_impl(input).unwrap_err();

        let input: DeriveInput = parse_quote!(
            struct Foo {
                #[mark_redaction(rename = 123)]
                a: Bar,
            }
        );
        mark_redaction_impl(input).unwrap_err();
    }
}
