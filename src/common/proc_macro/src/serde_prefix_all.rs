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

// From https://github.com/zeenix/serde-prefix-all:
//
// MIT License
//
// Copyright (c) 2019 Jonathan Sundqvist
// Copyright (c) 2025 Zeeshan Ali Khan
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

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::ToTokens;
use syn::spanned::Spanned;
use syn::{
    Attribute, AttributeArgs, Data, DataEnum, DataStruct, DeriveInput, Error, Lit, Meta,
    MetaNameValue, NestedMeta, parse_quote,
};

#[derive(Copy, Clone)]
enum Mode {
    Rename,
    Alias,
}

struct ParsedArgs {
    prefix: String,
    prefix_span: Span,
    mode: Mode,
}

fn parse_args(args: AttributeArgs) -> syn::Result<ParsedArgs> {
    let mut prefix: Option<(String, Span)> = None;
    let mut mode: Option<Mode> = None;

    for arg in args {
        match arg {
            NestedMeta::Lit(Lit::Str(lit_str)) if prefix.is_none() => {
                prefix = Some((lit_str.value(), lit_str.span()));
            }
            NestedMeta::Lit(Lit::Str(lit_str)) => {
                return Err(Error::new(
                    lit_str.span(),
                    "Prefix is already specified; duplicate string literal",
                ));
            }
            NestedMeta::Lit(lit) => {
                return Err(Error::new(lit.span(), "The attribute is not a string"));
            }
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, lit, .. }))
                if path.is_ident("mode") =>
            {
                if mode.is_some() {
                    return Err(Error::new(path.span(), "mode is already specified"));
                }

                let parsed_mode = match lit {
                    Lit::Str(lit_str) => match lit_str.value().as_str() {
                        "rename" => Mode::Rename,
                        "alias" => Mode::Alias,
                        _ => {
                            return Err(Error::new(
                                lit_str.span(),
                                "mode must be \"rename\" or \"alias\"",
                            ));
                        }
                    },
                    _ => return Err(Error::new(lit.span(), "mode must be a string literal")),
                };

                mode = Some(parsed_mode);
            }
            NestedMeta::Meta(meta) => {
                return Err(Error::new(
                    meta.span(),
                    "Unsupported attribute argument, expected a string literal prefix or mode",
                ));
            }
        }
    }

    let (prefix, span) =
        prefix.ok_or_else(|| Error::new(Span::call_site(), "Missing prefix string argument"))?;
    Ok(ParsedArgs {
        prefix,
        prefix_span: span,
        mode: mode.unwrap_or(Mode::Rename),
    })
}

pub(crate) fn try_prefix_all(
    args: AttributeArgs,
    mut input: DeriveInput,
) -> syn::Result<TokenStream> {
    let ParsedArgs {
        prefix,
        prefix_span,
        mode,
    } = parse_args(args)?;

    match &mut input.data {
        Data::Enum(item_enum) => handle_enum(item_enum, &prefix[..], mode)?,
        Data::Struct(item_struct) => handle_struct(item_struct, &prefix[..], mode)?,
        _ => {
            return Err(Error::new(
                prefix_span,
                "You can't use the macro on this type",
            ));
        }
    };

    Ok(input.to_token_stream().into())
}

fn create_attribute(prefix: &str, field_name: &str, mode: Mode) -> Attribute {
    let attr_prefix = format!("{prefix}{field_name}");
    match mode {
        Mode::Rename => parse_quote! { #[serde(rename = #attr_prefix)] },
        Mode::Alias => parse_quote! { #[serde(alias = #attr_prefix)] },
    }
}

fn take_skip_attr(attrs: &mut Vec<Attribute>) -> syn::Result<bool> {
    let mut found_skip = false;
    let mut filtered = Vec::with_capacity(attrs.len());

    for attr in attrs.drain(..) {
        if !attr.path.is_ident("serde_prefix_all") {
            filtered.push(attr);
            continue;
        }

        let meta = attr.parse_meta()?;
        match meta {
            Meta::List(list) => {
                let mut has_skip = false;
                for nested in &list.nested {
                    match nested {
                        NestedMeta::Meta(Meta::Path(path)) if path.is_ident("skip") => {
                            has_skip = true;
                        }
                        _ => {
                            return Err(Error::new(
                                nested.span(),
                                "Expected #[serde_prefix_all(skip)]",
                            ));
                        }
                    }
                }

                if !has_skip {
                    return Err(Error::new(
                        list.span(),
                        "Expected #[serde_prefix_all(skip)]",
                    ));
                }

                found_skip = true;
            }
            _ => {
                return Err(Error::new(
                    meta.span(),
                    "Expected #[serde_prefix_all(skip)]",
                ));
            }
        }
    }

    *attrs = filtered;
    Ok(found_skip)
}

fn handle_enum(input: &mut DataEnum, prefix: &str, mode: Mode) -> syn::Result<()> {
    let variants = &mut input.variants;
    for variant in variants.iter_mut() {
        if take_skip_attr(&mut variant.attrs)? {
            continue;
        }

        let field_name = variant.ident.to_string();
        let attr = create_attribute(prefix, &field_name[..], mode);
        variant.attrs.push(attr);
    }

    Ok(())
}

fn handle_struct(input: &mut DataStruct, prefix: &str, mode: Mode) -> syn::Result<()> {
    let fields = &mut input.fields;
    for field in fields.iter_mut() {
        if take_skip_attr(&mut field.attrs)? {
            continue;
        }

        let field_name = field.ident.as_ref().unwrap().to_string();
        let attr = create_attribute(prefix, &field_name[..], mode);
        field.attrs.push(attr);
    }

    Ok(())
}
