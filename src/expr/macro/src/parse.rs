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

//! Parse the tokens of the macro.

use proc_macro_error::abort;
use quote::ToTokens;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{LitStr, Token, parse_quote};

use super::*;

impl Parse for FunctionAttr {
    /// Parse the attribute of the function macro.
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut parsed = Self::default();

        let sig = input.parse::<LitStr>()?;
        let sig_str = sig.value();
        let (name_args, ret) = match sig_str.split_once("->") {
            Some((name_args, ret)) => (name_args, ret),
            None => (sig_str.as_str(), "void"),
        };
        let (name, args) = name_args
            .split_once('(')
            .ok_or_else(|| Error::new_spanned(&sig, "expected '('"))?;
        let args = args.trim_start().trim_end_matches([')', ' ']);
        let (is_table_function, ret) = match ret.trim_start().strip_prefix("setof") {
            Some(s) => (true, s),
            None => (false, ret),
        };
        parsed.name = name.trim().to_owned();
        parsed.args = if args.is_empty() {
            vec![]
        } else {
            args.split(',').map(|s| s.trim().to_owned()).collect()
        };
        parsed.ret = ret.trim().to_owned();
        parsed.is_table_function = is_table_function;

        if input.parse::<Token![,]>().is_err() {
            return Ok(parsed);
        }

        let metas = input.parse_terminated(syn::Meta::parse, Token![,])?;
        for meta in metas {
            let get_value = || {
                let kv = meta.require_name_value()?;
                let syn::Expr::Lit(lit) = &kv.value else {
                    return Err(Error::new(kv.value.span(), "expected literal"));
                };
                let syn::Lit::Str(lit) = &lit.lit else {
                    return Err(Error::new(kv.value.span(), "expected string literal"));
                };
                Ok(lit.value())
            };
            if meta.path().is_ident("batch_fn") {
                parsed.batch_fn = Some(get_value()?);
            } else if meta.path().is_ident("state") {
                parsed.state = Some(get_value()?);
            } else if meta.path().is_ident("init_state") {
                parsed.init_state = Some(get_value()?);
            } else if meta.path().is_ident("prebuild") {
                parsed.prebuild = Some(get_value()?);
            } else if meta.path().is_ident("type_infer") {
                parsed.type_infer = Some(get_value()?);
            } else if meta.path().is_ident("generic") {
                parsed.generic = Some(get_value()?);
            } else if meta.path().is_ident("volatile") {
                parsed.volatile = true;
            } else if meta.path().is_ident("deprecated") || meta.path().is_ident("internal") {
                parsed.deprecated = true;
            } else if meta.path().is_ident("rewritten") {
                parsed.rewritten = true;
            } else if meta.path().is_ident("append_only") {
                parsed.append_only = true;
            } else {
                return Err(Error::new(
                    meta.span(),
                    format!("invalid property: {:?}", meta.path()),
                ));
            }
        }
        Ok(parsed)
    }
}

impl Parse for UserFunctionAttr {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let itemfn: syn::ItemFn = input.parse()?;
        Ok(UserFunctionAttr::from(&itemfn.sig))
    }
}

impl From<&syn::Signature> for UserFunctionAttr {
    fn from(sig: &syn::Signature) -> Self {
        let (return_type_kind, iterator_item_kind, core_return_type) = match &sig.output {
            syn::ReturnType::Default => (ReturnTypeKind::T, None, "()".into()),
            syn::ReturnType::Type(_, ty) => {
                let (kind, inner) = check_type(ty);
                match strip_iterator(inner) {
                    Some(ty) => {
                        let (inner_kind, inner) = check_type(ty);
                        (kind, Some(inner_kind), inner.to_token_stream().to_string())
                    }
                    None => (kind, None, inner.to_token_stream().to_string()),
                }
            }
        };
        UserFunctionAttr {
            name: sig.ident.to_string(),
            async_: sig.asyncness.is_some(),
            writer_type_kind: sig.inputs.last().and_then(arg_writer_type),
            context: sig.inputs.iter().any(arg_is_context),
            retract: last_arg_is_retract(sig),
            args_option: sig.inputs.iter().map(arg_is_option).collect(),
            first_mut_ref_arg: first_mut_ref_arg(sig),
            return_type_kind,
            iterator_item_kind,
            core_return_type,
            generic: sig.generics.params.len(),
            return_type_span: sig.output.span(),
        }
    }
}

impl Parse for AggregateImpl {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let itemimpl: syn::ItemImpl = input.parse()?;
        let parse_function = |name: &str| {
            itemimpl.items.iter().find_map(|item| match item {
                syn::ImplItem::Fn(syn::ImplItemFn { sig, .. }) if sig.ident == name => {
                    Some(UserFunctionAttr::from(sig))
                }
                _ => None,
            })
        };
        let self_path = itemimpl.self_ty.to_token_stream().to_string();
        let struct_name = match self_path.split_once('<') {
            Some((path, _)) => path.trim().into(), // remove generic parameters
            None => self_path,
        };
        Ok(AggregateImpl {
            struct_name,
            accumulate: parse_function("accumulate").expect("expect accumulate function"),
            retract: parse_function("retract"),
            merge: parse_function("merge"),
            finalize: parse_function("finalize"),
            create_state: parse_function("create_state"),
            encode_state: parse_function("encode_state"),
            decode_state: parse_function("decode_state"),
        })
    }
}

impl Parse for AggregateFnOrImpl {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        // consume attributes
        let _ = input.call(syn::Attribute::parse_outer)?;
        if input.peek(Token![impl]) {
            Ok(AggregateFnOrImpl::Impl(input.parse()?))
        } else {
            Ok(AggregateFnOrImpl::Fn(input.parse()?))
        }
    }
}

/// Check if the argument is a writer and return its type.
fn arg_writer_type(arg: &syn::FnArg) -> Option<WriterTypeKind> {
    let syn::FnArg::Typed(arg) = arg else {
        return None;
    };
    let syn::Type::Reference(syn::TypeReference { elem, .. }) = arg.ty.as_ref() else {
        return None;
    };
    let elem = elem.as_ref();
    if elem == &parse_quote!(impl std::fmt::Write) {
        Some(WriterTypeKind::FmtWrite)
    } else if elem == &parse_quote!(impl std::io::Write) {
        Some(WriterTypeKind::IoWrite)
    } else if elem == &parse_quote!(impl Write) {
        abort! { elem, "use of ambiguous `Write` trait.";
            note = "`function!` macro can only recognize fully-qualified type.";
            help = "Please use `std::fmt::Write` or `std::io::Write` instead."
        };
    } else if elem == &parse_quote!(jsonbb::Builder) {
        Some(WriterTypeKind::JsonbbBuilder)
    } else if elem == &parse_quote!(Builder) {
        abort! { elem, "use of ambiguous `Builder` type.";
            note = "`function!` macro can only recognize fully-qualified type.";
            help = "Please use `jsonbb::Builder` instead."
        };
    } else {
        None
    }
}

/// Check if the argument is `&Context`.
fn arg_is_context(arg: &syn::FnArg) -> bool {
    let syn::FnArg::Typed(arg) = arg else {
        return false;
    };
    let syn::Type::Reference(syn::TypeReference { elem, .. }) = arg.ty.as_ref() else {
        return false;
    };
    let syn::Type::Path(path) = elem.as_ref() else {
        return false;
    };
    let Some(seg) = path.path.segments.last() else {
        return false;
    };
    seg.ident == "Context"
}

/// Check if the last argument is `retract: bool`.
fn last_arg_is_retract(sig: &syn::Signature) -> bool {
    let Some(syn::FnArg::Typed(arg)) = sig.inputs.last() else {
        return false;
    };
    let syn::Pat::Ident(pat) = &*arg.pat else {
        return false;
    };
    pat.ident.to_string().contains("retract")
}

/// Check if the argument is `Option`.
fn arg_is_option(arg: &syn::FnArg) -> bool {
    let syn::FnArg::Typed(arg) = arg else {
        return false;
    };
    let syn::Type::Path(path) = arg.ty.as_ref() else {
        return false;
    };
    let Some(seg) = path.path.segments.last() else {
        return false;
    };
    seg.ident == "Option"
}

/// Returns `T` if the first argument (except `self`) is `&mut T`.
fn first_mut_ref_arg(sig: &syn::Signature) -> Option<String> {
    let arg = match sig.inputs.first()? {
        syn::FnArg::Typed(arg) => arg,
        syn::FnArg::Receiver(_) => match sig.inputs.iter().nth(1)? {
            syn::FnArg::Typed(arg) => arg,
            _ => return None,
        },
    };
    let syn::Type::Reference(syn::TypeReference {
        elem,
        mutability: Some(_),
        ..
    }) = arg.ty.as_ref()
    else {
        return None;
    };
    Some(elem.to_token_stream().to_string())
}

/// Check the return type.
fn check_type(ty: &syn::Type) -> (ReturnTypeKind, &syn::Type) {
    if let Some(inner) = strip_outer_type(ty, "Result") {
        if let Some(inner) = strip_outer_type(inner, "Option") {
            (ReturnTypeKind::ResultOption, inner)
        } else {
            (ReturnTypeKind::Result, inner)
        }
    } else if let Some(inner) = strip_outer_type(ty, "Option") {
        (ReturnTypeKind::Option, inner)
    } else if let Some(inner) = strip_outer_type(ty, "DatumRef") {
        (ReturnTypeKind::Option, inner)
    } else {
        (ReturnTypeKind::T, ty)
    }
}

/// Check if the type is `type_<T>` and return `T`.
fn strip_outer_type<'a>(ty: &'a syn::Type, type_: &str) -> Option<&'a syn::Type> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let seg = path.path.segments.last()?;
    if seg.ident != type_ {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    let Some(syn::GenericArgument::Type(ty)) = args.args.first() else {
        return None;
    };
    Some(ty)
}

/// Check if the type is `impl Iterator<Item = T>` and return `T`.
fn strip_iterator(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::ImplTrait(impl_trait) = ty else {
        return None;
    };
    let syn::TypeParamBound::Trait(trait_bound) = impl_trait.bounds.first()? else {
        return None;
    };
    let segment = trait_bound.path.segments.last().unwrap();
    if segment.ident != "Iterator" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(angle_bracketed) = &segment.arguments else {
        return None;
    };
    for arg in &angle_bracketed.args {
        if let syn::GenericArgument::AssocType(b) = arg
            && b.ident == "Item"
        {
            return Some(&b.ty);
        }
    }
    None
}
