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

//! Parse the tokens of the macro.

use proc_macro2::Span;

use super::*;

impl FunctionAttr {
    /// Parse the attribute of the function macro.
    pub fn parse(attr: &syn::AttributeArgs, item: &mut syn::ItemFn) -> Result<Self> {
        let sig = attr.get(0).ok_or_else(|| {
            Error::new(
                Span::call_site(),
                "expected #[function(\"name(arg1, arg2) -> ret\")]",
            )
        })?;

        let sig_str = match sig {
            syn::NestedMeta::Lit(syn::Lit::Str(lit_str)) => lit_str.value(),
            _ => return Err(Error::new_spanned(sig, "expected string literal")),
        };

        let (name_args, ret) = sig_str
            .split_once("->")
            .ok_or_else(|| Error::new_spanned(sig, "expected '->'"))?;
        let (name, args) = name_args
            .split_once('(')
            .ok_or_else(|| Error::new_spanned(sig, "expected '('"))?;
        let args = args.trim_start().trim_end_matches([')', ' ']);

        let user_fn = UserFunctionAttr::parse(item)?;

        Ok(FunctionAttr {
            name: name.trim().to_string(),
            args: if args.is_empty() {
                vec![]
            } else {
                args.split(',').map(|s| s.trim().to_string()).collect()
            },
            ret: ret.trim().to_string(),
            batch_fn: find_argument(attr, "batch_fn"),
            state: find_argument(attr, "state"),
            init_state: find_argument(attr, "init_state"),
            user_fn,
        })
    }
}

impl UserFunctionAttr {
    fn parse(item: &mut syn::ItemFn) -> Result<Self> {
        Ok(UserFunctionAttr {
            name: item.sig.ident.to_string(),
            write: last_arg_is_write(item),
            arg_option: args_are_all_option(item),
            return_type: return_type(item),
            generic: item.sig.generics.params.len(),
            // prebuild: extract_prebuild_arg(item),
        })
    }
}

/// Check if the last argument is `&mut dyn Write`.
fn last_arg_is_write(item: &syn::ItemFn) -> bool {
    let Some(syn::FnArg::Typed(arg)) = item.sig.inputs.last() else { return false };
    let syn::Type::Reference(syn::TypeReference { elem, .. }) = arg.ty.as_ref() else {
        return false;
    };
    let syn::Type::TraitObject(syn::TypeTraitObject { bounds, .. }) = elem.as_ref() else {
        return false;
    };
    let Some(syn::TypeParamBound::Trait(syn::TraitBound { path, .. })) = bounds.first() else {
        return false;
    };
    path.segments.last().map_or(false, |s| s.ident == "Write")
}

/// Check if all arguments are `Option`s.
fn args_are_all_option(item: &syn::ItemFn) -> bool {
    if item.sig.inputs.is_empty() {
        return false;
    }
    for arg in &item.sig.inputs {
        let syn::FnArg::Typed(arg) = arg else { return false };
        let syn::Type::Path(path) = arg.ty.as_ref() else { return false };
        let Some(seg) = path.path.segments.last() else { return false };
        if seg.ident != "Option" {
            return false;
        }
    }
    true
}

/// Check the return type.
fn return_type(item: &syn::ItemFn) -> ReturnType {
    if return_value_is_result_option(item) {
        ReturnType::ResultOption
    } else if return_value_is(item, "Result") {
        ReturnType::Result
    } else if return_value_is(item, "Option") {
        ReturnType::Option
    } else {
        ReturnType::T
    }
}

/// Check if the return value is `type_`.
fn return_value_is(item: &syn::ItemFn, type_: &str) -> bool {
    let syn::ReturnType::Type(_, ty) = &item.sig.output else { return false };
    let syn::Type::Path(path) = ty.as_ref() else { return false };
    let Some(seg) = path.path.segments.last() else { return false };
    seg.ident == type_
}

/// Check if the return value is `Result<Option<T>>`.
fn return_value_is_result_option(item: &syn::ItemFn) -> bool {
    let syn::ReturnType::Type(_, ty) = &item.sig.output else { return false };
    let syn::Type::Path(path) = ty.as_ref() else { return false };
    let Some(seg) = path.path.segments.last() else { return false };
    if seg.ident != "Result" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else { return false };
    let Some(syn::GenericArgument::Type(ty)) = args.args.first() else { return false };
    let syn::Type::Path(path) = ty else { return false };
    let Some(seg) = path.path.segments.last() else { return false };
    seg.ident == "Option"
}

/// Extract `#[prebuild("function_name")]` from arguments.
fn _extract_prebuild_arg(item: &mut syn::ItemFn) -> Option<(usize, String)> {
    for (i, arg) in item.sig.inputs.iter_mut().enumerate() {
        let syn::FnArg::Typed(arg) = arg else { continue };
        if let Some(idx) = arg
            .attrs
            .iter_mut()
            .position(|att| att.path.is_ident("prebuild"))
        {
            let attr = arg.attrs.remove(idx);
            // XXX: this is a hack to parse a string literal from token stream
            let s = attr.tokens.to_string();
            let s = s.trim_start_matches("(\"").trim_end_matches("\")");
            return Some((i, s.to_string()));
        }
    }
    None
}

/// Find argument `#[xxx(.., name = "value")]`.
fn find_argument(attr: &syn::AttributeArgs, name: &str) -> Option<String> {
    attr.iter().find_map(|n| {
        let syn::NestedMeta::Meta(syn::Meta::NameValue(nv)) = n else { return None };
        if !nv.path.is_ident(name) {
            return None;
        }
        let syn::Lit::Str(ref lit_str) = nv.lit else { return None };
        Some(lit_str.value())
    })
}
