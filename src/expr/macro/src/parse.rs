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

use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{LitStr, Token};

use super::*;

impl Parse for FunctionAttr {
    /// Parse the attribute of the function macro.
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut parsed = Self::default();

        let sig = input.parse::<LitStr>()?;
        let sig_str = sig.value();
        let (name_args, ret) = sig_str
            .split_once("->")
            .ok_or_else(|| Error::new_spanned(&sig, "expected '->'"))?;
        let (name, args) = name_args
            .split_once('(')
            .ok_or_else(|| Error::new_spanned(&sig, "expected '('"))?;
        let args = args.trim_start().trim_end_matches([')', ' ']);
        let (is_table_function, ret) = match ret.trim_start().strip_prefix("setof") {
            Some(s) => (true, s),
            None => (false, ret),
        };
        parsed.name = name.trim().to_string();
        parsed.args = if args.is_empty() {
            vec![]
        } else {
            args.split(',').map(|s| s.trim().to_string()).collect()
        };
        parsed.ret = ret.trim().to_string();
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
            } else if meta.path().is_ident("deprecated") {
                parsed.deprecated = true;
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
        let sig = &itemfn.sig;
        let (return_type, iterator_item_type) = match &sig.output {
            syn::ReturnType::Default => (ReturnType::T, None),
            syn::ReturnType::Type(_, ty) => {
                let (return_type, inner) = check_type(ty);
                let iterator_item_type = strip_iterator(inner).map(|ty| check_type(ty).0);
                (return_type, iterator_item_type)
            }
        };
        Ok(UserFunctionAttr {
            name: sig.ident.to_string(),
            write: last_arg_is_write(sig),
            retract: last_arg_is_retract(sig),
            arg_option: args_contain_option(sig),
            return_type,
            iterator_item_type,
            generic: sig.generics.params.len(),
            return_type_span: sig.output.span(),
        })
    }
}

/// Check if the last argument is `&mut dyn Write`.
fn last_arg_is_write(sig: &syn::Signature) -> bool {
    let Some(syn::FnArg::Typed(arg)) = sig.inputs.last() else { return false };
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

/// Check if the last argument is `retract: bool`.
fn last_arg_is_retract(sig: &syn::Signature) -> bool {
    let Some(syn::FnArg::Typed(arg)) = sig.inputs.last() else { return false };
    let syn::Pat::Ident(pat) = &*arg.pat else { return false };
    pat.ident.to_string().contains("retract")
}

/// Check if any argument is `Option`.
fn args_contain_option(sig: &syn::Signature) -> bool {
    if sig.inputs.is_empty() {
        return false;
    }
    for arg in &sig.inputs {
        let syn::FnArg::Typed(arg) = arg else { return false };
        let syn::Type::Path(path) = arg.ty.as_ref() else { return false };
        let Some(seg) = path.path.segments.last() else { return false };
        if seg.ident == "Option" {
            return true;
        }
    }
    false
}

/// Check the return type.
fn check_type(ty: &syn::Type) -> (ReturnType, &syn::Type) {
    if let Some(inner) = strip_outer_type(ty, "Result") {
        if let Some(inner) = strip_outer_type(inner, "Option") {
            (ReturnType::ResultOption, inner)
        } else {
            (ReturnType::Result, inner)
        }
    } else if let Some(inner) = strip_outer_type(ty, "Option") {
        (ReturnType::Option, inner)
    } else if let Some(inner) = strip_outer_type(ty, "DatumRef") {
        (ReturnType::Option, inner)
    } else {
        (ReturnType::T, ty)
    }
}

/// Check if the type is `type_<T>` and return `T`.
fn strip_outer_type<'a>(ty: &'a syn::Type, type_: &str) -> Option<&'a syn::Type> {
    let syn::Type::Path(path) = ty else { return None };
    let Some(seg) = path.path.segments.last() else { return None };
    if seg.ident != type_ {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else { return None };
    let Some(syn::GenericArgument::Type(ty)) = args.args.first() else { return None };
    Some(ty)
}

/// Check if the type is `impl Iterator<Item = T>` and return `T`.
fn strip_iterator(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::ImplTrait(impl_trait) = ty else { return None; };
    let syn::TypeParamBound::Trait(trait_bound) = impl_trait.bounds.first()? else { return None; };
    let segment = trait_bound.path.segments.last().unwrap();
    if segment.ident != "Iterator" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(angle_bracketed) = &segment.arguments else {
        return None;
    };
    for arg in &angle_bracketed.args {
        if let syn::GenericArgument::AssocType(b) = arg && b.ident == "Item" {
            return Some(&b.ty);
        }
    }
    None
}
