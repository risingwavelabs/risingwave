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
use syn::spanned::Spanned;

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
        let (is_table_function, ret) = match ret.trim_start().strip_prefix("setof") {
            Some(s) => (true, s),
            None => (false, ret),
        };

        let user_fn = UserFunctionAttr::parse(item)?;

        Ok(FunctionAttr {
            name: name.trim().to_string(),
            args: if args.is_empty() {
                vec![]
            } else {
                args.split(',').map(|s| s.trim().to_string()).collect()
            },
            ret: ret.trim().to_string(),
            is_table_function,
            batch_fn: find_argument(attr, "batch_fn"),
            state: find_argument(attr, "state"),
            init_state: find_argument(attr, "init_state"),
            prebuild: find_argument(attr, "prebuild"),
            type_infer: find_argument(attr, "type_infer"),
            user_fn,
        })
    }
}

impl UserFunctionAttr {
    fn parse(item: &mut syn::ItemFn) -> Result<Self> {
        let (return_type_kind, iterator_item_kind, core_return_type) = match &item.sig.output {
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
        Ok(UserFunctionAttr {
            name: item.sig.ident.to_string(),
            writer: item.sig.inputs.iter().any(arg_is_writer),
            context: item.sig.inputs.iter().any(arg_is_context),
            arg_option: args_are_all_option(item),
            return_type_kind,
            iterator_item_kind,
            core_return_type,
            generic: item.sig.generics.params.len(),
            return_type_span: item.sig.output.span(),
        })
    }
}

/// Check if the argument is `&mut impl Write`.
fn arg_is_writer(arg: &syn::FnArg) -> bool {
    let syn::FnArg::Typed(arg) = arg else { return false };
    let syn::Type::Reference(syn::TypeReference { elem, .. }) = arg.ty.as_ref() else { return false; };
    let syn::Type::ImplTrait(syn::TypeImplTrait { bounds, .. }) = elem.as_ref() else { return false; };
    let Some(syn::TypeParamBound::Trait(syn::TraitBound { path, .. })) = bounds.first() else { return false; };
    let Some(seg) = path.segments.last() else { return false };
    seg.ident == "Write"
}

/// Check if the argument is `&Context`.
fn arg_is_context(arg: &syn::FnArg) -> bool {
    let syn::FnArg::Typed(arg) = arg else { return false };
    let syn::Type::Reference(syn::TypeReference { elem, .. }) = arg.ty.as_ref() else { return false; };
    let syn::Type::Path(path) = elem.as_ref() else { return false };
    let Some(seg) = path.path.segments.last() else { return false };
    seg.ident == "Context"
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
        if let syn::GenericArgument::Binding(b) = arg && b.ident == "Item" {
            return Some(&b.ty);
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
