//! Parse the tokens of the macro.

use proc_macro2::Span;

use super::*;

impl FunctionAttr {
    /// Parse the attribute of the function macro.
    pub fn parse(attr: &syn::AttributeArgs, item: &syn::ItemFn) -> Result<Self> {
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
            .split_once("(")
            .ok_or_else(|| Error::new_spanned(sig, "expected '('"))?;

        let batch = attr.iter().find_map(|n| {
            let syn::NestedMeta::Meta(syn::Meta::NameValue(nv)) = n else { return None };
            if !nv.path.is_ident("batch") {
                return None;
            };
            let syn::Lit::Str(ref lit_str) = nv.lit else { return None };
            Some(lit_str.value())
        });

        let user_fn = UserFunctionAttr::parse(item)?;

        Ok(FunctionAttr {
            name: name.trim().to_string(),
            args: args
                .trim_end_matches([')', ' '])
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            ret: ret.trim().to_string(),
            batch,
            user_fn,
        })
    }
}

impl UserFunctionAttr {
    fn parse(item: &syn::ItemFn) -> Result<Self> {
        Ok(UserFunctionAttr {
            name: item.sig.ident.to_string(),
            write: last_arg_is_write(item),
            arg_option: args_are_all_option(item),
            return_option: return_value_is(item, "Option"),
            return_result: return_value_is(item, "Result"),
        })
    }
}

/// Check if the last argument is `&mut dyn Write`.
fn last_arg_is_write(item: &syn::ItemFn) -> bool {
    let Some(syn::FnArg::Typed(arg)) = item.sig.inputs.last() else { return false };
    let syn::Type::Reference(syn::TypeReference { elem, .. }) = arg.ty.as_ref() else { return false };
    let syn::Type::TraitObject(syn::TypeTraitObject { bounds, .. }) = elem.as_ref() else { return false };
    let Some(syn::TypeParamBound::Trait(syn::TraitBound { path, .. })) = bounds.first() else { return false };
    path.segments.last().map_or(false, |s| s.ident == "Write")
}

/// Check if all arguments are `Option`s.
fn args_are_all_option(item: &syn::ItemFn) -> bool {
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

/// Check if the return value is `Result`.
fn return_value_is(item: &syn::ItemFn, type_: &str) -> bool {
    let syn::ReturnType::Type(_, ty) = &item.sig.output else { return false };
    let syn::Type::Path(path) = ty.as_ref() else { return false };
    let Some(seg) = path.path.segments.last() else { return false };
    seg.ident == type_
}
