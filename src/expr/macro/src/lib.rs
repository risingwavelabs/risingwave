use itertools::Itertools;
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote, ToTokens};
use syn::{parse_macro_input, Error, Result};

mod sql_type;
mod utils;

#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as syn::AttributeArgs);
    let item = parse_macro_input!(item as syn::ItemFn);
    match parse_function(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn parse_function(attr: syn::AttributeArgs, item: syn::ItemFn) -> Result<TokenStream2> {
    let fn_attr = FunctionAttr::parse(&attr, &item)?;

    let mut tokens = item.into_token_stream();
    tokens.extend(fn_attr.generate_descriptors()?);
    Ok(tokens)
}

#[derive(Debug)]
struct FunctionAttr {
    name: String,
    args: Vec<String>,
    ret: String,
    batch: Option<String>,
    user_fn: UserFunctionAttr,
}

impl FunctionAttr {
    /// Parse the attribute of the function macro.
    fn parse(attr: &syn::AttributeArgs, item: &syn::ItemFn) -> Result<Self> {
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

    /// Generate descriptors of the function.
    ///
    /// If the function arguments or return type contains wildcard, it will generate descriptors for
    /// each of them.
    fn generate_descriptors(&self) -> Result<TokenStream2> {
        let args = self
            .args
            .iter()
            .map(|ty| sql_type::expand_type_wildcard(&ty));
        let ret = sql_type::expand_type_wildcard(&self.ret);
        let mut tokens = TokenStream2::new();
        for (args, ret) in args.multi_cartesian_product().cartesian_product(ret) {
            let attr = FunctionAttr {
                name: self.name.clone(),
                args: args.iter().map(|s| s.to_string()).collect(),
                ret: ret.to_string(),
                batch: self.batch.clone(),
                user_fn: self.user_fn.clone(),
            };
            tokens.extend(attr.generate_descriptor_one()?);
        }
        Ok(tokens)
    }

    /// Generate a descriptor of the function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    fn generate_descriptor_one(&self) -> Result<TokenStream2> {
        let name = self.name.clone();

        fn to_data_type_name(ty: &str) -> Result<TokenStream2> {
            let variant = format_ident!(
                "{}",
                sql_type::to_data_type_name(ty).ok_or_else(|| Error::new(
                    Span::call_site(),
                    format!("unknown type: {}", ty),
                ))?
            );
            Ok(quote! { risingwave_common::types::DataTypeName::#variant })
        }
        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(to_data_type_name(ty)?);
        }
        let ret = to_data_type_name(match self.ret.as_str() {
            "auto" => sql_type::min_compatible_type(&self.args),
            t => t,
        })?;

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let descriptor_name = format_ident!("{}_{}_{}", self.name, self.args.join("_"), self.ret);
        let descriptor_type = quote! { crate::sig::func::FunctionDescriptor };
        let build_fn = self.generate_build_fn();
        Ok(quote! {
            static #descriptor_name: #descriptor_type = #descriptor_type {
                name: #name,
                ty: risingwave_pb::expr::expr_node::Type::#pb_type,
                args: &[#(#args),*],
                ret: #ret,
                build_from_prost: #build_fn,
            };
        })
    }

    fn generate_build_fn(&self) -> TokenStream2 {
        quote! {
            |prost| {
                todo!()
            }
        }
    }
}

#[derive(Debug, Clone)]
struct UserFunctionAttr {
    /// The last argument type is `&mut dyn Writer`.
    writer: bool,
    /// The argument type are `Option`s.
    nullable: bool,
    /// The return type is `Result`.
    fallible: bool,
}

impl UserFunctionAttr {
    fn parse(item: &syn::ItemFn) -> Result<Self> {
        Ok(UserFunctionAttr {
            writer: last_arg_is_writer(item),
            nullable: args_are_all_option(item),
            fallible: return_value_is_result(item),
        })
    }
}

/// Check if the last argument is `&mut dyn Writer`.
fn last_arg_is_writer(item: &syn::ItemFn) -> bool {
    let Some(syn::FnArg::Typed(arg)) = item.sig.inputs.last() else { return false };
    let syn::Type::Reference(syn::TypeReference { elem, .. }) = arg.ty.as_ref() else { return false };
    let syn::Type::TraitObject(syn::TypeTraitObject { bounds, .. }) = elem.as_ref() else { return false };
    let Some(syn::TypeParamBound::Trait(syn::TraitBound { path, .. })) = bounds.first() else { return false };
    path.segments.last().map_or(false, |s| s.ident == "Writer")
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
fn return_value_is_result(item: &syn::ItemFn) -> bool {
    let syn::ReturnType::Type(_, ty) = &item.sig.output else { return false };
    let syn::Type::Path(path) = ty.as_ref() else { return false };
    let Some(seg) = path.path.segments.last() else { return false };
    seg.ident == "Result"
}
