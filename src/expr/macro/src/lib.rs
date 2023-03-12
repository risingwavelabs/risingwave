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
        for (args, mut ret) in args.multi_cartesian_product().cartesian_product(ret) {
            if ret == "auto" {
                ret = sql_type::min_compatible_type(&args);
            }
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
        let ret = to_data_type_name(&self.ret)?;

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let descriptor_name = format_ident!("{}_{}_{}", self.name, self.args.join("_"), self.ret);
        let descriptor_type = quote! { crate::sig::func::FunctionDescriptor };
        let build_fn = self.generate_build_fn()?;
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

    fn generate_build_fn(&self) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let i = 0..self.args.len();
        let fn_name = format_ident!("{}", self.user_fn.name);
        let arg_arrays = self
            .args
            .iter()
            .map(|t| format_ident!("{}", sql_type::to_array_type(t)));
        let ret_array = format_ident!("{}", sql_type::to_array_type(&self.ret));
        let arg_types = self
            .args
            .iter()
            .map(|t| sql_type::to_data_type(t).parse::<TokenStream2>().unwrap());
        let ret_type = sql_type::to_data_type(&self.ret)
            .parse::<TokenStream2>()
            .unwrap();

        let prepare = quote! {
            use risingwave_common::array::*;
            use risingwave_common::types::*;
            use risingwave_pb::expr::expr_node::RexNode;

            let return_type = DataType::from(prost.get_return_type().unwrap());
            let RexNode::FuncCall(func_call) = prost.get_rex_node().unwrap() else {
                crate::bail!("Expected RexNode::FuncCall");
            };
            let children = func_call.get_children();
            crate::ensure!(children.len() == #num_args);
            let exprs = [#(crate::expr::build_from_prost(&children[#i])?),*];
        };

        let build_expr = if self.ret == "varchar" && self.user_fn.is_writer_style() {
            let template_struct = match num_args {
                1 => format_ident!("UnaryBytesExpression"),
                2 => format_ident!("BinaryBytesExpression"),
                3 => format_ident!("TernaryBytesExpression"),
                4 => format_ident!("QuaternaryBytesExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let i = 0..self.args.len();
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays),*, _>::new(
                    #(exprs[#i]),*,
                    return_type,
                    #fn_name,
                )))
            }
        } else if self.args.iter().all(|t| t == "boolean")
            && self.ret == "boolean"
            && !self.user_fn.return_result
            && self.batch.is_some()
        {
            let template_struct = match num_args {
                1 => format_ident!("BooleanUnaryExpression"),
                2 => format_ident!("BooleanBinaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let batch = format_ident!("{}", self.batch.as_ref().unwrap());
            let i = 0..self.args.len();
            let func = if self.user_fn.arg_option && self.user_fn.return_option {
                quote! { #fn_name }
            } else if self.user_fn.arg_option {
                let args = (0..num_args).map(|i| format_ident!("x{i}"));
                let args1 = args.clone();
                quote! { |#(#args),*| Some(#fn_name(#(#args1),*)) }
            } else {
                let args = (0..num_args).map(|i| format_ident!("x{i}"));
                let args1 = args.clone();
                let args2 = args.clone();
                let args3 = args.clone();
                quote! {
                    |#(#args),*| match (#(#args1),*) {
                        (#(Some(#args2)),*) => Some(#fn_name(#(#args3),*)),
                        _ => None,
                    }
                }
            };
            quote! {
                Ok(Box::new(crate::expr::template_fast::#template_struct::new(
                    #(exprs[#i]),*, #batch, #func,
                )))
            }
        } else if self.args.len() == 2 && self.ret == "boolean" && self.user_fn.is_pure() {
            let compatible_type = sql_type::to_data_type(sql_type::min_compatible_type(&self.args))
                .parse::<TokenStream2>()
                .unwrap();
            quote! {
                Ok(Box::new(crate::expr::template_fast::CompareExpression::<_, #(#arg_arrays),*>::new(
                    exprs[0], exprs[1], #fn_name::<#(#arg_types),*, #compatible_type>,
                )))
            }
        } else if self.args.iter().all(|t| sql_type::is_primitive(t)) && self.user_fn.is_pure() {
            let template_struct = match num_args {
                1 => format_ident!("UnaryExpression"),
                2 => format_ident!("BinaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let i = 0..self.args.len();
            quote! {
                Ok(Box::new(crate::expr::template_fast::#template_struct::<_, #(#arg_types),*, #ret_type>::new(
                    #(exprs[#i]),*,
                    return_type,
                    #fn_name,
                )))
            }
        } else if self.user_fn.arg_option {
            let template_struct = match num_args {
                1 => format_ident!("UnaryNullableExpression"),
                2 => format_ident!("BinaryNullableExpression"),
                3 => format_ident!("TernaryNullableExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let i = 0..self.args.len();
            let func = if self.user_fn.return_result {
                quote! { #fn_name }
            } else if self.user_fn.return_option {
                let args = (0..num_args).map(|i| format_ident!("x{i}"));
                let args1 = args.clone();
                quote! { |#(#args),*| Ok(#fn_name(#(#args1),*)) }
            } else {
                let args = (0..num_args).map(|i| format_ident!("x{i}"));
                let args1 = args.clone();
                quote! { |#(#args),*| Ok(Some(#fn_name(#(#args1),*))) }
            };
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays),*, #ret_array, _>::new(
                    #(exprs[#i]),*,
                    return_type,
                    #func,
                )))
            }
        } else {
            let template_struct = match num_args {
                1 => format_ident!("UnaryExpression"),
                2 => format_ident!("BinaryExpression"),
                3 => format_ident!("TernaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let i = 0..self.args.len();
            let func = if self.user_fn.return_result {
                quote! { #fn_name }
            } else {
                let args = (0..num_args).map(|i| format_ident!("x{i}"));
                let args1 = args.clone();
                quote! { |#(#args),*| Ok(#fn_name(#(#args1),*)) }
            };
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays),*, #ret_array, _>::new(
                    #(exprs[#i]),*,
                    return_type,
                    #func,
                )))
            }
        };
        Ok(quote! {
            |prost| {
                #prepare
                #build_expr
            }
        })
    }
}

#[derive(Debug, Clone)]
struct UserFunctionAttr {
    /// Function name
    name: String,
    /// The last argument type is `&mut dyn Write`.
    write: bool,
    /// The argument type are `Option`s.
    arg_option: bool,
    /// The return type is `Option`.
    return_option: bool,
    /// The return type is `Result`.
    return_result: bool,
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

    fn is_writer_style(&self) -> bool {
        self.write && !self.arg_option && self.return_result
    }

    fn is_pure(&self) -> bool {
        !self.write && !self.arg_option && !self.return_option && !self.return_result
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
