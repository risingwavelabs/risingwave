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

//! Generate code for the functions.

use itertools::Itertools;
use proc_macro2::Span;
use quote::{format_ident, quote};

use super::*;

impl FunctionAttr {
    /// Expands the wildcard in function arguments or return type.
    pub fn expand(&self) -> Vec<Self> {
        let args = self.args.iter().map(|ty| types::expand_type_wildcard(ty));
        let ret = types::expand_type_wildcard(&self.ret);
        // multi_cartesian_product should emit an empty set if the input is empty.
        let args_cartesian_product =
            args.multi_cartesian_product()
                .chain(match self.args.is_empty() {
                    true => vec![vec![]],
                    false => vec![],
                });
        let mut attrs = Vec::new();
        for (args, mut ret) in args_cartesian_product.cartesian_product(ret) {
            if ret == "auto" {
                ret = types::min_compatible_type(&args);
            }
            let attr = FunctionAttr {
                name: self.name.clone(),
                args: args.iter().map(|s| s.to_string()).collect(),
                ret: ret.to_string(),
                batch: self.batch.clone(),
                user_fn: self.user_fn.clone(),
            };
            attrs.push(attr);
        }
        attrs
    }

    /// Generate a descriptor of the function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_descriptor(&self, build_fn: bool) -> Result<TokenStream2> {
        let name = self.name.clone();
        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type(ty));
        }
        let ret = data_type(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}_{}_{}", self.name, self.args.join("_"), self.ret);
        let descriptor_type = quote! { crate::sig::func::FuncSign };
        let build_fn = if build_fn {
            let name = format_ident!("{}", self.user_fn.name);
            quote! { #name }
        } else {
            self.generate_build_fn()?
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                unsafe { crate::sig::func::_register(#descriptor_type {
                    func: risingwave_pb::expr::expr_node::Type::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                }) };
            }
        })
    }

    fn generate_build_fn(&self) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let fn_name = format_ident!("{}", self.user_fn.name);
        let arg_arrays = self
            .args
            .iter()
            .map(|t| format_ident!("{}", types::array_type(t)));
        let ret_array = format_ident!("{}", types::array_type(&self.ret));
        let arg_types = self
            .args
            .iter()
            .map(|t| types::ref_type(t).parse::<TokenStream2>().unwrap());
        let ret_type = types::ref_type(&self.ret).parse::<TokenStream2>().unwrap();
        let exprs = (0..num_args)
            .map(|i| format_ident!("e{i}"))
            .collect::<Vec<_>>();
        #[expect(
            clippy::redundant_clone,
            reason = "false positive https://github.com/rust-lang/rust-clippy/issues/10545"
        )]
        let exprs0 = exprs.clone();

        let build_expr = if self.ret == "varchar" && self.user_fn.is_writer_style() {
            let template_struct = match num_args {
                1 => format_ident!("UnaryBytesExpression"),
                2 => format_ident!("BinaryBytesExpression"),
                3 => format_ident!("TernaryBytesExpression"),
                4 => format_ident!("QuaternaryBytesExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let args = (0..=num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let func = match self.user_fn.return_type {
                ReturnType::T => quote! { Ok(#fn_name(#(#args1),*)) },
                ReturnType::Result => quote! { #fn_name(#(#args1),*) },
                _ => todo!("returning Option is not supported yet"),
            };
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays),*, _>::new(
                    #(#exprs),*,
                    return_type,
                    |#(#args),*| #func,
                )))
            }
        } else if self.args.iter().all(|t| t == "boolean")
            && self.ret == "boolean"
            && !self.user_fn.return_type.contains_result()
            && self.batch.is_some()
        {
            let template_struct = match num_args {
                1 => format_ident!("BooleanUnaryExpression"),
                2 => format_ident!("BooleanBinaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let batch = format_ident!("{}", self.batch.as_ref().unwrap());
            let args = (0..num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let func = if self.user_fn.arg_option && self.user_fn.return_type == ReturnType::Option
            {
                quote! { #fn_name(#(#args1),*) }
            } else if self.user_fn.arg_option {
                quote! { Some(#fn_name(#(#args1),*)) }
            } else {
                let args2 = args.clone();
                let args3 = args.clone();
                quote! {
                    match (#(#args1),*) {
                        (#(Some(#args2)),*) => Some(#fn_name(#(#args3),*)),
                        _ => None,
                    }
                }
            };
            quote! {
                Ok(Box::new(crate::expr::template_fast::#template_struct::new(
                    #(#exprs,)*
                    #batch,
                    |#(#args),*| #func,
                )))
            }
        } else if self.args.len() == 2 && self.ret == "boolean" && self.user_fn.is_pure() {
            let compatible_type = types::ref_type(types::min_compatible_type(&self.args))
                .parse::<TokenStream2>()
                .unwrap();
            let args = (0..num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let generic = if self.user_fn.generic == 3 {
                // XXX: for generic compare functions, we need to specify the compatible type
                quote! { ::<_, _, #compatible_type> }
            } else {
                quote! {}
            };
            quote! {
                Ok(Box::new(crate::expr::template_fast::CompareExpression::<_, #(#arg_arrays),*>::new(
                    #(#exprs,)*
                    |#(#args),*| #fn_name #generic(#(#args1),*),
                )))
            }
        } else if self.args.iter().all(|t| types::is_primitive(t)) && self.user_fn.is_pure() {
            let template_struct = match num_args {
                0 => format_ident!("NullaryExpression"),
                1 => format_ident!("UnaryExpression"),
                2 => format_ident!("BinaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            quote! {
                Ok(Box::new(crate::expr::template_fast::#template_struct::<_, #(#arg_types,)* #ret_type>::new(
                    #(#exprs,)*
                    return_type,
                    #fn_name,
                )))
            }
        } else if self.user_fn.arg_option || self.user_fn.return_type.contains_option() {
            let template_struct = match num_args {
                1 => format_ident!("UnaryNullableExpression"),
                2 => format_ident!("BinaryNullableExpression"),
                3 => format_ident!("TernaryNullableExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let args = (0..num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let generic = if self.user_fn.generic == 3 {
                // XXX: for generic compare functions, we need to specify the compatible type
                let compatible_type = types::ref_type(types::min_compatible_type(&self.args))
                    .parse::<TokenStream2>()
                    .unwrap();
                quote! { ::<_, _, #compatible_type> }
            } else {
                quote! {}
            };
            let mut func = quote! { #fn_name #generic(#(#args1),*) };
            func = match self.user_fn.return_type {
                ReturnType::T => quote! { Ok(Some(#func)) },
                ReturnType::Option => quote! { Ok(#func) },
                ReturnType::Result => quote! { #func.map(Some) },
                ReturnType::ResultOption => quote! { #func },
            };
            if !self.user_fn.arg_option {
                let args2 = args.clone();
                let args3 = args.clone();
                func = quote! {
                    match (#(#args2),*) {
                        (#(Some(#args3)),*) => #func,
                        _ => Ok(None),
                    }
                };
            };
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays,)* #ret_array, _>::new(
                    #(#exprs,)*
                    return_type,
                    |#(#args),*| #func,
                )))
            }
        } else {
            let template_struct = match num_args {
                0 => format_ident!("NullaryExpression"),
                1 => format_ident!("UnaryExpression"),
                2 => format_ident!("BinaryExpression"),
                3 => format_ident!("TernaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let args = (0..num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let func = match self.user_fn.return_type {
                ReturnType::T => quote! { Ok(#fn_name(#(#args1),*)) },
                ReturnType::Result => quote! { #fn_name(#(#args1),*) },
                _ => panic!("return type should not contain Option"),
            };
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays,)* #ret_array, _>::new(
                    #(#exprs,)*
                    return_type,
                    |#(#args),*| #func,
                )))
            }
        };
        Ok(quote! {
            |return_type, children| {
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_pb::expr::expr_node::RexNode;

                crate::ensure!(children.len() == #num_args);
                let mut iter = children.into_iter();
                #(let #exprs0 = iter.next().unwrap();)*

                #build_expr
            }
        })
    }

    /// Generate a descriptor of the aggregate function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_agg_descriptor(&self, build_fn: bool) -> Result<TokenStream2> {
        let name = self.name.clone();

        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type(ty));
        }
        let ret = data_type(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}_{}_{}", self.name, self.args.join("_"), self.ret);
        let descriptor_type = quote! { crate::sig::agg::AggFuncSig };
        let build_fn = if build_fn {
            let name = format_ident!("{}", self.user_fn.name);
            quote! { #name }
        } else {
            self.generate_agg_build_fn()?
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                unsafe { crate::sig::agg::_register(#descriptor_type {
                    func: risingwave_pb::expr::agg_call::PbType::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                }) };
            }
        })
    }

    /// Generate build function for aggregate function.
    fn generate_agg_build_fn(&self) -> Result<TokenStream2> {
        let arg_variant: TokenStream2 = types::variant(&self.args[0]).parse().unwrap();
        let arg_owned: TokenStream2 = types::owned_type(&self.args[0]).parse().unwrap();
        let arg_ref: TokenStream2 = types::ref_type(&self.args[0]).parse().unwrap();
        let ret_variant: TokenStream2 = types::variant(&self.ret).parse().unwrap();
        let state_type: TokenStream2 = types::owned_type(&self.ret).parse().unwrap();
        let fn_name = format_ident!("{}", self.user_fn.name);
        let mut next_state = quote! { #fn_name(state, value) };
        next_state = match self.user_fn.return_type {
            ReturnType::T => quote! { Some(#next_state) },
            ReturnType::Option => next_state,
            ReturnType::Result => quote! { Some(#next_state?) },
            ReturnType::ResultOption => quote! { #next_state? },
        };
        if !self.user_fn.arg_option {
            next_state = quote! {
                match (state, value) {
                    (Some(state), Some(value)) => #next_state,
                    (None, Some(value)) => Some(value.into()),
                    (state, None) => state,
                }
            };
        }

        Ok(quote! {
            |agg| {
                use std::collections::HashSet;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::bail;

                #[derive(Clone)]
                struct Agg<D: MaybeDistinct> {
                    return_type: DataType,
                    state: Option<#state_type>,
                    distinct: D,
                }

                trait MaybeDistinct: Clone + Send + 'static {
                    fn insert(&mut self, value: #arg_ref) -> bool;
                }
                impl MaybeDistinct for () {
                    fn insert(&mut self, _value: #arg_ref) -> bool {
                        true
                    }
                }
                impl MaybeDistinct for HashSet<#arg_owned> {
                    fn insert(&mut self, value: #arg_ref) -> bool {
                        HashSet::insert(self, value.to_owned_scalar())
                    }
                }

                #[async_trait::async_trait]
                impl<D: MaybeDistinct> crate::agg::Aggregator for Agg<D> {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }
                    async fn update_multi(
                        &mut self,
                        input: &DataChunk,
                        start_row_id: usize,
                        end_row_id: usize,
                    ) -> Result<()> {
                        let ArrayImpl::#arg_variant(input) = input.column_at(0).array_ref() else {
                            bail!("input type mismatch. expect: {}", stringify!(#arg_variant));
                        };
                        let mut state = self.state.as_ref().map(|x| x.as_scalar_ref());
                        for row_id in start_row_id..end_row_id {
                            let value = input.value_at(row_id);
                            if let Some(v) = value && !self.distinct.insert(v) {
                                continue;
                            }
                            state = #next_state;
                        }
                        self.state = state.map(|x| x.to_owned_scalar());
                        Ok(())
                    }
                    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                        let ArrayBuilderImpl::#ret_variant(builder) = builder else {
                            bail!("output type mismatch. expect: {}", stringify!(#ret_variant));
                        };
                        let res = self.state.take();
                        builder.append(res.as_ref().map(|x| x.as_scalar_ref()));
                        Ok(())
                    }
                }

                if agg.distinct {
                    Ok(Box::new(Agg {
                        return_type: agg.return_type,
                        state: None,
                        distinct: HashSet::<#arg_owned>::new(),
                    }))
                } else {
                    Ok(Box::new(Agg {
                        return_type: agg.return_type,
                        state: None,
                        distinct: (),
                    }))
                }
            }
        })
    }
}

fn data_type(ty: &str) -> TokenStream2 {
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { risingwave_common::types::DataTypeName::#variant }
}
