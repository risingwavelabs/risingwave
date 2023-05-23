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
                args: args.iter().map(|s| s.to_string()).collect(),
                ret: ret.to_string(),
                ..self.clone()
            };
            attrs.push(attr);
        }
        attrs
    }

    /// Generate a descriptor of the function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_descriptor(&self, build_fn: bool) -> Result<TokenStream2> {
        if self.is_table_function {
            return self.generate_table_function_descriptor(build_fn);
        }
        let name = self.name.clone();
        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
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
            && self.batch_fn.is_some()
        {
            let template_struct = match num_args {
                1 => format_ident!("BooleanUnaryExpression"),
                2 => format_ident!("BooleanBinaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let batch_fn = format_ident!("{}", self.batch_fn.as_ref().unwrap());
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
                    #batch_fn,
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
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
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
                    func: crate::agg::AggKind::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                }) };
            }
        })
    }

    /// Generate build function for aggregate function.
    fn generate_agg_build_fn(&self) -> Result<TokenStream2> {
        let ret_variant: TokenStream2 = types::variant(&self.ret).parse().unwrap();
        let ret_owned: TokenStream2 = types::owned_type(&self.ret).parse().unwrap();
        let state_type: TokenStream2 = match &self.state {
            Some(state) => state.parse().unwrap(),
            None => types::owned_type(&self.ret).parse().unwrap(),
        };
        let args = (0..self.args.len()).map(|i| format_ident!("v{i}"));
        let args = quote! { #(#args),* };
        let let_arrays = self.args.iter().enumerate().map(|(i, arg)| {
            let array = format_ident!("a{i}");
            let variant: TokenStream2 = types::variant(arg).parse().unwrap();
            quote! {
                let ArrayImpl::#variant(#array) = &**input.column_at(#i) else {
                    bail!("input type mismatch. expect: {}", stringify!(#variant));
                };
            }
        });
        let let_values = (0..self.args.len()).map(|i| {
            let v = format_ident!("v{i}");
            let a = format_ident!("a{i}");
            quote! { let #v = #a.value_at(row_id); }
        });
        let let_state = match &self.state {
            Some(_) => quote! { self.state.take() },
            None => quote! { self.state.as_ref().map(|x| x.as_scalar_ref()) },
        };
        let assign_state = match &self.state {
            Some(_) => quote! { state },
            None => quote! { state.map(|x| x.to_owned_scalar()) },
        };
        let init_state = match &self.init_state {
            Some(s) => s.parse().unwrap(),
            _ => quote! { None },
        };
        let fn_name = format_ident!("{}", self.user_fn.name);
        let mut next_state = quote! { #fn_name(state, #args) };
        next_state = match self.user_fn.return_type {
            ReturnType::T => quote! { Some(#next_state) },
            ReturnType::Option => next_state,
            ReturnType::Result => quote! { Some(#next_state?) },
            ReturnType::ResultOption => quote! { #next_state? },
        };
        if !self.user_fn.arg_option {
            if self.args.len() > 1 {
                todo!("multiple arguments are not supported for non-option function");
            }
            let first_state = match &self.init_state {
                Some(_) => quote! { unreachable!() },
                _ => quote! { Some(v0.into()) },
            };
            next_state = quote! {
                match (state, v0) {
                    (Some(state), Some(v0)) => #next_state,
                    (None, Some(v0)) => #first_state,
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
                use risingwave_common::buffer::Bitmap;
                use crate::Result;

                #[derive(Clone)]
                struct Agg {
                    return_type: DataType,
                    state: Option<#state_type>,
                }

                #[async_trait::async_trait]
                impl crate::agg::Aggregator for Agg {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }
                    async fn update_multi(
                        &mut self,
                        input: &DataChunk,
                        start_row_id: usize,
                        end_row_id: usize,
                    ) -> Result<()> {
                        #(#let_arrays)*
                        let mut state = #let_state;
                        for row_id in start_row_id..end_row_id {
                            if !input.vis().is_set(row_id) {
                                continue;
                            }
                            #(#let_values)*
                            state = #next_state;
                        }
                        self.state = #assign_state;
                        Ok(())
                    }
                    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                        let ArrayBuilderImpl::#ret_variant(builder) = builder else {
                            bail!("output type mismatch. expect: {}", stringify!(#ret_variant));
                        };
                        match std::mem::replace(&mut self.state, #init_state) {
                            Some(state) => builder.append(Some(<#ret_owned>::from(state).as_scalar_ref())),
                            None => builder.append_null(),
                        }
                        Ok(())
                    }
                }

                Ok(Box::new(Agg {
                    return_type: agg.return_type,
                    state: #init_state,
                }))
            }
        })
    }

    /// Generate a descriptor of the table function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    fn generate_table_function_descriptor(&self, build_fn: bool) -> Result<TokenStream2> {
        let name = self.name.clone();
        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let descriptor_type = quote! { crate::sig::table_function::FuncSign };
        let build_fn = if build_fn {
            let name = format_ident!("{}", self.user_fn.name);
            quote! { #name }
        } else {
            self.generate_build_table_function()?
        };
        let type_infer_fn = if let Some(func) = &self.type_infer {
            func.parse().unwrap()
        } else {
            if matches!(self.ret.as_str(), "any" | "list" | "struct") {
                return Err(Error::new(
                    Span::call_site(),
                    format!("type inference function is required for {}", self.ret),
                ));
            }
            let ty = data_type(&self.ret);
            quote! { |_| Ok(#ty) }
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                unsafe { crate::sig::table_function::_register(#descriptor_type {
                    func: risingwave_pb::expr::table_function::Type::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                    type_infer: #type_infer_fn,
                }) };
            }
        })
    }

    fn generate_build_table_function(&self) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let fn_name = format_ident!("{}", self.user_fn.name);
        let struct_name = format_ident!("{}", self.ident_name());
        let ids = (0..num_args)
            .filter(|i| match &self.prebuild {
                Some(s) => !s.contains(&format!("${i}")),
                None => true,
            })
            .collect_vec();
        let const_ids = (0..num_args).filter(|i| match &self.prebuild {
            Some(s) => s.contains(&format!("${i}")),
            None => false,
        });
        let elems: Vec<_> = ids.iter().map(|i| format_ident!("v{i}")).collect();
        let all_child: Vec<_> = (0..num_args).map(|i| format_ident!("child{i}")).collect();
        let const_child: Vec<_> = const_ids.map(|i| format_ident!("child{i}")).collect();
        let child: Vec<_> = ids.iter().map(|i| format_ident!("child{i}")).collect();
        let array_refs: Vec<_> = ids.iter().map(|i| format_ident!("array{i}")).collect();
        let arrays: Vec<_> = ids.iter().map(|i| format_ident!("a{i}")).collect();
        let arg_arrays = self
            .args
            .iter()
            .map(|t| format_ident!("{}", types::array_type(t)));
        let array_builder = format_ident!("{}Builder", types::array_type(&self.ret));
        let const_arg = match &self.prebuild {
            Some(_) => quote! { &self.const_arg },
            None => quote! {},
        };
        let const_arg_type = match &self.prebuild {
            Some(s) => s.split("::").next().unwrap().parse().unwrap(),
            None => quote! { () },
        };
        let const_arg_value = match &self.prebuild {
            Some(s) => s
                .replace('$', "child")
                .parse()
                .expect("invalid prebuild syntax"),
            None => quote! { () },
        };
        let value = match self.user_fn.return_type {
            ReturnType::T => quote! { Some(value) },
            ReturnType::Option => quote! { value },
            ReturnType::Result => quote! { Some(value?) },
            ReturnType::ResultOption => quote! { value? },
        };

        Ok(quote! {
            |return_type, chunk_size, children| {
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::util::iter_util::ZipEqFast;
                use itertools::multizip;

                crate::ensure!(children.len() == #num_args);
                let mut iter = children.into_iter();
                #(let #all_child = iter.next().unwrap();)*
                #(let #const_child = #const_child.eval_const()?;)*

                #[derive(Debug)]
                struct #struct_name {
                    return_type: DataType,
                    chunk_size: usize,
                    #(#child: BoxedExpression,)*
                    const_arg: #const_arg_type,
                }
                #[async_trait::async_trait]
                impl crate::table_function::TableFunction for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }
                    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
                        self.eval_inner(input)
                    }
                }
                impl #struct_name {
                    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
                    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
                        #(
                        let #array_refs = self.#child.eval_checked(input).await?;
                        let #arrays: &#arg_arrays = #array_refs.as_ref().into();
                        )*

                        let mut index_builder = I64ArrayBuilder::new(self.chunk_size);
                        let mut value_builder = #array_builder::with_type(self.chunk_size, self.return_type.clone());

                        for (i, (row, visible)) in multizip((#(#arrays.iter(),)*)).zip_eq_fast(input.vis().iter()).enumerate() {
                            if let (#(Some(#elems),)*) = row && visible {
                                for value in #fn_name(#(#elems,)* #const_arg) {
                                    index_builder.append(Some(i as i64));
                                    match #value {
                                        Some(v) => value_builder.append(Some(v.as_scalar_ref())),
                                        None => value_builder.append_null(),
                                    }

                                    if index_builder.len() == self.chunk_size {
                                        let index_array = std::mem::replace(&mut index_builder, I64ArrayBuilder::new(self.chunk_size)).finish();
                                        let value_array = std::mem::replace(&mut value_builder, #array_builder::with_type(self.chunk_size, self.return_type.clone())).finish();
                                        yield DataChunk::new(vec![index_array.into_ref(), value_array.into_ref()], self.chunk_size);
                                    }
                                }
                            }
                        }

                        if index_builder.len() > 0 {
                            let len = index_builder.len();
                            let index_array = index_builder.finish();
                            let value_array = value_builder.finish();
                            yield DataChunk::new(vec![index_array.into_ref(), value_array.into_ref()], len);
                        }
                    }
                }

                Ok(Box::new(#struct_name {
                    return_type,
                    chunk_size,
                    #(#child,)*
                    const_arg: #const_arg_value,
                }))
            }
        })
    }
}

fn data_type_name(ty: &str) -> TokenStream2 {
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { risingwave_common::types::DataTypeName::#variant }
}

fn data_type(ty: &str) -> TokenStream2 {
    if let Some(ty) = ty.strip_suffix("[]") {
        let inner_type = data_type(ty);
        return quote! { risingwave_common::types::DataType::List(Box::new(#inner_type)) };
    }
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { risingwave_common::types::DataType::#variant }
}
