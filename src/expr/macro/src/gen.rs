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
use proc_macro2::{Ident, Span};
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

    /// Generate a descriptor of the scalar or table function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_function_descriptor(
        &self,
        user_fn: &UserFunctionAttr,
        build_fn: bool,
    ) -> Result<TokenStream2> {
        if self.is_table_function {
            return self.generate_table_function_descriptor(user_fn, build_fn);
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
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else if self.args.len() >= 1 && &self.args[0] == "..." {
            self.generate_build_varargs_scalar_function(user_fn)?
        } else {
            self.generate_build_scalar_function(user_fn, true)?
        };
        let deprecated = self.deprecated;
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { crate::sig::func::_register(#descriptor_type {
                    func: risingwave_pb::expr::expr_node::Type::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                    deprecated: #deprecated,
                }) };
            }
        })
    }

    /// Generate a build function for the scalar function.
    ///
    /// If `optimize_const` is true, the function will be optimized for constant arguments,
    /// and fallback to the general version if any argument is not constant.
    fn generate_build_scalar_function(
        &self,
        user_fn: &UserFunctionAttr,
        optimize_const: bool,
    ) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let fn_name = format_ident!("{}", user_fn.name);
        let struct_name = match optimize_const {
            true => format_ident!("{}OptimizeConst", utils::to_camel_case(&self.ident_name())),
            false => format_ident!("{}", utils::to_camel_case(&self.ident_name())),
        };

        // we divide all arguments into two groups: prebuilt and non-prebuilt.
        // prebuilt arguments are collected from the "prebuild" field.
        // let's say we have a function with 3 arguments: [0, 1, 2]
        // and the prebuild field contains "$1".
        // then we have:
        //     prebuilt_indices = [1]
        //     non_prebuilt_indices = [0, 2]
        //
        // if the const argument optimization is enabled, prebuilt arguments are
        // evaluated at build time, thus the children only contain non-prebuilt arguments:
        //     children_indices = [0, 2]
        // otherwise, the children contain all arguments:
        //     children_indices = [0, 1, 2]

        let prebuilt_indices = match &self.prebuild {
            Some(s) => (0..num_args)
                .filter(|i| s.contains(&format!("${i}")))
                .collect_vec(),
            None => vec![],
        };
        let non_prebuilt_indices = match &self.prebuild {
            Some(s) => (0..num_args)
                .filter(|i| !s.contains(&format!("${i}")))
                .collect_vec(),
            _ => (0..num_args).collect_vec(),
        };
        let children_indices = match optimize_const {
            #[allow(clippy::redundant_clone)] // false-positive
            true => non_prebuilt_indices.clone(),
            false => (0..num_args).collect_vec(),
        };

        /// Return a list of identifiers with the given prefix and indices.
        fn idents(prefix: &str, indices: &[usize]) -> Vec<Ident> {
            indices
                .iter()
                .map(|i| format_ident!("{prefix}{i}"))
                .collect()
        }
        let inputs = idents("i", &children_indices);
        let prebuilt_inputs = idents("i", &prebuilt_indices);
        let non_prebuilt_inputs = idents("i", &non_prebuilt_indices);
        let all_child = idents("child", &(0..num_args).collect_vec());
        let child = idents("child", &children_indices);
        let array_refs = idents("array", &children_indices);
        let arrays = idents("a", &children_indices);
        let datums = idents("v", &children_indices);
        let arg_arrays = children_indices
            .iter()
            .map(|i| format_ident!("{}", types::array_type(&self.args[*i])));
        let arg_types = children_indices.iter().map(|i| {
            types::ref_type(&self.args[*i])
                .parse::<TokenStream2>()
                .unwrap()
        });
        let annotation: TokenStream2 = match user_fn.core_return_type.as_str() {
            // add type annotation for functions that return generic types
            "T" | "T1" | "T2" | "T3" => format!(": Option<{}>", types::owned_type(&self.ret))
                .parse()
                .unwrap(),
            _ => quote! {},
        };
        let ret_array_type = format_ident!("{}", types::array_type(&self.ret));
        let builder_type = format_ident!("{}Builder", types::array_type(&self.ret));
        let prebuilt_arg_type = match &self.prebuild {
            Some(s) if optimize_const => s.split("::").next().unwrap().parse().unwrap(),
            _ => quote! { () },
        };
        let prebuilt_arg_value = match &self.prebuild {
            // example:
            // prebuild = "RegexContext::new($1)"
            // return = "RegexContext::new(i1)"
            Some(s) => s
                .replace('$', "i")
                .parse()
                .expect("invalid prebuild syntax"),
            None => quote! { () },
        };
        let prebuild_const = if self.prebuild.is_some() && optimize_const {
            let build_general = self.generate_build_scalar_function(user_fn, false)?;
            quote! {{
                let build_general = #build_general;
                #(
                    // try to evaluate constant for prebuilt arguments
                    let #prebuilt_inputs = match children[#prebuilt_indices].eval_const() {
                        Ok(s) => s,
                        // prebuilt argument is not constant, fallback to general
                        Err(_) => return build_general(return_type, children),
                    };
                    // get reference to the constant value
                    let #prebuilt_inputs = match &#prebuilt_inputs {
                        Some(s) => s.as_scalar_ref_impl().try_into()?,
                        // the function should always return null if any const argument is null
                        None => return Ok(Box::new(crate::expr::LiteralExpression::new(
                            return_type,
                            None,
                        ))),
                    };
                )*
                #prebuilt_arg_value
            }}
        } else {
            quote! { () }
        };
        let generic = if self.ret == "boolean" && user_fn.generic == 3 {
            // XXX: for generic compare functions, we need to specify the compatible type
            let compatible_type = types::ref_type(types::min_compatible_type(&self.args))
                .parse::<TokenStream2>()
                .unwrap();
            quote! { ::<_, _, #compatible_type> }
        } else {
            quote! {}
        };
        let prebuilt_arg = match (&self.prebuild, optimize_const) {
            // use the prebuilt argument
            (Some(_), true) => quote! { &self.prebuilt_arg, },
            // build the argument on site
            (Some(_), false) => quote! { &#prebuilt_arg_value, },
            // no prebuilt argument
            (None, _) => quote! {},
        };
        let context = match user_fn.context {
            true => quote! { &self.context, },
            false => quote! {},
        };
        let writer = match user_fn.write {
            true => quote! { &mut writer, },
            false => quote! {},
        };
        // call the user defined function
        // inputs: [ Option<impl ScalarRef> ]
        let mut output =
            quote! { #fn_name #generic(#(#non_prebuilt_inputs,)* #prebuilt_arg #context #writer) };
        output = match user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { Some(#output) },
            ReturnTypeKind::Option => output,
            ReturnTypeKind::Result => quote! { Some(#output?) },
            ReturnTypeKind::ResultOption => quote! { #output? },
        };
        // if user function accepts non-option arguments, we assume the function
        // returns null on null input, so we need to unwrap the inputs before calling.
        if !user_fn.arg_option {
            output = quote! {
                match (#(#inputs,)*) {
                    (#(Some(#inputs),)*) => #output,
                    _ => None,
                }
            };
        };
        // output: Option<impl ScalarRef or Scalar>
        let append_output = match user_fn.write {
            true => quote! {{
                let mut writer = builder.writer().begin();
                if #output.is_some() {
                    writer.finish();
                } else {
                    drop(writer);
                    builder.append_null();
                }
            }},
            false if user_fn.core_return_type == "impl AsRef < [u8] >" => quote! {
                builder.append(#output.as_ref().map(|s| s.as_ref()));
            },
            false => quote! {
                let output #annotation = #output;
                builder.append(output.as_ref().map(|s| s.as_scalar_ref()));
            },
        };
        // the output expression in `eval_row`
        let row_output = match user_fn.write {
            true => quote! {{
                let mut writer = String::new();
                #output.map(|_| writer.into())
            }},
            false if user_fn.core_return_type == "impl AsRef < [u8] >" => quote! {
                #output.map(|s| s.as_ref().into())
            },
            false => quote! {{
                let output #annotation = #output;
                output.map(|s| s.into())
            }},
        };
        // the main body in `eval`
        let eval = if let Some(batch_fn) = &self.batch_fn {
            // user defined batch function
            let fn_name = format_ident!("{}", batch_fn);
            quote! {
                let c = #fn_name(#(#arrays),*);
                Ok(Arc::new(c.into()))
            }
        } else if (types::is_primitive(&self.ret) || self.ret == "boolean") && user_fn.is_pure() {
            // SIMD optimization for primitive types
            match self.args.len() {
                0 => quote! {
                    let c = #ret_array_type::from_iter_bitmap(
                        std::iter::repeat_with(|| #fn_name()).take(input.capacity())
                        Bitmap::ones(input.capacity()),
                    );
                    Ok(Arc::new(c.into()))
                },
                1 => quote! {
                    let c = #ret_array_type::from_iter_bitmap(
                        a0.raw_iter().map(|a| #fn_name(a)),
                        a0.null_bitmap().clone()
                    );
                    Ok(Arc::new(c.into()))
                },
                2 => quote! {
                    // allow using `zip` for performance
                    #[allow(clippy::disallowed_methods)]
                    let c = #ret_array_type::from_iter_bitmap(
                        a0.raw_iter()
                            .zip(a1.raw_iter())
                            .map(|(a, b)| #fn_name #generic(a, b)),
                        a0.null_bitmap() & a1.null_bitmap(),
                    );
                    Ok(Arc::new(c.into()))
                },
                n => todo!("SIMD optimization for {n} arguments"),
            }
        } else {
            // no optimization
            let array_zip = match num_args {
                0 => quote! { std::iter::repeat(()).take(input.capacity()) },
                _ => quote! { multizip((#(#arrays.iter(),)*)) },
            };
            quote! {
                let mut builder = #builder_type::with_type(input.capacity(), self.context.return_type.clone());

                match input.vis() {
                    Vis::Bitmap(vis) => {
                        // allow using `zip` for performance
                        #[allow(clippy::disallowed_methods)]
                        for ((#(#inputs,)*), visible) in #array_zip.zip(vis.iter()) {
                            if !visible {
                                builder.append_null();
                                continue;
                            }
                            #append_output
                        }
                    }
                    Vis::Compact(_) => {
                        for (#(#inputs,)*) in #array_zip {
                            #append_output
                        }
                    }
                }
                Ok(Arc::new(builder.finish().into()))
            }
        };

        Ok(quote! {
            |return_type: DataType, children: Vec<crate::expr::BoxedExpression>|
                -> crate::Result<crate::expr::BoxedExpression>
            {
                use std::sync::Arc;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::row::OwnedRow;
                use risingwave_common::util::iter_util::ZipEqFast;
                use itertools::multizip;

                use crate::expr::{Context, BoxedExpression};
                use crate::Result;

                crate::ensure!(children.len() == #num_args);
                let prebuilt_arg = #prebuild_const;
                let context = Context {
                    return_type,
                    arg_types: children.iter().map(|c| c.return_type()).collect(),
                };
                let mut iter = children.into_iter();
                #(let #all_child = iter.next().unwrap();)*

                #[derive(Debug)]
                struct #struct_name {
                    context: Context,
                    #(#child: BoxedExpression,)*
                    prebuilt_arg: #prebuilt_arg_type,
                }
                #[async_trait::async_trait]
                impl crate::expr::Expression for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.context.return_type.clone()
                    }
                    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
                        // evaluate children and downcast arrays
                        #(
                            let #array_refs = self.#child.eval_checked(input).await?;
                            let #arrays: &#arg_arrays = #array_refs.as_ref().into();
                        )*
                        #eval
                    }
                    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
                        #(
                            let #datums = self.#child.eval_row(input).await?;
                            let #inputs: Option<#arg_types> = #datums.as_ref().map(|s| s.as_scalar_ref_impl().try_into().unwrap());
                        )*
                        Ok(#row_output)
                    }
                }

                Ok(Box::new(#struct_name {
                    context,
                    #(#child,)*
                    prebuilt_arg,
                }))
            }
        })
    }

    /// Generate a build function for variable arguments scalar function.
    fn generate_build_varargs_scalar_function(
        &self,
        user_fn: &UserFunctionAttr,
    ) -> Result<TokenStream2> {
        let fn_name = format_ident!("{}", user_fn.name);
        let struct_name = format_ident!("{}", utils::to_camel_case(&self.ident_name()));
        let builder_type = format_ident!("{}Builder", types::array_type(&self.ret));

        let mut output = quote! { #fn_name(row) };
        output = match user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { Some(#output) },
            ReturnTypeKind::Option => output,
            ReturnTypeKind::Result => quote! { Some(#output?) },
            ReturnTypeKind::ResultOption => quote! { #output? },
        };

        Ok(quote! {
            |return_type, children| {
                use std::sync::Arc;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::row::OwnedRow;

                use crate::expr::BoxedExpression;
                use crate::Result;

                #[derive(Debug)]
                struct #struct_name {
                    return_type: DataType,
                    children: Vec<BoxedExpression>,
                }
                #[async_trait::async_trait]
                impl crate::expr::Expression for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }
                    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
                        let mut columns = Vec::with_capacity(self.children.len());
                        for child in &self.children {
                            columns.push(child.eval_checked(input).await?);
                        }
                        let chunk = DataChunk::new(columns, input.vis().clone());

                        let mut builder = #builder_type::with_type(input.capacity(), self.return_type.clone());
                        for row in chunk.rows_with_holes() {
                            if let Some(row) = row {
                                builder.append(#output.as_ref().map(|s| s.as_scalar_ref()));
                            } else {
                                builder.append_null();
                            }
                        }
                        Ok(Arc::new(builder.finish().into()))
                    }
                    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
                        let mut row = Vec::with_capacity(self.children.len());
                        for child in &self.children {
                            row.push(child.eval_row(input).await?);
                        }
                        let row = OwnedRow::new(row);
                        Ok(#output.map(|s| s.into()))
                    }
                }

                Ok(Box::new(#struct_name {
                    return_type,
                    children,
                }))
            }
        })
    }

    /// Generate a descriptor of the aggregate function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_aggregate_descriptor(
        &self,
        user_fn: &UserFunctionAttr,
        build_fn: bool,
    ) -> Result<TokenStream2> {
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
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else {
            self.generate_agg_build_fn(user_fn)?
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
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
    fn generate_agg_build_fn(&self, user_fn: &UserFunctionAttr) -> Result<TokenStream2> {
        let state_type: TokenStream2 = match &self.state {
            Some(state) if state == "ref" => types::ref_type(&self.ret).parse().unwrap(),
            Some(state) if state != "ref" => state.parse().unwrap(),
            _ => types::owned_type(&self.ret).parse().unwrap(),
        };
        let let_arrays = self
            .args
            .iter()
            .enumerate()
            .map(|(i, arg)| {
                let array = format_ident!("a{i}");
                let variant: TokenStream2 = types::variant(arg).parse().unwrap();
                quote! {
                    let ArrayImpl::#variant(#array) = &**input.column_at(#i) else {
                        bail!("input type mismatch. expect: {}", stringify!(#variant));
                    };
                }
            })
            .collect_vec();
        let let_values = (0..self.args.len())
            .map(|i| {
                let v = format_ident!("v{i}");
                let a = format_ident!("a{i}");
                quote! { let #v = unsafe { #a.value_at_unchecked(row_id) }; }
            })
            .collect_vec();
        let let_state = match &self.state {
            Some(s) if s == "ref" => {
                quote! { state0.as_ref().map(|x| x.as_scalar_ref_impl().try_into().unwrap()) }
            }
            _ => quote! { state0.take().map(|s| s.try_into().unwrap()) },
        };
        let assign_state = match &self.state {
            Some(s) if s == "ref" => quote! { state.map(|x| x.to_owned_scalar().into()) },
            _ => quote! { state.map(|s| s.into()) },
        };
        let create_state = self.init_state.as_ref().map(|state| {
            let state: TokenStream2 = state.parse().unwrap();
            quote! {
                fn create_state(&self) -> AggregateState {
                    AggregateState::Datum(Some(#state.into()))
                }
            }
        });
        let fn_name = format_ident!("{}", user_fn.name);
        let args = (0..self.args.len()).map(|i| format_ident!("v{i}"));
        let args = quote! { #(#args,)* };
        let retract = match user_fn.retract {
            true => quote! { matches!(op, Op::Delete | Op::UpdateDelete) },
            false => quote! {},
        };
        let check_retract = match user_fn.retract {
            true => quote! {},
            false => {
                let msg = format!("aggregate function {} only supports append", self.name);
                quote! { assert_eq!(op, Op::Insert, #msg); }
            }
        };
        let mut next_state = quote! { #fn_name(state, #args #retract) };
        next_state = match user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { Some(#next_state) },
            ReturnTypeKind::Option => next_state,
            ReturnTypeKind::Result => quote! { Some(#next_state?) },
            ReturnTypeKind::ResultOption => quote! { #next_state? },
        };
        if !user_fn.arg_option {
            match self.args.len() {
                0 => {
                    next_state = quote! {
                        match state {
                            Some(state) => #next_state,
                            None => state,
                        }
                    };
                }
                1 => {
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
                _ => todo!("multiple arguments are not supported for non-option function"),
            }
        }

        Ok(quote! {
            |agg| {
                use std::collections::HashSet;
                use std::ops::Range;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::bail;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::estimate_size::EstimateSize;

                use crate::Result;
                use crate::agg::AggregateState;

                #[derive(Clone)]
                struct Agg {
                    return_type: DataType,
                }

                #[async_trait::async_trait]
                impl crate::agg::AggregateFunction for Agg {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }

                    #create_state

                    async fn update(&self, state0: &mut AggregateState, input: &StreamChunk) -> Result<()> {
                        #(#let_arrays)*
                        let state0 = state0.as_datum_mut();
                        let mut state: Option<#state_type> = #let_state;
                        match input.vis() {
                            Vis::Bitmap(bitmap) => {
                                for row_id in bitmap.iter_ones() {
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #check_retract
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                            Vis::Compact(_) => {
                                for row_id in 0..input.capacity() {
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #check_retract
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                        }
                        *state0 = #assign_state;
                        Ok(())
                    }

                    async fn update_range(&self, state0: &mut AggregateState, input: &StreamChunk, range: Range<usize>) -> Result<()> {
                        assert!(range.end <= input.capacity());
                        #(#let_arrays)*
                        let state0 = state0.as_datum_mut();
                        let mut state: Option<#state_type> = #let_state;
                        match input.vis() {
                            Vis::Bitmap(bitmap) => {
                                for row_id in bitmap.iter_ones() {
                                    if row_id < range.start {
                                        continue;
                                    } else if row_id >= range.end {
                                        break;
                                    }
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #check_retract
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                            Vis::Compact(_) => {
                                for row_id in range {
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #check_retract
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                        }
                        *state0 = #assign_state;
                        Ok(())
                    }

                    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
                        Ok(state.as_datum().clone())
                    }
                }

                Ok(Box::new(Agg {
                    return_type: agg.return_type.clone(),
                }))
            }
        })
    }

    /// Generate a descriptor of the table function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    fn generate_table_function_descriptor(
        &self,
        user_fn: &UserFunctionAttr,
        build_fn: bool,
    ) -> Result<TokenStream2> {
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
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else {
            self.generate_build_table_function(user_fn)?
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
                use risingwave_common::types::{DataType, DataTypeName};
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

    fn generate_build_table_function(&self, user_fn: &UserFunctionAttr) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let return_types = output_types(&self.ret);
        let fn_name = format_ident!("{}", user_fn.name);
        let struct_name = format_ident!("{}", utils::to_camel_case(&self.ident_name()));
        let arg_ids = (0..num_args)
            .filter(|i| match &self.prebuild {
                Some(s) => !s.contains(&format!("${i}")),
                None => true,
            })
            .collect_vec();
        let const_ids = (0..num_args).filter(|i| match &self.prebuild {
            Some(s) => s.contains(&format!("${i}")),
            None => false,
        });
        let inputs: Vec<_> = arg_ids.iter().map(|i| format_ident!("i{i}")).collect();
        let all_child: Vec<_> = (0..num_args).map(|i| format_ident!("child{i}")).collect();
        let const_child: Vec<_> = const_ids.map(|i| format_ident!("child{i}")).collect();
        let child: Vec<_> = arg_ids.iter().map(|i| format_ident!("child{i}")).collect();
        let array_refs: Vec<_> = arg_ids.iter().map(|i| format_ident!("array{i}")).collect();
        let arrays: Vec<_> = arg_ids.iter().map(|i| format_ident!("a{i}")).collect();
        let arg_arrays = self
            .args
            .iter()
            .map(|t| format_ident!("{}", types::array_type(t)));
        let outputs = (0..return_types.len())
            .map(|i| format_ident!("o{i}"))
            .collect_vec();
        let builders = (0..return_types.len())
            .map(|i| format_ident!("builder{i}"))
            .collect_vec();
        let builder_types = return_types
            .iter()
            .map(|ty| format_ident!("{}Builder", types::array_type(ty)))
            .collect_vec();
        let return_types = if return_types.len() == 1 {
            vec![quote! { self.return_type.clone() }]
        } else {
            (0..return_types.len())
                .map(|i| quote! { self.return_type.as_struct().types().nth(#i).unwrap().clone() })
                .collect()
        };
        let build_value_array = if return_types.len() == 1 {
            quote! { let [value_array] = value_arrays; }
        } else {
            quote! {
                let bitmap = value_arrays[0].null_bitmap().clone();
                let value_array = StructArray::new(
                    self.return_type.as_struct().clone(),
                    value_arrays.to_vec(),
                    bitmap,
                ).into_ref();
            }
        };
        let prebuilt_arg = match &self.prebuild {
            Some(_) => quote! { &self.prebuilt_arg },
            None => quote! {},
        };
        let prebuilt_arg_type = match &self.prebuild {
            Some(s) => s.split("::").next().unwrap().parse().unwrap(),
            None => quote! { () },
        };
        let prebuilt_arg_value = match &self.prebuild {
            Some(s) => s
                .replace('$', "child")
                .parse()
                .expect("invalid prebuild syntax"),
            None => quote! { () },
        };
        let iter = match user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { iter },
            ReturnTypeKind::Option => quote! { iter.flatten() },
            ReturnTypeKind::Result => quote! { iter? },
            ReturnTypeKind::ResultOption => quote! { value?.flatten() },
        };
        let iterator_item_type = user_fn.iterator_item_kind.clone().ok_or_else(|| {
            Error::new(
                user_fn.return_type_span,
                "expect `impl Iterator` in return type",
            )
        })?;
        let output = match iterator_item_type {
            ReturnTypeKind::T => quote! { Some(output) },
            ReturnTypeKind::Option => quote! { output },
            ReturnTypeKind::Result => quote! { Some(output?) },
            ReturnTypeKind::ResultOption => quote! { output? },
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
                #(
                    let #const_child = #const_child.eval_const()?;
                    let #const_child = match &#const_child {
                        Some(s) => s.as_scalar_ref_impl().try_into()?,
                        // the function should always return empty if any const argument is null
                        None => return Ok(crate::table_function::empty(return_type)),
                    };
                )*

                #[derive(Debug)]
                struct #struct_name {
                    return_type: DataType,
                    chunk_size: usize,
                    #(#child: BoxedExpression,)*
                    prebuilt_arg: #prebuilt_arg_type,
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

                        let mut index_builder = I32ArrayBuilder::new(self.chunk_size);
                        #(let mut #builders = #builder_types::with_type(self.chunk_size, #return_types);)*

                        for (i, (row, visible)) in multizip((#(#arrays.iter(),)*)).zip_eq_fast(input.vis().iter()).enumerate() {
                            if let (#(Some(#inputs),)*) = row && visible {
                                let iter = #fn_name(#(#inputs,)* #prebuilt_arg);
                                for output in #iter {
                                    index_builder.append(Some(i as i32));
                                    match #output {
                                        Some((#(#outputs),*)) => { #(#builders.append(Some(#outputs.as_scalar_ref()));)* }
                                        None => { #(#builders.append_null();)* }
                                    }

                                    if index_builder.len() == self.chunk_size {
                                        let index_array = std::mem::replace(&mut index_builder, I32ArrayBuilder::new(self.chunk_size)).finish().into_ref();
                                        let value_arrays = [#(std::mem::replace(&mut #builders, #builder_types::with_type(self.chunk_size, #return_types)).finish().into_ref()),*];
                                        #build_value_array
                                        yield DataChunk::new(vec![index_array, value_array], self.chunk_size);
                                    }
                                }
                            }
                        }

                        if index_builder.len() > 0 {
                            let len = index_builder.len();
                            let index_array = index_builder.finish().into_ref();
                            let value_arrays = [#(#builders.finish().into_ref()),*];
                            #build_value_array
                            yield DataChunk::new(vec![index_array, value_array], len);
                        }
                    }
                }

                Ok(Box::new(#struct_name {
                    return_type,
                    chunk_size,
                    #(#child,)*
                    prebuilt_arg: #prebuilt_arg_value,
                }))
            }
        })
    }
}

fn data_type_name(ty: &str) -> TokenStream2 {
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { DataTypeName::#variant }
}

fn data_type(ty: &str) -> TokenStream2 {
    if let Some(ty) = ty.strip_suffix("[]") {
        let inner_type = data_type(ty);
        return quote! { DataType::List(Box::new(#inner_type)) };
    }
    if ty.starts_with("struct<") {
        return quote! { DataType::Struct(#ty.parse().expect("invalid struct type")) };
    }
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { DataType::#variant }
}

/// Extract multiple output types.
///
/// ```ignore
/// output_types("int32") -> ["int32"]
/// output_types("struct<key varchar, value jsonb>") -> ["varchar", "jsonb"]
/// ```
fn output_types(ty: &str) -> Vec<&str> {
    if let Some(s) = ty.strip_prefix("struct<") && let Some(args) = s.strip_suffix('>') {
        args.split(',').map(|s| s.split_whitespace().nth(1).unwrap()).collect()
    } else {
        vec![ty]
    }
}
