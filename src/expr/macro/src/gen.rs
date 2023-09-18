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
        let variadic = matches!(self.args.last(), Some(t) if t == "...");
        let args = match variadic {
            true => &self.args[..self.args.len() - 1],
            false => &self.args[..],
        }
        .iter()
        .map(|ty| data_type_name(ty))
        .collect_vec();
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let descriptor_type = quote! { risingwave_expr::sig::func::FuncSign };
        let build_fn = if build_fn {
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else {
            self.generate_build_scalar_function(user_fn, true)?
        };
        let deprecated = self.deprecated;
        Ok(quote! {
            #[risingwave_expr::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { risingwave_expr::sig::func::_register(#descriptor_type {
                    func: risingwave_pb::expr::expr_node::Type::#pb_type,
                    inputs_type: &[#(#args),*],
                    variadic: #variadic,
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
        let variadic = matches!(self.args.last(), Some(t) if t == "...");
        let num_args = self.args.len() - if variadic { 1 } else { 0 };
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
                        None => return Ok(Box::new(risingwave_expr::expr::LiteralExpression::new(
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

        // ensure the number of children matches the number of arguments
        let check_children = match variadic {
            true => quote! { risingwave_expr::ensure!(children.len() >= #num_args); },
            false => quote! { risingwave_expr::ensure!(children.len() == #num_args); },
        };

        // evaluate variadic arguments in `eval`
        let eval_variadic = variadic.then(|| {
            quote! {
                let mut columns = Vec::with_capacity(self.children.len() - #num_args);
                for child in &self.children[#num_args..] {
                    columns.push(child.eval_checked(input).await?);
                }
                let variadic_input = DataChunk::new(columns, input.vis().clone());
            }
        });
        // evaluate variadic arguments in `eval_row`
        let eval_row_variadic = variadic.then(|| {
            quote! {
                let mut row = Vec::with_capacity(self.children.len() - #num_args);
                for child in &self.children[#num_args..] {
                    row.push(child.eval_row(input).await?);
                }
                let variadic_row = OwnedRow::new(row);
            }
        });

        let generic = (self.ret == "boolean" && user_fn.generic == 3).then(|| {
            // XXX: for generic compare functions, we need to specify the compatible type
            let compatible_type = types::ref_type(types::min_compatible_type(&self.args))
                .parse::<TokenStream2>()
                .unwrap();
            quote! { ::<_, _, #compatible_type> }
        });
        let prebuilt_arg = match (&self.prebuild, optimize_const) {
            // use the prebuilt argument
            (Some(_), true) => quote! { &self.prebuilt_arg, },
            // build the argument on site
            (Some(_), false) => quote! { &#prebuilt_arg_value, },
            // no prebuilt argument
            (None, _) => quote! {},
        };
        let variadic_args = variadic.then(|| quote! { variadic_row, });
        let context = user_fn.context.then(|| quote! { &self.context, });
        let writer = user_fn.write.then(|| quote! { &mut writer, });
        let await_ = user_fn.async_.then(|| quote! { .await });
        // call the user defined function
        // inputs: [ Option<impl ScalarRef> ]
        let mut output = quote! { #fn_name #generic(
            #(#non_prebuilt_inputs,)*
            #prebuilt_arg
            #variadic_args
            #context
            #writer
        ) #await_ };
        // handle error if the function returns `Result`
        // wrap a `Some` if the function doesn't return `Option`
        output = match user_fn.return_type_kind {
            // XXX: we don't support void type yet. return null::int for now.
            _ if self.ret == "void" => quote! { { #output; Option::<i32>::None } },
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
        // now the `output` is: Option<impl ScalarRef or Scalar>
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
            assert!(
                !variadic,
                "customized batch function is not supported for variadic functions"
            );
            // user defined batch function
            let fn_name = format_ident!("{}", batch_fn);
            quote! {
                let c = #fn_name(#(#arrays),*);
                Ok(Arc::new(c.into()))
            }
        } else if (types::is_primitive(&self.ret) || self.ret == "boolean")
            && user_fn.is_pure()
            && !variadic
        {
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
            let array_zip = match children_indices.len() {
                0 => quote! { std::iter::repeat(()).take(input.capacity()) },
                _ => quote! { multizip((#(#arrays.iter(),)*)) },
            };
            let let_variadic = variadic.then(|| {
                quote! {
                    let variadic_row = variadic_input.row_at_unchecked_vis(i);
                }
            });
            quote! {
                let mut builder = #builder_type::with_type(input.capacity(), self.context.return_type.clone());

                match input.vis() {
                    Vis::Bitmap(vis) => {
                        // allow using `zip` for performance
                        #[allow(clippy::disallowed_methods)]
                        for (i, ((#(#inputs,)*), visible)) in #array_zip.zip(vis.iter()).enumerate() {
                            if !visible {
                                builder.append_null();
                                continue;
                            }
                            #let_variadic
                            #append_output
                        }
                    }
                    Vis::Compact(_) => {
                        for (i, (#(#inputs,)*)) in #array_zip.enumerate() {
                            #let_variadic
                            #append_output
                        }
                    }
                }
                Ok(Arc::new(builder.finish().into()))
            }
        };

        Ok(quote! {
            |return_type: DataType, children: Vec<risingwave_expr::expr::BoxedExpression>|
                -> risingwave_expr::Result<risingwave_expr::expr::BoxedExpression>
            {
                use std::sync::Arc;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::row::OwnedRow;
                use risingwave_common::util::iter_util::ZipEqFast;
                use itertools::multizip;

                use risingwave_expr::expr::{Context, BoxedExpression};
                use risingwave_expr::Result;

                #check_children
                let prebuilt_arg = #prebuild_const;
                let context = Context {
                    return_type,
                    arg_types: children.iter().map(|c| c.return_type()).collect(),
                };

                #[derive(Debug)]
                struct #struct_name {
                    context: Context,
                    children: Vec<BoxedExpression>,
                    prebuilt_arg: #prebuilt_arg_type,
                }
                #[async_trait::async_trait]
                impl risingwave_expr::expr::Expression for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.context.return_type.clone()
                    }
                    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
                        #(
                            let #array_refs = self.children[#children_indices].eval_checked(input).await?;
                            let #arrays: &#arg_arrays = #array_refs.as_ref().into();
                        )*
                        #eval_variadic
                        #eval
                    }
                    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
                        #(
                            let #datums = self.children[#children_indices].eval_row(input).await?;
                            let #inputs: Option<#arg_types> = #datums.as_ref().map(|s| s.as_scalar_ref_impl().try_into().unwrap());
                        )*
                        #eval_row_variadic
                        Ok(#row_output)
                    }
                }

                Ok(Box::new(#struct_name {
                    context,
                    children,
                    prebuilt_arg,
                }))
            }
        })
    }

    /// Generate a descriptor of the aggregate function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    /// `user_fn` could be either `fn` or `impl`.
    /// If `build_fn` is true, `user_fn` must be a `fn` that builds the aggregate function.
    pub fn generate_aggregate_descriptor(
        &self,
        user_fn: &AggregateFnOrImpl,
        build_fn: bool,
    ) -> Result<TokenStream2> {
        let name = self.name.clone();

        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);
        let state_type = match &self.state {
            Some(ty) if ty != "ref" => data_type_name(ty),
            _ => data_type_name(&self.ret),
        };
        let append_only = match build_fn {
            false => !user_fn.has_retract(),
            true => self.append_only,
        };

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = match append_only {
            false => format_ident!("{}", self.ident_name()),
            true => format_ident!("{}_append_only", self.ident_name()),
        };
        let descriptor_type = quote! { risingwave_expr::sig::agg::AggFuncSig };
        let build_fn = if build_fn {
            let name = format_ident!("{}", user_fn.as_fn().name);
            quote! { #name }
        } else {
            self.generate_agg_build_fn(user_fn)?
        };
        Ok(quote! {
            #[risingwave_expr::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { risingwave_expr::sig::agg::_register(#descriptor_type {
                    func: risingwave_expr::agg::AggKind::#pb_type,
                    inputs_type: &[#(#args),*],
                    state_type: #state_type,
                    ret_type: #ret,
                    build: #build_fn,
                    append_only: #append_only,
                }) };
            }
        })
    }

    /// Generate build function for aggregate function.
    fn generate_agg_build_fn(&self, user_fn: &AggregateFnOrImpl) -> Result<TokenStream2> {
        let state_type: TokenStream2 = match &self.state {
            Some(state) if state == "ref" => types::ref_type(&self.ret).parse().unwrap(),
            Some(state) if state != "ref" => types::owned_type(state).parse().unwrap(),
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
        let args = (0..self.args.len()).map(|i| format_ident!("v{i}"));
        let args = quote! { #(#args,)* };
        let panic_on_retract = {
            let msg = format!(
                "attempt to retract on aggregate function {}, but it is append-only",
                self.name
            );
            quote! { assert_eq!(op, Op::Insert, #msg); }
        };
        let mut next_state = match user_fn {
            AggregateFnOrImpl::Fn(f) => {
                let fn_name = format_ident!("{}", f.name);
                match f.retract {
                    true => {
                        quote! { #fn_name(state, #args matches!(op, Op::Delete | Op::UpdateDelete)) }
                    }
                    false => quote! {{
                        #panic_on_retract
                        #fn_name(state, #args)
                    }},
                }
            }
            AggregateFnOrImpl::Impl(i) => {
                let retract = match i.retract {
                    Some(_) => quote! { self.function.retract(state, #args) },
                    None => panic_on_retract,
                };
                quote! {
                    if matches!(op, Op::Delete | Op::UpdateDelete) {
                        #retract
                    } else {
                        self.function.accumulate(state, #args)
                    }
                }
            }
        };
        next_state = match user_fn.accumulate().return_type_kind {
            ReturnTypeKind::T => quote! { Some(#next_state) },
            ReturnTypeKind::Option => next_state,
            ReturnTypeKind::Result => quote! { Some(#next_state?) },
            ReturnTypeKind::ResultOption => quote! { #next_state? },
        };
        if !user_fn.accumulate().arg_option {
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
                    let first_state = if self.init_state.is_some() {
                        quote! { unreachable!() }
                    } else if let Some(s) = &self.state && s == "ref" {
                        // for min/max/first/last, the state is the first value
                        quote! { Some(v0) }
                    } else {
                        quote! {{
                            let state = #state_type::default();
                            #next_state
                        }}
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
        let get_result = match user_fn {
            AggregateFnOrImpl::Impl(impl_) if impl_.finalize.is_some() => {
                quote! {
                    let state = match state {
                        Some(s) => s.as_scalar_ref_impl().try_into().unwrap(),
                        None => return Ok(None),
                    };
                    Ok(Some(self.function.finalize(state).into()))
                }
            }
            _ => quote! { Ok(state.clone()) },
        };
        let function_field = match user_fn {
            AggregateFnOrImpl::Fn(_) => quote! {},
            AggregateFnOrImpl::Impl(i) => {
                let struct_name = format_ident!("{}", i.struct_name);
                quote! { function: #struct_name, }
            }
        };
        let function_new = match user_fn {
            AggregateFnOrImpl::Fn(_) => quote! {},
            AggregateFnOrImpl::Impl(i) => {
                let struct_name = format_ident!("{}", i.struct_name);
                quote! { function: #struct_name::default(), }
            }
        };

        Ok(quote! {
            |agg| {
                use std::collections::HashSet;
                use std::ops::Range;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::bail;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::estimate_size::EstimateSize;

                use risingwave_expr::Result;
                use risingwave_expr::agg::AggregateState;

                #[derive(Clone)]
                struct Agg {
                    return_type: DataType,
                    #function_field
                }

                #[async_trait::async_trait]
                impl risingwave_expr::agg::AggregateFunction for Agg {
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
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                            Vis::Compact(_) => {
                                for row_id in 0..input.capacity() {
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
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
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                            Vis::Compact(_) => {
                                for row_id in range {
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                        }
                        *state0 = #assign_state;
                        Ok(())
                    }

                    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
                        let state = state.as_datum();
                        #get_result
                    }
                }

                Ok(Box::new(Agg {
                    return_type: agg.return_type.clone(),
                    #function_new
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
        let descriptor_type = quote! { risingwave_expr::sig::table_function::FuncSign };
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
            #[risingwave_expr::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { risingwave_expr::sig::table_function::_register(#descriptor_type {
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

                risingwave_expr::ensure!(children.len() == #num_args);
                let mut iter = children.into_iter();
                #(let #all_child = iter.next().unwrap();)*
                #(
                    let #const_child = #const_child.eval_const()?;
                    let #const_child = match &#const_child {
                        Some(s) => s.as_scalar_ref_impl().try_into()?,
                        // the function should always return empty if any const argument is null
                        None => return Ok(risingwave_expr::table_function::empty(return_type)),
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
                impl risingwave_expr::table_function::TableFunction for #struct_name {
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
