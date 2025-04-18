// Copyright 2025 RisingWave Labs
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
        // handle variadic argument
        if self
            .args
            .last()
            .is_some_and(|arg| arg.starts_with("variadic"))
        {
            // expand:  foo(a, b, variadic anyarray)
            // to:      foo(a, b, ...)
            //        + foo_variadic(a, b, anyarray)
            let mut attrs = Vec::new();
            attrs.extend(
                FunctionAttr {
                    args: {
                        let mut args = self.args.clone();
                        *args.last_mut().unwrap() = "...".to_owned();
                        args
                    },
                    ..self.clone()
                }
                .expand(),
            );
            attrs.extend(
                FunctionAttr {
                    name: format!("{}_variadic", self.name),
                    args: {
                        let mut args = self.args.clone();
                        let last = args.last_mut().unwrap();
                        *last = last.strip_prefix("variadic ").unwrap().into();
                        args
                    },
                    ..self.clone()
                }
                .expand(),
            );
            return attrs;
        }
        let args = self.args.iter().map(|ty| types::expand_type_wildcard(ty));
        let ret = types::expand_type_wildcard(&self.ret);
        let mut attrs = Vec::new();
        for (args, mut ret) in args.multi_cartesian_product().cartesian_product(ret) {
            if ret == "auto" {
                ret = types::min_compatible_type(&args);
            }
            let attr = FunctionAttr {
                args: args.iter().map(|s| s.to_string()).collect(),
                ret: ret.to_owned(),
                ..self.clone()
            };
            attrs.push(attr);
        }
        attrs
    }

    /// Generate the type infer function: `fn(&[DataType]) -> Result<DataType>`
    fn generate_type_infer_fn(&self) -> Result<TokenStream2> {
        if let Some(func) = &self.type_infer {
            if func == "unreachable" {
                return Ok(
                    quote! { |_| unreachable!("type inference for this function should be specially handled in frontend, and should not call sig.type_infer") },
                );
            }
            // use the user defined type inference function
            return Ok(func.parse().unwrap());
        } else if self.ret == "any" {
            // TODO: if there are multiple "any", they should be the same type
            if let Some(i) = self.args.iter().position(|t| t == "any") {
                // infer as the type of "any" argument
                return Ok(quote! { |args| Ok(args[#i].clone()) });
            }
            if let Some(i) = self.args.iter().position(|t| t == "anyarray") {
                // infer as the element type of "anyarray" argument
                return Ok(quote! { |args| Ok(args[#i].as_list().clone()) });
            }
        } else if self.ret == "anyarray" {
            if let Some(i) = self.args.iter().position(|t| t == "anyarray") {
                // infer as the type of "anyarray" argument
                return Ok(quote! { |args| Ok(args[#i].clone()) });
            }
            if let Some(i) = self.args.iter().position(|t| t == "any") {
                // infer as the array type of "any" argument
                return Ok(quote! { |args| Ok(DataType::List(Box::new(args[#i].clone()))) });
            }
        } else if self.ret == "struct" {
            if let Some(i) = self.args.iter().position(|t| t == "struct") {
                // infer as the type of "struct" argument
                return Ok(quote! { |args| Ok(args[#i].clone()) });
            }
        } else if self.ret == "anymap" {
            if let Some(i) = self.args.iter().position(|t| t == "anymap") {
                // infer as the type of "anymap" argument
                return Ok(quote! { |args| Ok(args[#i].clone()) });
            }
        } else if self.ret == "vector" {
            if let Some(i) = self.args.iter().position(|t| t == "vector") {
                // infer as the type of "vector" argument
                // Example usage: `last_value(*) -> auto`
                return Ok(quote! { |args| Ok(args[#i].clone()) });
            }
        } else {
            // the return type is fixed
            let ty = data_type(&self.ret);
            return Ok(quote! { |_| Ok(#ty) });
        }
        Err(Error::new(
            Span::call_site(),
            "type inference function cannot be automatically derived. You should provide: `type_infer = \"|args| Ok(...)\"`",
        ))
    }

    /// Generate a descriptor (`FuncSign`) of the scalar or table function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    ///
    /// # Arguments
    /// `build_fn`: whether the user provided a function is a build function.
    /// (from the `#[build_function]` macro)
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
        .map(|ty| sig_data_type(ty))
        .collect_vec();
        let ret = sig_data_type(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let build_fn = if build_fn {
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else if self.rewritten {
            quote! { |_, _| Err(ExprError::UnsupportedFunction(#name.into())) }
        } else {
            // This is the core logic for `#[function]`
            self.generate_build_scalar_function(user_fn, true)?
        };
        let type_infer_fn = self.generate_type_infer_fn()?;
        let deprecated = self.deprecated;

        Ok(quote! {
            #[risingwave_expr::codegen::linkme::distributed_slice(risingwave_expr::sig::FUNCTIONS)]
            fn #ctor_name() -> risingwave_expr::sig::FuncSign {
                use risingwave_common::types::{DataType, DataTypeName};
                use risingwave_expr::sig::{FuncSign, SigDataType, FuncBuilder};

                FuncSign {
                    name: risingwave_pb::expr::expr_node::Type::#pb_type.into(),
                    inputs_type: vec![#(#args),*],
                    variadic: #variadic,
                    ret_type: #ret,
                    build: FuncBuilder::Scalar(#build_fn),
                    type_infer: #type_infer_fn,
                    deprecated: #deprecated,
                }
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
                    columns.push(child.eval(input).await?);
                }
                let variadic_input = DataChunk::new(columns, input.visibility().clone());
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
        let variadic_args = variadic.then(|| quote! { &variadic_row, });
        let context = user_fn.context.then(|| quote! { &self.context, });
        let writer = user_fn.write.then(|| quote! { &mut writer, });
        let await_ = user_fn.async_.then(|| quote! { .await });

        let record_error = {
            // Uniform arguments into `DatumRef`.
            #[allow(clippy::disallowed_methods)] // allow zip
            let inputs_args = inputs
                .iter()
                .zip(user_fn.args_option.iter())
                .map(|(input, opt)| {
                    if *opt {
                        quote! { #input.map(|s| ScalarRefImpl::from(s)) }
                    } else {
                        quote! { Some(ScalarRefImpl::from(#input)) }
                    }
                });
            let inputs_args = quote! {
                let args: &[DatumRef<'_>] = &[#(#inputs_args),*];
                let args = args.iter().copied();
            };
            let var_args = variadic.then(|| {
                quote! {
                    let args = args.chain(variadic_row.iter());
                }
            });

            quote! {
                #inputs_args
                #var_args
                errors.push(ExprError::function(
                    stringify!(#fn_name),
                    args,
                    e,
                ));
            }
        };

        // call the user defined function
        // inputs: [ Option<impl ScalarRef> ]
        let mut output = quote! { #fn_name #generic(
            #(#non_prebuilt_inputs,)*
            #variadic_args
            #prebuilt_arg
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
            ReturnTypeKind::Result => quote! {
                match #output {
                    Ok(x) => Some(x),
                    Err(e) => {
                        #record_error
                        None
                    }
                }
            },
            ReturnTypeKind::ResultOption => quote! {
                match #output {
                    Ok(x) => x,
                    Err(e) => {
                        #record_error
                        None
                    }
                }
            },
        };
        // if user function accepts non-option arguments, we assume the function
        // returns null on null input, so we need to unwrap the inputs before calling.
        if self.prebuild.is_some() {
            output = quote! {
                match (#(#inputs,)*) {
                    (#(Some(#inputs),)*) => #output,
                    _ => None,
                }
            };
        } else {
            #[allow(clippy::disallowed_methods)] // allow zip
            let some_inputs = inputs
                .iter()
                .zip(user_fn.args_option.iter())
                .map(|(input, opt)| {
                    if *opt {
                        quote! { #input }
                    } else {
                        quote! { Some(#input) }
                    }
                });
            output = quote! {
                match (#(#inputs,)*) {
                    (#(#some_inputs,)*) => #output,
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
                Arc::new(c.into())
            }
        } else if (types::is_primitive(&self.ret) || self.ret == "boolean")
            && user_fn.is_pure()
            && !variadic
            && self.prebuild.is_none()
        {
            // SIMD optimization for primitive types
            match self.args.len() {
                0 => quote! {
                    let c = #ret_array_type::from_iter_bitmap(
                        std::iter::repeat_with(|| #fn_name()).take(input.capacity())
                        Bitmap::ones(input.capacity()),
                    );
                    Arc::new(c.into())
                },
                1 => quote! {
                    let c = #ret_array_type::from_iter_bitmap(
                        a0.raw_iter().map(|a| #fn_name(a)),
                        a0.null_bitmap().clone()
                    );
                    Arc::new(c.into())
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
                    Arc::new(c.into())
                },
                n => todo!("SIMD optimization for {n} arguments"),
            }
        } else {
            // no optimization
            let let_variadic = variadic.then(|| {
                quote! {
                    let variadic_row = variadic_input.row_at_unchecked_vis(i);
                }
            });
            quote! {
                let mut builder = #builder_type::with_type(input.capacity(), self.context.return_type.clone());

                if input.is_compacted() {
                    for i in 0..input.capacity() {
                        #(let #inputs = unsafe { #arrays.value_at_unchecked(i) };)*
                        #let_variadic
                        #append_output
                    }
                } else {
                    for i in 0..input.capacity() {
                        if unsafe { !input.visibility().is_set_unchecked(i) } {
                            builder.append_null();
                            continue;
                        }
                        #(let #inputs = unsafe { #arrays.value_at_unchecked(i) };)*
                        #let_variadic
                        #append_output
                    }
                }
                Arc::new(builder.finish().into())
            }
        };

        Ok(quote! {
            |return_type: DataType, children: Vec<risingwave_expr::expr::BoxedExpression>|
                -> risingwave_expr::Result<risingwave_expr::expr::BoxedExpression>
            {
                use std::sync::Arc;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::bitmap::Bitmap;
                use risingwave_common::row::OwnedRow;
                use risingwave_common::util::iter_util::ZipEqFast;

                use risingwave_expr::expr::{Context, BoxedExpression};
                use risingwave_expr::{ExprError, Result};
                use risingwave_expr::codegen::*;

                #check_children
                let prebuilt_arg = #prebuild_const;
                let context = Context {
                    return_type,
                    arg_types: children.iter().map(|c| c.return_type()).collect(),
                    variadic: #variadic,
                };

                #[derive(Debug)]
                struct #struct_name {
                    context: Context,
                    children: Vec<BoxedExpression>,
                    prebuilt_arg: #prebuilt_arg_type,
                }
                #[async_trait]
                impl risingwave_expr::expr::Expression for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.context.return_type.clone()
                    }
                    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
                        #(
                            let #array_refs = self.children[#children_indices].eval(input).await?;
                            let #arrays: &#arg_arrays = #array_refs.as_ref().into();
                        )*
                        #eval_variadic
                        let mut errors = vec![];
                        let array = { #eval };
                        if errors.is_empty() {
                            Ok(array)
                        } else {
                            Err(ExprError::Multiple(array, errors.into()))
                        }
                    }
                    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
                        #(
                            let #datums = self.children[#children_indices].eval_row(input).await?;
                            let #inputs: Option<#arg_types> = #datums.as_ref().map(|s| s.as_scalar_ref_impl().try_into().unwrap());
                        )*
                        #eval_row_variadic
                        let mut errors: Vec<ExprError> = vec![];
                        let output = #row_output;
                        if let Some(err) = errors.into_iter().next() {
                            Err(err.into())
                        } else {
                            Ok(output)
                        }
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
            args.push(sig_data_type(ty));
        }
        let ret = sig_data_type(&self.ret);
        let state_type = match &self.state {
            Some(ty) if ty != "ref" => {
                let ty = data_type(ty);
                quote! { Some(#ty) }
            }
            _ => quote! { None },
        };
        let append_only = match build_fn {
            false => !user_fn.has_retract(),
            true => self.append_only,
        };

        let pb_kind = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = match append_only {
            false => format_ident!("{}", self.ident_name()),
            true => format_ident!("{}_append_only", self.ident_name()),
        };
        let build_fn = if build_fn {
            let name = format_ident!("{}", user_fn.as_fn().name);
            quote! { #name }
        } else if self.rewritten {
            quote! { |_| Err(ExprError::UnsupportedFunction(#name.into())) }
        } else {
            self.generate_agg_build_fn(user_fn)?
        };
        let build_retractable = match append_only {
            true => quote! { None },
            false => quote! { Some(#build_fn) },
        };
        let build_append_only = match append_only {
            false => quote! { None },
            true => quote! { Some(#build_fn) },
        };
        let retractable_state_type = match append_only {
            true => quote! { None },
            false => state_type.clone(),
        };
        let append_only_state_type = match append_only {
            false => quote! { None },
            true => state_type,
        };
        let type_infer_fn = self.generate_type_infer_fn()?;
        let deprecated = self.deprecated;

        Ok(quote! {
            #[risingwave_expr::codegen::linkme::distributed_slice(risingwave_expr::sig::FUNCTIONS)]
            fn #ctor_name() -> risingwave_expr::sig::FuncSign {
                use risingwave_common::types::{DataType, DataTypeName};
                use risingwave_expr::sig::{FuncSign, SigDataType, FuncBuilder};

                FuncSign {
                    name: risingwave_pb::expr::agg_call::PbKind::#pb_kind.into(),
                    inputs_type: vec![#(#args),*],
                    variadic: false,
                    ret_type: #ret,
                    build: FuncBuilder::Aggregate {
                        retractable: #build_retractable,
                        append_only: #build_append_only,
                        retractable_state_type: #retractable_state_type,
                        append_only_state_type: #append_only_state_type,
                    },
                    type_infer: #type_infer_fn,
                    deprecated: #deprecated,
                }
            }
        })
    }

    /// Generate build function for aggregate function.
    fn generate_agg_build_fn(&self, user_fn: &AggregateFnOrImpl) -> Result<TokenStream2> {
        // If the first argument of the aggregate function is of type `&mut T`,
        // we assume it is a user defined state type.
        let custom_state = user_fn.accumulate().first_mut_ref_arg.as_ref();
        let state_type: TokenStream2 = match (custom_state, &self.state) {
            (Some(s), _) => s.parse().unwrap(),
            (_, Some(state)) if state == "ref" => types::ref_type(&self.ret).parse().unwrap(),
            (_, Some(state)) if state != "ref" => types::owned_type(state).parse().unwrap(),
            _ => types::owned_type(&self.ret).parse().unwrap(),
        };
        let let_arrays = self
            .args
            .iter()
            .enumerate()
            .map(|(i, arg)| {
                let array = format_ident!("a{i}");
                let array_type: TokenStream2 = types::array_type(arg).parse().unwrap();
                quote! {
                    let #array: &#array_type = input.column_at(#i).as_ref().into();
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
        let downcast_state = if custom_state.is_some() {
            quote! { let mut state: &mut #state_type = state0.downcast_mut(); }
        } else if let Some(s) = &self.state
            && s == "ref"
        {
            quote! { let mut state: Option<#state_type> = state0.as_datum_mut().as_ref().map(|x| x.as_scalar_ref_impl().try_into().unwrap()); }
        } else {
            quote! { let mut state: Option<#state_type> = state0.as_datum_mut().take().map(|s| s.try_into().unwrap()); }
        };
        let restore_state = if custom_state.is_some() {
            quote! {}
        } else if let Some(s) = &self.state
            && s == "ref"
        {
            quote! { *state0.as_datum_mut() = state.map(|x| x.to_owned_scalar().into()); }
        } else {
            quote! { *state0.as_datum_mut() = state.map(|s| s.into()); }
        };
        let create_state = if custom_state.is_some() {
            quote! {
                fn create_state(&self) -> Result<AggregateState> {
                    Ok(AggregateState::Any(Box::<#state_type>::default()))
                }
            }
        } else if let Some(state) = &self.init_state {
            let state: TokenStream2 = state.parse().unwrap();
            quote! {
                fn create_state(&self) -> Result<AggregateState> {
                    Ok(AggregateState::Datum(Some(#state.into())))
                }
            }
        } else {
            // by default: `AggregateState::Datum(None)`
            quote! {}
        };
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
                let context = f.context.then(|| quote! { &self.context, });
                let fn_name = format_ident!("{}", f.name);
                match f.retract {
                    true => {
                        quote! { #fn_name(state, #args matches!(op, Op::Delete | Op::UpdateDelete) #context) }
                    }
                    false => quote! {{
                        #panic_on_retract
                        #fn_name(state, #args #context)
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
        if user_fn.accumulate().args_option.iter().all(|b| !b) {
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
                        // for count, the state will never be None
                        quote! { unreachable!() }
                    } else if let Some(s) = &self.state
                        && s == "ref"
                    {
                        // for min/max/first/last, the state is the first value
                        quote! { Some(v0) }
                    } else if let AggregateFnOrImpl::Impl(impl_) = user_fn
                        && impl_.create_state.is_some()
                    {
                        // use user-defined create_state function
                        quote! {{
                            let state = self.function.create_state();
                            #next_state
                        }}
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
        let update_state = if custom_state.is_some() {
            quote! { _ = #next_state; }
        } else {
            quote! { state = #next_state; }
        };
        let get_result = if custom_state.is_some() {
            quote! { Ok(state.downcast_ref::<#state_type>().into()) }
        } else if let AggregateFnOrImpl::Impl(impl_) = user_fn
            && impl_.finalize.is_some()
        {
            quote! {
                let state = match state.as_datum() {
                    Some(s) => s.as_scalar_ref_impl().try_into().unwrap(),
                    None => return Ok(None),
                };
                Ok(Some(self.function.finalize(state).into()))
            }
        } else {
            quote! { Ok(state.as_datum().clone()) }
        };
        let function_field = match user_fn {
            AggregateFnOrImpl::Fn(_) => quote! {},
            AggregateFnOrImpl::Impl(i) => {
                let struct_name = format_ident!("{}", i.struct_name);
                let generic = self.generic.as_ref().map(|g| {
                    let g = format_ident!("{g}");
                    quote! { <#g> }
                });
                quote! { function: #struct_name #generic, }
            }
        };
        let function_new = match user_fn {
            AggregateFnOrImpl::Fn(_) => quote! {},
            AggregateFnOrImpl::Impl(i) => {
                let struct_name = format_ident!("{}", i.struct_name);
                let generic = self.generic.as_ref().map(|g| {
                    let g = format_ident!("{g}");
                    quote! { ::<#g> }
                });
                quote! { function: #struct_name #generic :: default(), }
            }
        };

        Ok(quote! {
            |agg| {
                use std::collections::HashSet;
                use std::ops::Range;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::bail;
                use risingwave_common::bitmap::Bitmap;
                use risingwave_common_estimate_size::EstimateSize;

                use risingwave_expr::expr::Context;
                use risingwave_expr::Result;
                use risingwave_expr::aggregate::AggregateState;
                use risingwave_expr::codegen::async_trait;

                let context = Context {
                    return_type: agg.return_type.clone(),
                    arg_types: agg.args.arg_types().to_owned(),
                    variadic: false,
                };

                struct Agg {
                    context: Context,
                    #function_field
                }

                #[async_trait]
                impl risingwave_expr::aggregate::AggregateFunction for Agg {
                    fn return_type(&self) -> DataType {
                        self.context.return_type.clone()
                    }

                    #create_state

                    async fn update(&self, state0: &mut AggregateState, input: &StreamChunk) -> Result<()> {
                        #(#let_arrays)*
                        #downcast_state
                        for row_id in input.visibility().iter_ones() {
                            let op = unsafe { *input.ops().get_unchecked(row_id) };
                            #(#let_values)*
                            #update_state
                        }
                        #restore_state
                        Ok(())
                    }

                    async fn update_range(&self, state0: &mut AggregateState, input: &StreamChunk, range: Range<usize>) -> Result<()> {
                        assert!(range.end <= input.capacity());
                        #(#let_arrays)*
                        #downcast_state
                        if input.is_compacted() {
                            for row_id in range {
                                let op = unsafe { *input.ops().get_unchecked(row_id) };
                                #(#let_values)*
                                #update_state
                            }
                        } else {
                            for row_id in input.visibility().iter_ones() {
                                if row_id < range.start {
                                    continue;
                                } else if row_id >= range.end {
                                    break;
                                }
                                let op = unsafe { *input.ops().get_unchecked(row_id) };
                                #(#let_values)*
                                #update_state
                            }
                        }
                        #restore_state
                        Ok(())
                    }

                    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
                        #get_result
                    }
                }

                Ok(Box::new(Agg {
                    context,
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
            args.push(sig_data_type(ty));
        }
        let ret = sig_data_type(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let build_fn = if build_fn {
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else if self.rewritten {
            quote! { |_, _| Err(ExprError::UnsupportedFunction(#name.into())) }
        } else {
            self.generate_build_table_function(user_fn)?
        };
        let type_infer_fn = self.generate_type_infer_fn()?;
        let deprecated = self.deprecated;

        Ok(quote! {
            #[risingwave_expr::codegen::linkme::distributed_slice(risingwave_expr::sig::FUNCTIONS)]
            fn #ctor_name() -> risingwave_expr::sig::FuncSign {
                use risingwave_common::types::{DataType, DataTypeName};
                use risingwave_expr::sig::{FuncSign, SigDataType, FuncBuilder};

                FuncSign {
                    name: risingwave_pb::expr::table_function::Type::#pb_type.into(),
                    inputs_type: vec![#(#args),*],
                    variadic: false,
                    ret_type: #ret,
                    build: FuncBuilder::Table(#build_fn),
                    type_infer: #type_infer_fn,
                    deprecated: #deprecated,
                }
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
        let arg_arrays = arg_ids
            .iter()
            .map(|i| format_ident!("{}", types::array_type(&self.args[*i])));
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
            vec![quote! { self.context.return_type.clone() }]
        } else {
            (0..return_types.len())
                .map(|i| quote! { self.context.return_type.as_struct().types().nth(#i).unwrap().clone() })
                .collect()
        };
        #[allow(clippy::disallowed_methods)]
        let optioned_outputs = user_fn
            .core_return_type
            .split(',')
            .map(|t| t.contains("Option"))
            // example: "(Option<&str>, i32)" => [true, false]
            .zip(&outputs)
            .map(|(optional, o)| match optional {
                false => quote! { Some(#o.as_scalar_ref()) },
                true => quote! { #o.map(|o| o.as_scalar_ref()) },
            })
            .collect_vec();
        let build_value_array = if return_types.len() == 1 {
            quote! { let [value_array] = value_arrays; }
        } else {
            quote! {
                let value_array = StructArray::new(
                    self.context.return_type.as_struct().clone(),
                    value_arrays.to_vec(),
                    Bitmap::ones(len),
                ).into_ref();
            }
        };
        let context = user_fn.context.then(|| quote! { &self.context, });
        let prebuilt_arg = match &self.prebuild {
            Some(_) => quote! { &self.prebuilt_arg, },
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
        let iter = quote! { #fn_name(#(#inputs,)* #prebuilt_arg #context) };
        let mut iter = match user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { #iter },
            ReturnTypeKind::Option => quote! { match #iter {
                Some(it) => it,
                None => continue,
            } },
            ReturnTypeKind::Result => quote! { match #iter {
                Ok(it) => it,
                Err(e) => {
                    index_builder.append(Some(i as i32));
                    #(#builders.append_null();)*
                    error_builder.append_display(Some(e.as_report()));
                    continue;
                }
            } },
            ReturnTypeKind::ResultOption => quote! { match #iter {
                Ok(Some(it)) => it,
                Ok(None) => continue,
                Err(e) => {
                    index_builder.append(Some(i as i32));
                    #(#builders.append_null();)*
                    error_builder.append_display(Some(e.as_report()));
                    continue;
                }
            } },
        };
        // if user function accepts non-option arguments, we assume the function
        // returns empty on null input, so we need to unwrap the inputs before calling.
        #[allow(clippy::disallowed_methods)] // allow zip
        let some_inputs = inputs
            .iter()
            .zip(user_fn.args_option.iter())
            .map(|(input, opt)| {
                if *opt {
                    quote! { #input }
                } else {
                    quote! { Some(#input) }
                }
            });
        iter = quote! {
            match (#(#inputs,)*) {
                (#(#some_inputs,)*) => #iter,
                _ => continue,
            }
        };
        let iterator_item_type = user_fn.iterator_item_kind.clone().ok_or_else(|| {
            Error::new(
                user_fn.return_type_span,
                "expect `impl Iterator` in return type",
            )
        })?;
        let append_output = match iterator_item_type {
            ReturnTypeKind::T => quote! {
                let (#(#outputs),*) = output;
                #(#builders.append(#optioned_outputs);)* error_builder.append_null();
            },
            ReturnTypeKind::Option => quote! { match output {
                Some((#(#outputs),*)) => { #(#builders.append(#optioned_outputs);)* error_builder.append_null(); }
                None => { #(#builders.append_null();)* error_builder.append_null(); }
            } },
            ReturnTypeKind::Result => quote! { match output {
                Ok((#(#outputs),*)) => { #(#builders.append(#optioned_outputs);)* error_builder.append_null(); }
                Err(e) => { #(#builders.append_null();)* error_builder.append_display(Some(e.as_report())); }
            } },
            ReturnTypeKind::ResultOption => quote! { match output {
                Ok(Some((#(#outputs),*))) => { #(#builders.append(#optioned_outputs);)* error_builder.append_null(); }
                Ok(None) => { #(#builders.append_null();)* error_builder.append_null(); }
                Err(e) => { #(#builders.append_null();)* error_builder.append_display(Some(e.as_report())); }
            } },
        };

        Ok(quote! {
            |return_type, chunk_size, children| {
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::bitmap::Bitmap;
                use risingwave_common::util::iter_util::ZipEqFast;
                use risingwave_expr::expr::{BoxedExpression, Context};
                use risingwave_expr::{Result, ExprError};
                use risingwave_expr::codegen::*;

                risingwave_expr::ensure!(children.len() == #num_args);

                let context = Context {
                    return_type: return_type.clone(),
                    arg_types: children.iter().map(|c| c.return_type()).collect(),
                    variadic: false,
                };

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
                    context: Context,
                    chunk_size: usize,
                    #(#child: BoxedExpression,)*
                    prebuilt_arg: #prebuilt_arg_type,
                }
                #[async_trait]
                impl risingwave_expr::table_function::TableFunction for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.context.return_type.clone()
                    }
                    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
                        self.eval_inner(input)
                    }
                }
                impl #struct_name {
                    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
                    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
                        #(
                        let #array_refs = self.#child.eval(input).await?;
                        let #arrays: &#arg_arrays = #array_refs.as_ref().into();
                        )*

                        let mut index_builder = I32ArrayBuilder::new(self.chunk_size);
                        #(let mut #builders = #builder_types::with_type(self.chunk_size, #return_types);)*
                        let mut error_builder = Utf8ArrayBuilder::new(self.chunk_size);

                        for i in 0..input.capacity() {
                            if unsafe { !input.visibility().is_set_unchecked(i) } {
                                continue;
                            }
                            #(let #inputs = unsafe { #arrays.value_at_unchecked(i) };)*
                            for output in #iter {
                                index_builder.append(Some(i as i32));
                                #append_output

                                if index_builder.len() == self.chunk_size {
                                    let len = index_builder.len();
                                    let index_array = std::mem::replace(&mut index_builder, I32ArrayBuilder::new(self.chunk_size)).finish().into_ref();
                                    let value_arrays = [#(std::mem::replace(&mut #builders, #builder_types::with_type(self.chunk_size, #return_types)).finish().into_ref()),*];
                                    #build_value_array
                                    let error_array = std::mem::replace(&mut error_builder, Utf8ArrayBuilder::new(self.chunk_size)).finish().into_ref();
                                    if error_array.null_bitmap().any() {
                                        yield DataChunk::new(vec![index_array, value_array, error_array], self.chunk_size);
                                    } else {
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
                            let error_array = error_builder.finish().into_ref();
                            if error_array.null_bitmap().any() {
                                yield DataChunk::new(vec![index_array, value_array, error_array], len);
                            } else {
                                yield DataChunk::new(vec![index_array, value_array], len);
                            }
                        }
                    }
                }

                Ok(Box::new(#struct_name {
                    context,
                    chunk_size,
                    #(#child,)*
                    prebuilt_arg: #prebuilt_arg_value,
                }))
            }
        })
    }
}

fn sig_data_type(ty: &str) -> TokenStream2 {
    match ty {
        "any" => quote! { SigDataType::Any },
        "anyarray" => quote! { SigDataType::AnyArray },
        "anymap" => quote! { SigDataType::AnyMap },
        "vector" => quote! { SigDataType::Vector },
        "struct" => quote! { SigDataType::AnyStruct },
        _ if ty.starts_with("struct") && ty.contains("any") => quote! { SigDataType::AnyStruct },
        _ => {
            let datatype = data_type(ty);
            quote! { SigDataType::Exact(#datatype) }
        }
    }
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
    // TODO: enable the check
    // assert!(
    //     !matches!(ty, "any" | "anyarray" | "anymap" | "struct"),
    //     "{ty}, {variant}"
    // );

    quote! { DataType::#variant }
}

/// Extract multiple output types.
///
/// ```ignore
/// output_types("int4") -> ["int4"]
/// output_types("struct<key varchar, value jsonb>") -> ["varchar", "jsonb"]
/// ```
fn output_types(ty: &str) -> Vec<&str> {
    if let Some(s) = ty.strip_prefix("struct<")
        && let Some(args) = s.strip_suffix('>')
    {
        args.split(',')
            .map(|s| s.split_whitespace().nth(1).unwrap())
            .collect()
    } else {
        vec![ty]
    }
}
