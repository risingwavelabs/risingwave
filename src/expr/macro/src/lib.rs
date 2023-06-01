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

#![feature(lint_reasons)]
#![feature(let_chains)]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::ToTokens;
use syn::{parse_macro_input, Error, Result};

mod gen;
mod parse;
mod types;
mod utils;

/// Defining the RisingWave SQL function from a Rust function.
///
/// [Online version of this doc.](https://risingwavelabs.github.io/risingwave/risingwave_expr_macro/attr.function.html)
///
/// # Table of Contents
///
/// - [Function Signature](#function-signature)
///     - [Multiple Function Definitions](#multiple-function-definitions)
///     - [Type Expansion](#type-expansion)
///     - [Automatic Type Inference](#automatic-type-inference)
///     - [Custom Type Inference Function](#custom-type-inference-function)
/// - [Rust Function Requirements](#rust-function-requirements)
///     - [Nullable Arguments](#nullable-arguments)
///     - [Return Value](#return-value)
///     - [Optimization](#optimization)
///     - [Functions Returning Strings](#functions-returning-strings)
///     - [Preprocessing Constant Arguments](#preprocessing-constant-arguments)
/// - [Table Function](#table-function)
/// - [Registration and Invocation](#registration-and-invocation)
/// - [Appendix: Type Matrix](#appendix-type-matrix)
///
/// The following example demonstrates a simple usage:
///
/// ```ignore
/// #[function("add(int32, int32) -> int32")]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
/// ```
///
/// # Function Signature
///
/// Each function must have a signature, specified in the `function("...")` part of the macro
/// invocation. The signature follows this pattern:
///
/// ```text
/// name([arg_types],*) -> [setof] return_type
/// ```
///
/// Where `name` is the function name, which must match the function name defined in `prost`.
///
/// The allowed data types are listed in the `name` column of the appendix's [type matrix].
/// Wildcards or `auto` can also be used, as explained below.
///
/// When `setof` appears before the return type, this indicates that the function is a set-returning
/// function (table function), meaning it can return multiple values instead of just one. For more
/// details, see the section on table functions.
///
/// ## Multiple Function Definitions
///
/// Multiple `#[function]` macros can be applied to a single generic Rust function to define
/// multiple SQL functions of different types. For example:
///
/// ```ignore
/// #[function("add(int16, int16) -> int16")]
/// #[function("add(int32, int32) -> int32")]
/// #[function("add(int64, int64) -> int64")]
/// fn add<T: Add>(x: T, y: T) -> T {
///     x + y
/// }
/// ```
///
/// ## Type Expansion
///
/// Types can be automatically expanded to multiple types using wildcards. Here are some examples:
///
/// - `*`: expands to all types.
/// - `*int`: expands to int16, int32, int64.
/// - `*float`: expands to float32, float64.
///
/// For instance, `#[function("cast(varchar) -> *int")]` will be expanded to the following three
/// functions:
///
/// ```ignore
/// #[function("cast(varchar) -> int16")]
/// #[function("cast(varchar) -> int32")]
/// #[function("cast(varchar) -> int64")]
/// ```
///
/// Please note the difference between `*` and `any`. `*` will generate a function for each type,
/// whereas `any` will only generate one function with a dynamic data type `Scalar`.
///
/// ## Automatic Type Inference
///
/// Correspondingly, the return type can be denoted as `auto` to be automatically inferred based on
/// the input types. It will be inferred as the smallest type that can accommodate all input types.
///
/// For example, `#[function("add(*int, *int) -> auto")]` will be expanded to:
///
/// ```ignore
/// #[function("add(int16, int16) -> int16")]
/// #[function("add(int16, int32) -> int32")]
/// #[function("add(int16, int64) -> int64")]
/// #[function("add(int32, int16) -> int32")]
/// ...
/// ```
///
/// Especially when there is only one input argument, `auto` will be inferred as the type of that
/// argument. For example, `#[function("neg(*int) -> auto")]` will be expanded to:
///
/// ```ignore
/// #[function("neg(int16) -> int16")]
/// #[function("neg(int32) -> int32")]
/// #[function("neg(int64) -> int64")]
/// ```
///
/// ## Custom Type Inference Function
///
/// A few functions might have a return type that dynamically changes based on the input argument
/// types, such as `unnest`.
///
/// In such cases, the `type_infer` option can be used to specify a function to infer the return
/// type based on the input argument types. Its function signature is
///
/// ```ignore
/// fn(&[DataType]) -> Result<DataType>
/// ```
///
/// For example:
///
/// ```ignore
/// #[function(
///     "unnest(list) -> setof any",
///     type_infer = "|args| Ok(args[0].unnest_list())"
/// )]
/// ```
///
/// This type inference function will be invoked at the frontend.
///
/// # Rust Function Requirements
///
/// The `#[function]` macro can handle various types of Rust functions.
///
/// Each argument corresponds to the *reference type* in the [type matrix].
///
/// The return value type can be the *reference type* or *owned type* in the [type matrix].
///
/// For instance:
///
/// ```ignore
/// #[function("trim_array(list, int32) -> list")]
/// fn trim_array(array: ListRef<'_>, n: i32) -> ListValue {...}
/// ```
///
/// ## Nullable Arguments
///
/// The functions above will only be called when all arguments are not null. If null arguments need
/// to be considered, the `Option` type can be used:
///
/// ```ignore
/// #[function("trim_array(list, int32) -> list")]
/// fn trim_array(array: Option<ListRef<'_>>, n: Option<i32>) -> ListValue {...}
/// ```
///
/// Note that we currently only support all arguments being either `Option` or non-`Option`. Mixed
/// cases are not supported.
///
/// ## Return Value
///
/// Similarly, the return value type can be one of the following:
///
/// - `T`: Indicates that a non-null value is always returned, and errors will not occur.
/// - `Option<T>`: Indicates that a null value may be returned, but errors will not occur.
/// - `Result<T>`: Indicates that an error may occur, but a null value will not be returned.
/// - `Result<Option<T>>`: Indicates that a null value may be returned, and an error may also occur.
///
/// ## Optimization
///
/// When all input and output types of the function are *primitive type* (refer to the [type
/// matrix]) and do not contain any Option or Result, the `#[function]` macro will automatically
/// generate SIMD vectorized execution code.
///
/// ## Functions Returning Strings
///
/// For functions that return varchar types, you can also use the writer style function signature to
/// avoid memory copying and dynamic memory allocation:
///
/// ```ignore
/// #[function("trim(varchar) -> varchar")]
/// pub fn trim(s: &str, writer: &mut dyn Write) {
///     writer.write_str(s.trim()).unwrap();
/// }
/// ```
///
/// If errors may be returned, then the return value should be `Result<()>`:
///
/// ```ignore
/// #[function("trim(varchar) -> varchar")]
/// pub fn trim(s: &str, writer: &mut dyn Write) -> Result<()> {
///     writer.write_str(s.trim()).unwrap();
///     Ok(())
/// }
/// ```
///
/// ## Preprocessing Constant Arguments
///
/// When some input arguments of the function are constants, they can be preprocessed to avoid
/// calculations every time the function is called.
///
/// A classic use case is regular expression matching:
///
/// ```ignore
/// #[function(
///     "regexp_match(varchar, varchar, varchar) -> varchar[]",
///     prebuild = "RegexpContext::from_pattern_flags($1, $2)?"
/// )]
/// fn regexp_match(text: &str, regex: &RegexpContext) -> ListValue {
///     regex.captures(text).collect()
/// }
/// ```
///
/// The `prebuild` argument can be specified, and its value is a Rust expression used to construct a
/// new variable from the input arguments of the function. Here `$1`, `$2` represent the second and
/// third arguments of the function (indexed from 0), and their types are `Datum`. In the Rust
/// function signature, these positions of parameters will be omitted, replaced by an extra new
/// variable at the end.
///
/// TODO: This macro will support both variable and constant inputs, and automatically optimize the
/// preprocessing of constants. Currently, it only supports constant inputs.
///
/// # Table Function
///
/// A table function is a special kind of function that can return multiple values instead of just
/// one. Its function signature must include the `setof` keyword, and the Rust function should
/// return an iterator of the form `impl Iterator<Item = T>` or its derived types.
///
/// For example:
/// ```ignore
/// #[function("generate_series(int32, int32) -> setof int32")]
/// fn generate_series(start: i32, stop: i32) -> impl Iterator<Item = i32> {
///     start..=stop
/// }
/// ```
///
/// Likewise, the return value `Iterator` can include `Option` or `Result` either internally or
/// externally. For instance:
///
/// - `impl Iterator<Item = Result<T>>`
/// - `Result<impl Iterator<Item = T>>`
/// - `Result<impl Iterator<Item = Result<Option<T>>>>`
///
/// Currently, table function arguments do not support the `Option` type. That is, the function will
/// only be invoked when all arguments are not null.
///
/// # Registration and Invocation
///
/// Every function defined by `#[function]` is automatically registered in the global function
/// table.
///
/// You can build expressions through the following functions:
///
/// ```ignore
/// // scalar functions
/// risingwave_expr::expr::build(...) -> BoxedExpression
/// risingwave_expr::expr::build_from_prost(...) -> BoxedExpression
/// // table functions
/// risingwave_expr::table_function::build(...) -> BoxedTableFunction
/// risingwave_expr::table_function::build_from_prost(...) -> BoxedTableFunction
/// ```
///
/// Or get their metadata through the following functions:
///
/// ```ignore
/// // scalar functions
/// risingwave_expr::sig::func::FUNC_SIG_MAP::get(...)
/// // table functions
/// risingwave_expr::sig::table_function::FUNC_SIG_MAP::get(...)
/// ```
///
/// # Appendix: Type Matrix
///
/// | name        | SQL type           | owned type    | reference type     | primitive? |
/// | ----------- | ------------------ | ------------- | ------------------ | ---------- |
/// | boolean     | `boolean`          | `bool`        | `bool`             | yes        |
/// | int16       | `smallint`         | `i16`         | `i16`              | yes        |
/// | int32       | `integer`          | `i32`         | `i32`              | yes        |
/// | int64       | `bigint`           | `i64`         | `i64`              | yes        |
/// | int256      | `rw_int256`        | `Int256`      | `Int256Ref<'_>`    | no         |
/// | float32     | `real`             | `F32`         | `F32`              | yes        |
/// | float64     | `double precision` | `F64`         | `F64`              | yes        |
/// | decimal     | `numeric`          | `Decimal`     | `Decimal`          | yes        |
/// | serial      | `serial`           | `Serial`      | `Serial`           | yes        |
/// | date        | `date`             | `Date`        | `Date`             | yes        |
/// | time        | `time`             | `Time`        | `Time`             | yes        |
/// | timestamp   | `timestamp`        | `Timestamp`   | `Timestamp`        | yes        |
/// | timestamptz | `timestamptz`      | `i64`         | `i64`              | yes        |
/// | interval    | `interval`         | `Interval`    | `Interval`         | yes        |
/// | varchar     | `varchar`          | `Box<str>`    | `&str`             | no         |
/// | bytea       | `bytea`            | `Box<[u8]>`   | `&[u8]`            | no         |
/// | jsonb       | `jsonb`            | `JsonbVal`    | `JsonbRef<'_>`     | no         |
/// | list        | `any[]`            | `ListValue`   | `ListRef<'_>`      | no         |
/// | struct      | `record`           | `StructValue` | `StructRef<'_>`    | no         |
/// | any         | `any`              | `ScalarImpl`  | `ScalarRef<'_>`    | no         |
///
/// [type matrix]: #appendix-type-matrix
#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as syn::AttributeArgs);
    let item = parse_macro_input!(item as syn::ItemFn);

    fn inner(attr: syn::AttributeArgs, mut item: syn::ItemFn) -> Result<TokenStream2> {
        let fn_attr = FunctionAttr::parse(&attr, &mut item)?;

        let mut tokens = item.into_token_stream();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_descriptor(false)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn build_function(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as syn::AttributeArgs);
    let item = parse_macro_input!(item as syn::ItemFn);

    fn inner(attr: syn::AttributeArgs, mut item: syn::ItemFn) -> Result<TokenStream2> {
        let fn_attr = FunctionAttr::parse(&attr, &mut item)?;

        let mut tokens = item.into_token_stream();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_descriptor(true)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as syn::AttributeArgs);
    let item = parse_macro_input!(item as syn::ItemFn);

    fn inner(attr: syn::AttributeArgs, mut item: syn::ItemFn) -> Result<TokenStream2> {
        let fn_attr = FunctionAttr::parse(&attr, &mut item)?;

        let mut tokens = item.into_token_stream();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_agg_descriptor(false)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn build_aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as syn::AttributeArgs);
    let item = parse_macro_input!(item as syn::ItemFn);

    fn inner(attr: syn::AttributeArgs, mut item: syn::ItemFn) -> Result<TokenStream2> {
        let fn_attr = FunctionAttr::parse(&attr, &mut item)?;

        let mut tokens = item.into_token_stream();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_agg_descriptor(true)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[derive(Debug, Clone)]
struct FunctionAttr {
    name: String,
    args: Vec<String>,
    ret: String,
    is_table_function: bool,
    batch_fn: Option<String>,
    state: Option<String>,
    init_state: Option<String>,
    prebuild: Option<String>,
    type_infer: Option<String>,
    user_fn: UserFunctionAttr,
}

#[derive(Debug, Clone)]
struct UserFunctionAttr {
    /// Function name
    name: String,
    /// The last argument type is `&mut dyn Write`.
    write: bool,
    /// The argument type are `Option`s.
    arg_option: bool,
    /// The return type.
    return_type: ReturnType,
    /// The inner type `T` in `impl Iterator<Item = T>`
    iterator_item_type: Option<ReturnType>,
    /// The number of generic types.
    generic: usize,
    /// The span of return type.
    return_type_span: proc_macro2::Span,
    // /// `#[list(0)]` in arguments.
    // list: Vec<(usize, usize)>,
    // /// `#[struct(0)]` in arguments.
    // struct_: Vec<(usize, usize)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ReturnType {
    T,
    Option,
    Result,
    ResultOption,
}

impl ReturnType {
    fn contains_result(&self) -> bool {
        matches!(self, ReturnType::Result | ReturnType::ResultOption)
    }

    fn contains_option(&self) -> bool {
        matches!(self, ReturnType::Option | ReturnType::ResultOption)
    }
}

impl FunctionAttr {
    /// Return a unique name that can be used as an identifier.
    fn ident_name(&self) -> String {
        format!("{}_{}_{}", self.name, self.args.join("_"), self.ret)
            .replace("[]", "list")
            .replace(['<', '>', ' ', ','], "_")
    }
}

impl UserFunctionAttr {
    fn is_writer_style(&self) -> bool {
        self.write && !self.arg_option
    }

    fn is_pure(&self) -> bool {
        !self.write && !self.arg_option && self.return_type == ReturnType::T
    }
}
