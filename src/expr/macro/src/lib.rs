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
            .replace(&['<', '>', ' ', ','], "_")
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
