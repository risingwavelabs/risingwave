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
    fn is_writer_style(&self) -> bool {
        self.write && !self.arg_option && self.return_result
    }

    fn is_pure(&self) -> bool {
        !self.write && !self.arg_option && !self.return_option && !self.return_result
    }
}
