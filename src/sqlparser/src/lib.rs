// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL Parser for Rust
//!
//! Example code:
//!
//! This crate provides an ANSI:SQL 2011 lexer and parser that can parse SQL
//! into an Abstract Syntax Tree (AST).
//!
//! ```
//! use risingwave_sqlparser::parser::Parser;
//!
//! let sql = "SELECT a, b, 123, myfunc(b) \
//!            FROM table_1 \
//!            WHERE a > b AND b < 100 \
//!            ORDER BY a DESC, b";
//!
//! let ast = Parser::parse_sql(sql).unwrap();
//!
//! println!("AST: {:?}", ast);
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![feature(lint_reasons)]
#![expect(clippy::derive_partial_eq_without_eq)]
#![expect(clippy::doc_markdown)]

#[cfg(not(feature = "std"))]
extern crate alloc;

pub mod ast;
pub mod keywords;
pub mod parser;
pub mod tokenizer;

#[doc(hidden)]
// This is required to make utilities accessible by both the crate-internal
// unit-tests and by the integration tests <https://stackoverflow.com/a/44541071/1026>
// External users are not supposed to rely on this module.
pub mod test_utils;
