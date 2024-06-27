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

#![feature(register_tool)]
#![register_tool(rw)]
#![allow(rw::format_error)] // test code

use std::io;

use risingwave_sqlparser::parser::Parser;

/// Just read from stdin and print the AST
/// ```shell
/// echo "SELECT 1;" | cargo run --bin sqlparser
/// ```
fn main() {
    tracing_subscriber::fmt::init();

    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer).unwrap();
    let result = Parser::parse_sql(&buffer);
    match result {
        Ok(statements) => {
            for statement in statements {
                println!("{:#?}", statement);
            }
        }
        Err(e) => {
            eprintln!("{}", e);
        }
    }
}
