#![allow(rw::format_error)]

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
            println!("{}", e);
        }
    }
}
