use std::io;

use risingwave_sqlparser::parser::Parser;
fn main() {
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer).unwrap();
    let result = Parser::parse_sql(&buffer);
    println!("{:#?}", result);
}
