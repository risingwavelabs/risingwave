use risingwave_sqlparser::parser::Parser;
use std::io;
fn main() {
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer).unwrap();
    let result = Parser::parse_sql(&buffer);
    println!("{:#?}", result);
}