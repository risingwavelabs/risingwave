use std::io::BufRead;

use risingwave_sqlparser::parser::*;

/// Input SQL, output AST.
fn main() {
    let mut sql = String::new();
    #[allow(clippy::significant_drop_in_scrutinee)]
    for line in std::io::stdin().lock().lines() {
        sql += &line.unwrap();
        if !sql.ends_with(';') {
            continue;
        }
        let ast = Parser::parse_sql(&sql).unwrap();
        println!("{:?}", ast);
    }
}
