use honggfuzz::fuzz;
use risingwave_sqlparser::dialect::GenericDialect;
use risingwave_sqlparser::parser::Parser;

fn main() {
    loop {
        fuzz!(|data: String| {
            let dialect = GenericDialect {};
            let _ = Parser::parse_sql(&dialect, &data);
        });
    }
}
