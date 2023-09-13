use std::fmt::Write;

use risingwave_expr::ExprError;
use risingwave_expr_macro::function;

#[function("col_description(varchar, int32) -> varchar")]
fn col_description(_name: &str, _col: i32, writer: &mut impl Write) -> Result<(), ExprError> {
    writer.write_str("").unwrap();

    Ok(())
}
