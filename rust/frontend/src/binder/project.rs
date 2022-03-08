use risingwave_common::error::Result;
use risingwave_sqlparser::ast::SelectItem;

use crate::binder::Binder;
use crate::expr::ExprImpl;

impl Binder {
    pub fn bind_project(&mut self, select_items: Vec<SelectItem>) -> Result<Vec<ExprImpl>> {
        let mut select_list = vec![];
        for item in select_items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { .. } => todo!(),
                SelectItem::QualifiedWildcard(_) => todo!(),
                SelectItem::Wildcard => {
                    select_list.extend(self.bind_all_columns()?.into_iter());
                }
            }
        }
        Ok(select_list)
    }
}
