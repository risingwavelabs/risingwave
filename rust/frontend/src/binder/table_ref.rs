use std::collections::hash_map::Entry;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{
    JoinConstraint, JoinOperator, ObjectName, TableFactor, TableWithJoins,
};

use super::bind_context::ColumnBinding;
use crate::binder::Binder;
use crate::catalog::catalog_service::DEFAULT_SCHEMA_NAME;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::TableId;
use crate::expr::{Expr, ExprImpl};

#[derive(Debug)]
pub enum TableRef {
    BaseTable(Box<BaseTableRef>),
    Join(Box<BoundJoin>),
}

#[derive(Debug)]
pub struct BoundJoin {
    pub left: TableRef,
    pub right: TableRef,
    pub cond: ExprImpl,
}

#[derive(Debug, Clone)]
pub struct BaseTableRef {
    pub name: String, // explain-only
    pub table_id: TableId,
    pub columns: Vec<ColumnCatalog>,
}

impl Binder {
    pub(super) fn bind_vec_table_with_joins(
        &mut self,
        from: Vec<TableWithJoins>,
    ) -> Result<Option<TableRef>> {
        let mut from_iter = from.into_iter();
        let first = match from_iter.next() {
            Some(t) => t,
            None => return Ok(None),
        };
        let mut root = self.bind_table_with_joins(first)?;
        for t in from_iter {
            let right = self.bind_table_with_joins(t)?;
            root = TableRef::Join(Box::new(BoundJoin {
                left: root,
                right,
                cond: ExprImpl::literal_bool(true),
            }));
        }
        Ok(Some(root))
    }

    fn bind_table_with_joins(&mut self, table: TableWithJoins) -> Result<TableRef> {
        let mut root = self.bind_table_factor(table.relation)?;
        for join in table.joins {
            let right = self.bind_table_factor(join.relation)?;
            match join.join_operator {
                JoinOperator::Inner(constraint) => {
                    let cond = self.bind_join_constraint(constraint)?;
                    let join = BoundJoin {
                        left: root,
                        right,
                        cond,
                    };
                    root = TableRef::Join(Box::new(join));
                }
                _ => return Err(ErrorCode::NotImplementedError("Non inner-join".into()).into()),
            }
        }

        Ok(root)
    }

    fn bind_join_constraint(&mut self, constraint: JoinConstraint) -> Result<ExprImpl> {
        Ok(match constraint {
            JoinConstraint::None => ExprImpl::literal_bool(true),
            JoinConstraint::Natural => {
                return Err(ErrorCode::NotImplementedError("Natural join".into()).into())
            }
            JoinConstraint::On(expr) => {
                let bound_expr = self.bind_expr(expr)?;
                if bound_expr.return_type() != DataType::Boolean {
                    return Err(ErrorCode::InternalError(format!(
                        "argument of ON must be boolean, not type {:?}",
                        bound_expr.return_type()
                    ))
                    .into());
                }
                bound_expr
            }
            JoinConstraint::Using(_columns) => {
                return Err(ErrorCode::NotImplementedError("USING".into()).into())
            }
        })
    }

    pub(super) fn bind_table_factor(&mut self, table_factor: TableFactor) -> Result<TableRef> {
        match table_factor {
            TableFactor::Table { name, .. } => {
                Ok(TableRef::BaseTable(Box::new(self.bind_table(name)?)))
            }
            _ => Err(ErrorCode::NotImplementedError(format!(
                "unsupported table factor {:?}",
                table_factor
            ))
            .into()),
        }
    }

    pub(super) fn bind_table(&mut self, name: ObjectName) -> Result<BaseTableRef> {
        let mut identifiers = name.0;
        let table_name = identifiers
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("empty table name".into()))?
            .value;
        let schema_name = identifiers
            .pop()
            .map(|ident| ident.value)
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.into());

        let table_catalog = self
            .catalog
            .get_schema(&schema_name)
            .and_then(|c| c.get_table(&table_name))
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("relation \"{}\"", table_name)))?;
        let columns = table_catalog.columns().to_vec();
        let table_id = table_catalog.id();

        self.bind_context(&columns, table_name.clone())?;

        Ok(BaseTableRef {
            name: table_name,
            table_id,
            columns,
        })
    }

    /// Fill the BindContext for table.
    fn bind_context(&mut self, columns: &[ColumnCatalog], table_name: String) -> Result<()> {
        let begin = self.context.columns.len();
        columns
            .iter()
            .enumerate()
            .for_each(|(index, column_catalog)| {
                self.context.columns.push(ColumnBinding::new(
                    table_name.clone(),
                    column_catalog.name().into(),
                    begin + index,
                    column_catalog.data_type(),
                ));
                self.context
                    .indexs_of
                    .entry(column_catalog.name().to_string())
                    .or_default()
                    .push(self.context.columns.len() - 1);
            });

        match self.context.range_of.entry(table_name.clone()) {
            Entry::Occupied(_) => Err(ErrorCode::InternalError(format!(
                "Duplicated table name: {}",
                table_name
            ))
            .into()),
            Entry::Vacant(entry) => {
                entry.insert((begin, self.context.columns.len()));
                Ok(())
            }
        }
    }
}
