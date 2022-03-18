use std::collections::hash_map::Entry;

use risingwave_common::catalog::{CellBasedTableDesc, ColumnDesc, DEFAULT_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use risingwave_sqlparser::ast::{
    JoinConstraint, JoinOperator, ObjectName, Query, TableFactor, TableWithJoins,
};

use super::bind_context::ColumnBinding;
use super::{BoundQuery, UNNAMED_SUBQUERY};
use crate::binder::Binder;
use crate::catalog::TableId;
use crate::expr::{Expr, ExprImpl};

#[derive(Debug)]
pub enum TableRef {
    BaseTable(Box<BaseTableRef>),
    SubQuery(Box<SubQuery>),
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
    pub cell_based_desc: CellBasedTableDesc,
    pub columns: Vec<ColumnDesc>,
}

#[derive(Debug)]
pub struct SubQuery {
    pub query: BoundQuery,
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
            TableFactor::Derived {
                lateral, subquery, ..
            } => {
                if lateral {
                    Err(ErrorCode::NotImplementedError("unsupported lateral".into()).into())
                } else {
                    Ok(TableRef::SubQuery(Box::new(self.bind_subquery(*subquery)?)))
                }
            }
            _ => Err(ErrorCode::NotImplementedError(format!(
                "unsupported table factor {:?}",
                table_factor
            ))
            .into()),
        }
    }

    /// return the (schema_name, table_name)
    pub fn resolve_table_name(name: ObjectName) -> Result<(String, String)> {
        let mut identifiers = name.0;
        let table_name = identifiers
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("empty table name".into()))?
            .value;

        let schema_name = identifiers
            .pop()
            .map(|ident| ident.value)
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.into());

        Ok((schema_name, table_name))
    }
    pub(super) fn bind_table(&mut self, name: ObjectName) -> Result<BaseTableRef> {
        let (schema_name, table_name) = Self::resolve_table_name(name)?;
        let table_catalog = {
            let schema_catalog = self
                .get_schema_by_name(&schema_name)
                .ok_or_else(|| ErrorCode::ItemNotFound(format!("schema \"{}\"", schema_name)))?;
            schema_catalog
                .get_table_by_name(&table_name)
                .ok_or_else(|| ErrorCode::ItemNotFound(format!("relation \"{}\"", table_name)))?
                .clone()
        };

        let table_id = table_catalog.id();
        let cell_based_desc = table_catalog.cell_based_table();
        let columns = table_catalog.columns().to_vec();

        let columns = columns
            .into_iter()
            .map(|c| c.column_desc.unwrap().into())
            .collect::<Vec<ColumnDesc>>();
        self.bind_context(
            columns.iter().cloned().map(|c| (c.name, c.data_type)),
            table_name.clone(),
        )?;

        Ok(BaseTableRef {
            name: table_name,
            cell_based_desc,
            table_id,
            columns,
        })
    }

    /// Fill the BindContext for table.
    fn bind_context(
        &mut self,
        columns: impl IntoIterator<Item = (String, DataType)>,
        table_name: String,
    ) -> Result<()> {
        let begin = self.context.columns.len();
        columns
            .into_iter()
            .enumerate()
            .for_each(|(index, (name, data_type))| {
                self.context.columns.push(ColumnBinding::new(
                    table_name.clone(),
                    name.clone(),
                    begin + index,
                    data_type,
                ));
                self.context
                    .indexs_of
                    .entry(name)
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

    /// Before binding a subquery, we push the current context to the stack and create a new
    /// context.
    ///
    /// After finishing binding, we pop the previous context from the stack. And
    /// update it with the output of the subquery.
    pub(super) fn bind_subquery(&mut self, query: Query) -> Result<SubQuery> {
        self.push_context();
        let query = self.bind_query(query)?;
        self.pop_context();
        self.bind_context(
            itertools::zip_eq(query.names().into_iter(), query.data_types().into_iter()),
            UNNAMED_SUBQUERY.to_string(),
        )?;
        Ok(SubQuery { query })
    }
}
