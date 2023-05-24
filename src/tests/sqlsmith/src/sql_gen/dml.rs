// Copyright 2023 RisingWave Labs
//
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

use std::iter;

use anyhow::{bail, Result};
use itertools::Itertools;
use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Expr::BinaryOp;
use risingwave_sqlparser::ast::{
    Assignment, AssignmentValue, BinaryOperator, Expr, ObjectName, Query, SetExpr, Statement,
    Values,
};

use crate::sql_gen::SqlGenerator;
use crate::Table;

impl<'a, R: Rng + 'a> SqlGenerator<'a, R> {
    pub(crate) fn generate_insert_statement(
        &mut self,
        table: &Table,
        row_count: usize,
    ) -> Statement {
        let table_name = ObjectName(vec![table.name.as_str().into()]);
        let data_types = table
            .columns
            .iter()
            .cloned()
            .map(|c| c.data_type)
            .collect_vec();
        let values = self.gen_values(&data_types, row_count);
        let source = Query {
            with: None,
            body: SetExpr::Values(Values(values)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        };
        Statement::Insert {
            table_name,
            columns: vec![],
            source: Box::new(source),
            returning: vec![],
        }
    }

    pub(crate) fn generate_update_statements(
        &mut self,
        tables: &[Table],
        inserts: &[Statement],
    ) -> Result<Vec<Statement>> {
        let mut updates = vec![];
        for insert in inserts {
            match insert {
                Statement::Insert {
                    table_name, source, ..
                } => {
                    let values = Self::extract_insert_values(source)?;
                    let table = tables
                        .iter()
                        .find(|table| table.name == table_name.real_value())
                        .expect("Inserted values should always have an existing table");
                    let pk_indices = &table.pk_indices;
                    let mut updates_for_insert =
                        self.generate_update_statements_inner(table, values, pk_indices);
                    updates.append(&mut updates_for_insert);
                }
                _ => bail!("Should only have insert statements"),
            }
        }
        Ok(updates)
    }

    pub(crate) fn generate_update_statements_inner(
        &mut self,
        table: &Table,
        values: &[Vec<Expr>],
        pk_indices: &[usize],
    ) -> Vec<Statement> {
        let data_types = table
            .columns
            .iter()
            .cloned()
            .map(|c| c.data_type)
            .collect_vec();
        if pk_indices.is_empty() {
            // do delete for a random subset of rows.
            let delete_statements = self.generate_delete_statements(table, values);
            // then insert back some number of rows.
            let insert_statement = self.generate_insert_statement(table, delete_statements.len());
            delete_statements
                .into_iter()
                .chain(iter::once(insert_statement))
                .collect()
        } else {
            let value_indices = (0..table.columns.len())
                .filter(|i| !pk_indices.contains(i))
                .collect_vec();
            let update_values = values
                .iter()
                .filter_map(|row| {
                    if self.rng.gen_bool(0.2) {
                        let mut updated_row = row.clone();
                        for value_index in &value_indices {
                            let data_type = &data_types[*value_index];
                            updated_row[*value_index] = self.gen_simple_scalar(data_type)
                        }
                        Some(updated_row)
                    } else {
                        None
                    }
                })
                .collect_vec();
            let update_statements = update_values
                .iter()
                .map(|row| Self::row_to_update_statement(table, pk_indices, &value_indices, row))
                .collect_vec();
            update_statements
        }
    }

    fn row_to_update_statement(
        table: &Table,
        pk_indices: &[usize],
        value_indices: &[usize],
        row: &[Expr],
    ) -> Statement {
        let assignments = value_indices
            .iter()
            .copied()
            .map(|i| {
                let name = table.columns[i].name.as_str();
                let id = vec![name.into()];
                let value = AssignmentValue::Expr(row[i].clone());
                Assignment { id, value }
            })
            .collect_vec();
        Statement::Update {
            table_name: ObjectName::from_test_str(&table.name),
            assignments,
            selection: Some(Self::create_selection_expr(table, pk_indices, row)),
            returning: vec![],
        }
    }

    fn create_selection_expr(table: &Table, selected_indices: &[usize], row: &[Expr]) -> Expr {
        assert!(!selected_indices.is_empty());
        let match_exprs = selected_indices
            .iter()
            .copied()
            .map(|i| {
                let match_val = row[i].clone();
                let match_col = Expr::Identifier(table.columns[i].name.as_str().into());
                
                Expr::BinaryOp {
                    left: Box::new(match_col),
                    op: BinaryOperator::Eq,
                    right: Box::new(match_val),
                }
            })
            .collect_vec();
        match_exprs
            .into_iter()
            .reduce(|l, r| BinaryOp {
                left: Box::new(l),
                op: BinaryOperator::And,
                right: Box::new(r),
            })
            .expect("pk should be non empty")
    }

    fn generate_delete_statements(
        &mut self,
        table: &Table,
        values: &[Vec<Expr>],
    ) -> Vec<Statement> {
        let selected = (0..table.columns.len()).collect_vec();
        values
            .iter()
            .filter_map(|row| {
                if self.rng.gen_bool(0.2) {
                    let selection = Some(Self::create_selection_expr(table, &selected, row));
                    Some(Statement::Delete {
                        table_name: ObjectName::from_test_str(&table.name),
                        selection,
                        returning: vec![],
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    fn extract_insert_values(source: &Query) -> Result<&[Vec<Expr>]> {
        let body = &source.body;
        match body {
            SetExpr::Values(values) => Ok(&values.0),
            _ => bail!("Should not have insert values"),
        }
    }

    fn gen_values(&mut self, data_types: &[DataType], row_count: usize) -> Vec<Vec<Expr>> {
        (0..row_count).map(|_| self.gen_row(data_types)).collect()
    }

    fn gen_row(&mut self, data_types: &[DataType]) -> Vec<Expr> {
        data_types
            .iter()
            .map(|typ| self.gen_simple_scalar(typ))
            .collect()
    }
}
