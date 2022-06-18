// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::vec;

use rand::prelude::{SliceRandom, ThreadRng};
use rand::Rng;
use risingwave_frontend::expr::DataTypeName;
use risingwave_sqlparser::ast::{
    ColumnDef, Expr, Ident, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement,
    TableWithJoins, With,
};

mod expr;
mod relation;
mod scalar;

#[derive(Clone)]
pub struct Table {
    pub name: String,

    pub columns: Vec<ColumnDef>,
}

struct SqlGenerator<'a> {
    tables: Vec<Table>,
    rng: &'a mut ThreadRng,

    bound_relations: Vec<Table>,
}

impl<'a> SqlGenerator<'a> {
    fn new(rng: &'a mut ThreadRng, tables: Vec<Table>) -> Self {
        SqlGenerator {
            tables,
            rng,
            bound_relations: vec![],
        }
    }

    fn gen_stmt(&mut self) -> Statement {
        Statement::Query(Box::new(self.gen_query()))
    }

    fn gen_query(&mut self) -> Query {
        Query {
            with: self.gen_with(),
            body: self.gen_set_expr(),
            order_by: self.gen_order_by(),
            limit: self.gen_limit(),
            offset: None,
            fetch: None,
        }
    }

    fn gen_with(&mut self) -> Option<With> {
        None
    }

    fn gen_set_expr(&mut self) -> SetExpr {
        match self.rng.gen_range(0..=9) {
            0..=9 => SetExpr::Select(Box::new(self.gen_select_stmt())),
            _ => unreachable!(),
        }
    }

    fn gen_order_by(&mut self) -> Vec<OrderByExpr> {
        if self.bound_relations.is_empty() {
            return vec![];
        }
        let mut order_by = vec![];
        while self.flip_coin() {
            let table = self.bound_relations.choose(&mut self.rng).unwrap();
            let column = table.columns.choose(&mut self.rng).unwrap();
            order_by.push(OrderByExpr {
                expr: Expr::Identifier(Ident::new(format!("{}.{}", table.name, column.name))),
                asc: Some(self.rng.gen_bool(0.5)),
                nulls_first: None,
            })
        }
        order_by
    }

    fn gen_limit(&self) -> Option<Expr> {
        None
    }

    fn gen_select_stmt(&mut self) -> Select {
        // Generate random tables/relations first so that select items can refer to them.
        let from = self.gen_from();
        Select {
            distinct: false,
            projection: self.gen_select_list(),
            from,
            lateral_views: vec![],
            selection: self.gen_where(),
            group_by: self.gen_group_by(),
            having: self.gen_having(),
        }
    }

    fn gen_select_list(&mut self) -> Vec<SelectItem> {
        use DataTypeName as T;
        let ret_type = *[
            T::Boolean,
            T::Int16,
            T::Int32,
            T::Int64,
            T::Decimal,
            T::Float32,
            T::Float64,
            T::Varchar,
            T::Date,
            T::Timestamp,
            T::Timestampz,
            T::Time,
            T::Interval,
        ]
        .choose(&mut self.rng)
        .unwrap();
        let items_num = self.rng.gen_range(1..=4);
        (0..items_num)
            .map(|i| SelectItem::ExprWithAlias {
                expr: self.gen_expr(ret_type),
                alias: Ident::new(format!("col_{}", i)),
            })
            .collect()
    }

    fn gen_from(&mut self) -> Vec<TableWithJoins> {
        (0..self.tables.len())
            .filter_map(|_| {
                if self.flip_coin() {
                    Some(self.gen_from_relation())
                } else {
                    None
                }
            })
            .collect()
    }

    fn gen_where(&self) -> Option<Expr> {
        None
    }

    fn gen_group_by(&self) -> Vec<Expr> {
        vec![]
    }

    fn gen_having(&self) -> Option<Expr> {
        None
    }

    /// 50/50 chance to be true/false.
    fn flip_coin(&mut self) -> bool {
        self.rng.gen_bool(0.5)
    }
}

/// Generate a random SQL string.
pub fn sql_gen(rng: &mut ThreadRng, tables: Vec<Table>) -> String {
    let mut gen = SqlGenerator::new(rng, tables);
    format!("{}", gen.gen_stmt())
}
