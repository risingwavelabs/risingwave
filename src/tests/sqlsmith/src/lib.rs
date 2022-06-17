use std::vec;

use rand::prelude::{SliceRandom, ThreadRng};
use rand::Rng;
use risingwave_frontend::expr::DataTypeName;
use risingwave_sqlparser::ast::{
    ColumnDef, Expr, Ident, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement,
    TableWithJoins, With,
};

mod expr;
mod test_runner;

#[allow(dead_code)]
pub struct Table {
    columns: Vec<ColumnDef>,
}

pub struct SqlGenerator {
    #[allow(dead_code)]
    tables: Vec<Table>,
    rng: ThreadRng,
}

impl SqlGenerator {
    pub fn new(tables: Vec<Table>) -> Self {
        let rng = rand::thread_rng();
        SqlGenerator { tables, rng }
    }

    pub fn gen(&mut self) -> String {
        format!("{}", self.gen_stmt())
    }

    pub fn gen_stmt(&mut self) -> Statement {
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

    fn gen_order_by(&self) -> Vec<OrderByExpr> {
        vec![]
    }

    fn gen_limit(&self) -> Option<Expr> {
        None
    }

    fn gen_select_stmt(&mut self) -> Select {
        Select {
            distinct: false,
            projection: self.gen_select_list(),
            from: self.gen_from(),
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
            // T::Date,
            // T::Timestamp,
            // T::Timestampz,
            // T::Time,
            // T::Interval,
        ]
        .choose(&mut self.rng)
        .unwrap();
        let items_num = self.rng.gen_range(1..=5);
        (0..items_num)
            .map(|i| SelectItem::ExprWithAlias {
                expr: self.gen_expr(ret_type),
                alias: Ident::new(format!("col_{}", i)),
            })
            .collect()
    }

    fn gen_from(&self) -> Vec<TableWithJoins> {
        vec![]
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
}
