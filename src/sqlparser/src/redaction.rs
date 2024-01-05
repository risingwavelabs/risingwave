// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;

use crate::ast::{
    ColumnDef, ColumnOption, CreateSink, CreateSinkStatement, CreateSourceStatement, Distinct,
    Expr, Function, FunctionArg, FunctionArgExpr, Ident, Join, JoinConstraint, JoinOperator,
    ObjectName, Query, Select, SelectItem, SetExpr, SourceWatermark, SqlOption, Statement,
    TableAlias, TableConstraint, TableFactor, TableWithJoins, Value, WindowSpec,
};

/// Returns statements with redacted idents and `with_options`.
///
/// Note that only `CREATE` statements are redacted currently.
pub fn redact_statement(stmt: &Statement, context: &mut MaskingContext) -> Statement {
    let mut masked = stmt.clone();
    match &mut masked {
        Statement::CreateView {
            name,
            columns,
            query,
            with_options,
            ..
        } => {
            name.mask(context);
            columns.mask(context);
            query.mask(context);
            with_options.mask(context);
        }
        Statement::CreateTable {
            name,
            columns,
            query,
            constraints,
            source_watermarks,
            cdc_table_info,
            include_column_options,
            with_options,
            ..
        } => {
            name.mask(context);
            columns.iter_mut().for_each(|c| {
                c.mask(context);
            });
            if let Some(q) = query {
                q.mask(context);
            }
            constraints.iter_mut().for_each(|c| {
                c.mask(context);
            });
            source_watermarks.iter_mut().for_each(|source_watermark| {
                source_watermark.mask(context);
            });
            if let Some(cdc_table_info) = cdc_table_info {
                cdc_table_info.source_name.mask(context);
                cdc_table_info.external_table_name.mask(context)
            }
            include_column_options.iter_mut().for_each(|(i1, i2)| {
                i1.mask(context);
                i2.mask(context);
            });
            with_options.mask(context);
        }
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            include,
            distributed_by,
            ..
        } => {
            name.mask(context);
            table_name.mask(context);
            columns.iter_mut().for_each(|o| o.expr.mask(context));
            include.mask(context);
            distributed_by.mask(context);
        }
        Statement::CreateSource {
            stmt:
                CreateSourceStatement {
                    columns,
                    constraints,
                    source_name,
                    source_watermarks,
                    include_column_options,
                    with_properties,
                    ..
                },
        } => {
            source_name.mask(context);
            columns.iter_mut().for_each(|c| {
                c.mask(context);
            });
            constraints.iter_mut().for_each(|c| {
                c.mask(context);
            });
            source_watermarks.iter_mut().for_each(|source_watermark| {
                source_watermark.mask(context);
            });
            include_column_options.iter_mut().for_each(|(i1, i2)| {
                i1.mask(context);
                i2.mask(context);
            });
            with_properties.0.mask(context);
        }
        Statement::CreateSink {
            stmt:
                CreateSinkStatement {
                    sink_name,
                    sink_from,
                    columns,
                    into_table_name,
                    with_properties,
                    ..
                },
        } => {
            sink_name.mask(context);
            match sink_from {
                CreateSink::From(i) => i.mask(context),
                CreateSink::AsQuery(q) => q.mask(context),
            }
            columns.mask(context);
            into_table_name.mask(context);
            with_properties.0.mask(context);
        }
        _ => {}
    }
    masked
}

#[derive(Default)]
pub struct MaskingContext {
    seq: u64,
    masking: HashMap<String, u64>,
}

impl MaskingContext {
    fn seq(&mut self, s: &str) -> u64 {
        let seq = self.masking.entry(s.to_owned()).or_insert_with(|| {
            self.seq += 1;
            self.seq
        });
        *seq
    }
}

trait Masking {
    fn mask(&mut self, context: &mut MaskingContext);
}

impl Masking for String {
    fn mask(&mut self, context: &mut MaskingContext) {
        *self = format!("_{}_", context.seq(self))
    }
}

impl Masking for Ident {
    fn mask(&mut self, context: &mut MaskingContext) {
        let seq = context.seq(&self.value);
        self.value = format!("_{seq}_",);
    }
}

impl Masking for Option<Ident> {
    fn mask(&mut self, context: &mut MaskingContext) {
        if let Some(i) = self {
            i.mask(context);
        }
    }
}

impl Masking for Vec<Ident> {
    fn mask(&mut self, context: &mut MaskingContext) {
        for i in self.iter_mut() {
            i.mask(context);
        }
    }
}

impl Masking for Vec<Expr> {
    fn mask(&mut self, context: &mut MaskingContext) {
        for e in self.iter_mut() {
            e.mask(context);
        }
    }
}

impl Masking for Option<Expr> {
    fn mask(&mut self, context: &mut MaskingContext) {
        if let Some(e) = self {
            e.mask(context);
        }
    }
}

impl Masking for Option<Box<Expr>> {
    fn mask(&mut self, context: &mut MaskingContext) {
        if let Some(e) = self {
            e.mask(context);
        }
    }
}

impl Masking for Expr {
    fn mask(&mut self, context: &mut MaskingContext) {
        match self {
            Expr::Identifier(i) => {
                i.mask(context);
            }
            Expr::CompoundIdentifier(i) => {
                i.mask(context);
            }
            Expr::FieldIdentifier(e, i) => {
                e.mask(context);
                i.mask(context);
            }
            Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsJson { expr, .. }
            | Expr::SomeOp(expr)
            | Expr::AllOp(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::TryCast { expr, .. }
            | Expr::AtTimeZone {
                timestamp: expr, ..
            }
            | Expr::Extract { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::Nested(expr) => expr.mask(context),
            Expr::IsDistinctFrom(e1, e2) | Expr::IsNotDistinctFrom(e1, e2) => {
                e1.mask(context);
                e2.mask(context);
            }
            Expr::InList { expr, list, .. } => {
                expr.mask(context);
                list.mask(context);
            }
            Expr::InSubquery { expr, subquery, .. } => {
                expr.mask(context);
                subquery.mask(context);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                expr.mask(context);
                low.mask(context);
                high.mask(context);
            }
            Expr::SimilarTo {
                expr,
                pat,
                esc_text,
                ..
            } => {
                expr.mask(context);
                pat.mask(context);
                esc_text.mask(context);
            }
            Expr::BinaryOp { left, right, .. } => {
                left.mask(context);
                right.mask(context);
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                expr.mask(context);
                substring_from.mask(context);
                substring_for.mask(context);
            }
            Expr::Position { substring, string } => {
                substring.mask(context);
                string.mask(context);
            }
            Expr::Overlay {
                expr,
                new_substring,
                start,
                count,
            } => {
                expr.mask(context);
                new_substring.mask(context);
                start.mask(context);
                count.mask(context);
            }
            Expr::Trim {
                expr, trim_what, ..
            } => {
                expr.mask(context);
                trim_what.mask(context);
            }
            Expr::Value(_) => {}
            Expr::Parameter { .. } => {}
            Expr::TypedString { .. } => {}
            Expr::Function(func) => {
                func.mask(context);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                operand.mask(context);
                conditions.mask(context);
                results.mask(context);
                else_result.mask(context);
            }
            Expr::Exists(query) => {
                query.mask(context);
            }
            Expr::Subquery(query) => {
                query.mask(context);
            }
            Expr::GroupingSets(expr_vec_vec)
            | Expr::Cube(expr_vec_vec)
            | Expr::Rollup(expr_vec_vec) => {
                expr_vec_vec
                    .iter_mut()
                    .for_each(|expr_vec| expr_vec.mask(context));
            }
            Expr::Row(expr_vec) => {
                expr_vec.mask(context);
            }
            Expr::Array(array) => array.elem.mask(context),
            Expr::ArraySubquery(query) => {
                query.mask(context);
            }
            Expr::ArrayIndex { obj, index } => {
                obj.mask(context);
                index.mask(context);
            }
            Expr::ArrayRangeIndex { obj, start, end } => {
                obj.mask(context);
                start.mask(context);
                end.mask(context);
            }
            Expr::LambdaFunction { args, body } => {
                args.mask(context);
                body.mask(context);
            }
        }
    }
}

impl Masking for Query {
    fn mask(&mut self, context: &mut MaskingContext) {
        let Query {
            with,
            body,
            order_by,
            ..
        } = self;
        if let Some(with) = with {
            for cte in &mut with.cte_tables {
                cte.alias.mask(context);
                cte.query.mask(context);
                cte.from.mask(context);
            }
        }
        body.mask(context);
        order_by.iter_mut().for_each(|e| e.expr.mask(context));
    }
}

impl Masking for SetExpr {
    fn mask(&mut self, context: &mut MaskingContext) {
        match self {
            SetExpr::Select(q) => {
                q.mask(context);
            }
            SetExpr::Query(q) => {
                q.mask(context);
            }
            SetExpr::SetOperation { left, right, .. } => {
                left.mask(context);
                right.mask(context);
            }
            SetExpr::Values(v) => {
                for expr_vec in &mut v.0 {
                    expr_vec.mask(context);
                }
            }
        }
    }
}

impl Masking for Select {
    fn mask(&mut self, context: &mut MaskingContext) {
        let Select {
            distinct,
            projection,
            from,
            lateral_views,
            selection,
            group_by,
            having,
        } = self;
        if let Distinct::DistinctOn(expr_vec) = distinct {
            expr_vec.mask(context);
        }
        for p in projection.iter_mut() {
            match p {
                SelectItem::UnnamedExpr(expr) => {
                    expr.mask(context);
                }
                SelectItem::ExprQualifiedWildcard(expr, idents) => {
                    expr.mask(context);
                    idents.mask(context);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    expr.mask(context);
                    alias.mask(context)
                }
                SelectItem::QualifiedWildcard(name, expr_vec) => {
                    name.mask(context);
                    if let Some(e) = expr_vec {
                        e.mask(context);
                    }
                }
                SelectItem::Wildcard(expr_vec) => {
                    if let Some(e) = expr_vec {
                        e.mask(context);
                    }
                }
            }
        }
        for l in lateral_views.iter_mut() {
            l.lateral_view.mask(context);
            l.lateral_view_name.mask(context);
            l.lateral_col_alias.mask(context);
        }
        selection.mask(context);
        group_by.mask(context);
        having.mask(context);

        for f in from.iter_mut() {
            f.mask(context);
        }
    }
}

impl Masking for ColumnDef {
    fn mask(&mut self, context: &mut MaskingContext) {
        self.name.mask(context);
        for opt in &mut self.options {
            opt.name.mask(context);
            opt.option.mask(context);
        }
    }
}

impl Masking for TableConstraint {
    fn mask(&mut self, context: &mut MaskingContext) {
        match self {
            TableConstraint::Unique { name, columns, .. } => {
                name.mask(context);
                columns.mask(context);
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                ..
            } => {
                name.mask(context);
                columns.mask(context);
                foreign_table.mask(context);
                referred_columns.mask(context);
            }
            TableConstraint::Check { name, expr } => {
                name.mask(context);
                expr.mask(context);
            }
        }
    }
}

impl Masking for SourceWatermark {
    fn mask(&mut self, context: &mut MaskingContext) {
        self.column.mask(context);
        self.expr.mask(context);
    }
}

impl Masking for ColumnOption {
    fn mask(&mut self, context: &mut MaskingContext) {
        match self {
            ColumnOption::DefaultColumns(expr)
            | ColumnOption::Check(expr)
            | ColumnOption::GeneratedColumns(expr) => {
                expr.mask(context);
            }
            ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                ..
            } => {
                foreign_table.mask(context);
                referred_columns.mask(context);
            }
            ColumnOption::Null => {}
            ColumnOption::NotNull => {}
            ColumnOption::Unique { .. } => {}
            ColumnOption::DialectSpecific(_) => {}
        }
    }
}

impl Masking for Vec<SqlOption> {
    fn mask<'a>(&mut self, _context: &mut MaskingContext) {
        for sql_option in self {
            sql_option.value = Value::SingleQuotedString("[REDACTED]".to_string());
        }
    }
}

impl Masking for TableFactor {
    fn mask(&mut self, context: &mut MaskingContext) {
        match self {
            TableFactor::Table { name, alias, .. } => {
                name.mask(context);
                if let Some(a) = alias {
                    a.mask(context);
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                subquery.mask(context);
                if let Some(a) = alias {
                    a.mask(context);
                }
            }
            TableFactor::TableFunction {
                name, alias, args, ..
            } => {
                name.mask(context);
                if let Some(a) = alias {
                    a.mask(context);
                }
                args.iter_mut().for_each(|a| a.mask(context));
            }
            TableFactor::NestedJoin(join) => {
                join.mask(context);
            }
        }
    }
}

impl Masking for TableWithJoins {
    fn mask(&mut self, context: &mut MaskingContext) {
        let TableWithJoins { relation, joins } = self;
        relation.mask(context);
        for join in joins.iter_mut() {
            join.mask(context);
        }
    }
}

impl Masking for Join {
    fn mask(&mut self, context: &mut MaskingContext) {
        self.relation.mask(context);
        match &mut self.join_operator {
            JoinOperator::Inner(join_constraint)
            | JoinOperator::LeftOuter(join_constraint)
            | JoinOperator::RightOuter(join_constraint)
            | JoinOperator::FullOuter(join_constraint) => match join_constraint {
                JoinConstraint::On(expr) => {
                    expr.mask(context);
                }
                JoinConstraint::Using(idents) => {
                    idents.mask(context);
                }
                JoinConstraint::Natural => {}
                JoinConstraint::None => {}
            },
            JoinOperator::CrossJoin => {}
        }
    }
}

impl Masking for TableAlias {
    fn mask(&mut self, context: &mut MaskingContext) {
        self.name.mask(context);
        self.columns.mask(context);
    }
}

impl Masking for ObjectName {
    fn mask(&mut self, context: &mut MaskingContext) {
        self.0.mask(context);
    }
}

impl Masking for Option<ObjectName> {
    fn mask(&mut self, context: &mut MaskingContext) {
        if let Some(o) = self {
            o.mask(context);
        }
    }
}

impl Masking for FunctionArg {
    fn mask(&mut self, context: &mut MaskingContext) {
        match self {
            FunctionArg::Named { name, arg } => {
                name.mask(context);
                arg.mask(context);
            }
            FunctionArg::Unnamed(arg) => {
                arg.mask(context);
            }
        }
    }
}

impl Masking for FunctionArgExpr {
    fn mask(&mut self, context: &mut MaskingContext) {
        match self {
            FunctionArgExpr::Expr(expr) => {
                expr.mask(context);
            }
            FunctionArgExpr::ExprQualifiedWildcard(expr, idents) => {
                expr.mask(context);
                idents.mask(context);
            }
            FunctionArgExpr::QualifiedWildcard(object_name, expr_vec) => {
                object_name.mask(context);
                if let Some(e) = expr_vec {
                    e.mask(context);
                }
            }
            FunctionArgExpr::Wildcard(expr_vec) => {
                if let Some(e) = expr_vec {
                    e.mask(context);
                }
            }
        }
    }
}

impl Masking for Function {
    fn mask(&mut self, context: &mut MaskingContext) {
        let Function {
            args,
            over,
            order_by,
            filter,
            within_group,
            ..
        } = self;
        args.iter_mut().for_each(|arg| arg.mask(context));
        if let Some(window_spec) = over {
            window_spec.mask(context);
        }
        order_by.iter_mut().for_each(|e| e.expr.mask(context));
        if let Some(expr) = filter {
            expr.mask(context);
        }
        if let Some(expr) = within_group {
            expr.expr.mask(context);
        }
    }
}

impl Masking for WindowSpec {
    fn mask(&mut self, context: &mut MaskingContext) {
        let WindowSpec {
            partition_by,
            order_by,
            ..
        } = self;
        partition_by.mask(context);
        order_by.iter_mut().for_each(|e| e.expr.mask(context));
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Statement;
    use crate::redaction::{redact_statement, MaskingContext};

    fn statement(sql: &str) -> Statement {
        use itertools::Itertools;
        crate::parser::Parser::parse_sql(sql)
            .unwrap()
            .into_iter()
            .exactly_one()
            .unwrap()
    }

    #[test]
    fn test_masking() {
        let create_table = r#"
        CREATE TABLE auction (
            id BIGINT,
            item_name VARCHAR,
            description VARCHAR,
            initial_bid BIGINT,
            reserve BIGINT,
            date_time TIMESTAMP,
            expires TIMESTAMP,
            seller BIGINT,
            category BIGINT,
            extra VARCHAR,
            PRIMARY KEY (id)
        );
        "#;
        let create_table_redacted_expected = "CREATE TABLE _1_ (_2_ BIGINT, _3_ CHARACTER VARYING, _4_ CHARACTER VARYING, _5_ BIGINT, _6_ BIGINT, _7_ TIMESTAMP, _8_ TIMESTAMP, _9_ BIGINT, _10_ BIGINT, _11_ CHARACTER VARYING, PRIMARY KEY (_2_))";

        let create_mv = r#"
        CREATE MATERIALIZED VIEW mv_nexmark_q102
        AS
        SELECT
            a.id AS auction_id,
            a.item_name AS auction_item_name,
            COUNT(b.auction) AS bid_count
        FROM auction a
        JOIN bid b ON a.id = b.auction
        GROUP BY a.id, a.item_name
        HAVING COUNT(b.auction) >= (
            SELECT COUNT(*) / COUNT(DISTINCT auction) FROM bid
        );
        "#;
        let create_mv_expected = "CREATE MATERIALIZED VIEW _12_ AS SELECT _13_._2_ AS _14_, _13_._3_ AS _15_, COUNT(_16_._1_) AS _17_ FROM _1_ AS _13_ JOIN _18_ AS _16_ ON _13_._2_ = _16_._1_ GROUP BY _13_._2_, _13_._3_ HAVING COUNT(_16_._1_) >= (SELECT COUNT(*) / COUNT(DISTINCT _1_) FROM _18_)";
        let create_sink = r#"
        CREATE SINK sink_nexmark_q102 AS
        SELECT
            a.id AS auction_id,
            a.item_name AS auction_item_name,
            COUNT(b.auction) AS bid_count
        FROM auction a
        JOIN bid b ON a.id = b.auction
        GROUP BY a.id, a.item_name
        HAVING COUNT(b.auction) >= (
            SELECT COUNT(*) / COUNT(DISTINCT auction) FROM bid
        )
        WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
        "#;
        let create_sink_expected = "CREATE SINK _19_ AS SELECT _13_._2_ AS _14_, _13_._3_ AS _15_, COUNT(_16_._1_) AS _17_ FROM _1_ AS _13_ JOIN _18_ AS _16_ ON _13_._2_ = _16_._1_ GROUP BY _13_._2_, _13_._3_ HAVING COUNT(_16_._1_) >= (SELECT COUNT(*) / COUNT(DISTINCT _1_) FROM _18_) WITH (connector = '[REDACTED]', type = '[REDACTED]', force_append_only = '[REDACTED]')";
        let mut context = MaskingContext::default();
        let create_table_redacted = redact_statement(&statement(create_table), &mut context);
        let create_mv_redacted = redact_statement(&statement(create_mv), &mut context);
        let create_sink_redacted = redact_statement(&statement(create_sink), &mut context);
        assert_eq!(
            create_table_redacted_expected,
            create_table_redacted.to_string()
        );
        assert_eq!(create_mv_expected, create_mv_redacted.to_string());
        assert_eq!(create_sink_expected, create_sink_redacted.to_string());
    }
}
