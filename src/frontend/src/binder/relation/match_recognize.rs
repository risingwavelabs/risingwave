// Copyright 2025 RisingWave Labs
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

use std::collections::BTreeSet;

use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::Field;
use risingwave_sqlparser::ast::{
    AfterMatchSkip, Expr as AstExpr, MatchRecognizePattern, MatchRecognizeSymbol, Measure,
    OrderByExpr, RowsPerMatch, SymbolDefinition, TableAlias, TableFactor,
};

use super::{Binder, Relation};
use crate::error::Result as RwResult;
use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};

/// A bound `MEASURES` item: an expression over the matched rows and its output name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundMeasure {
    pub expr: ExprImpl,
    pub name: String,
}

/// A bound `DEFINE` item: a pattern variable and the predicate that defines membership.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundSymbolDefinition {
    pub symbol: String,
    pub definition: ExprImpl,
}

#[derive(Debug, Clone)]
pub struct BoundMatchRecognize {
    pub input: Relation,
    pub partition_by: Vec<ExprImpl>,
    pub order_by: Vec<ExprImpl>,
    pub measures: Vec<BoundMeasure>,
    pub rows_per_match: Option<RowsPerMatch>,
    pub after_match_skip: Option<AfterMatchSkip>,
    pub pattern: MatchRecognizePattern,
    pub defines: Vec<BoundSymbolDefinition>,
}

impl Binder {
    pub(super) fn bind_match_recognize(
        &mut self,
        table: &TableFactor,
        partition_by: &[AstExpr],
        order_by: &[OrderByExpr],
        measures: &[Measure],
        rows_per_match: &Option<RowsPerMatch>,
        after_match_skip: &Option<AfterMatchSkip>,
        pattern: &MatchRecognizePattern,
        symbols: &[SymbolDefinition],
        alias: Option<&TableAlias>,
    ) -> RwResult<BoundMatchRecognize> {
        // ALL ROWS PER MATCH is not in the v1 subset.
        if matches!(rows_per_match, Some(RowsPerMatch::AllRows)) {
            bail_not_implemented!("ALL ROWS PER MATCH");
        }
        // AFTER MATCH SKIP TO FIRST/LAST <symbol> is not in the v1 subset.
        if matches!(
            after_match_skip,
            Some(AfterMatchSkip::ToFirst(_) | AfterMatchSkip::ToLast(_))
        ) {
            bail_not_implemented!("AFTER MATCH SKIP TO FIRST/LAST <symbol>");
        }

        self.push_context();

        // Bind the input. This registers the input's columns in the current context.
        let input = self.bind_table_factor(table)?;

        // PARTITION BY / ORDER BY are evaluated over the input rows, so bind them while only the
        // input is in scope — unqualified column references resolve unambiguously here.
        let partition_by = partition_by
            .iter()
            .map(|e| self.bind_expr(e))
            .collect::<RwResult<Vec<_>>>()?;
        let order_by = order_by
            .iter()
            .map(|o| self.bind_expr(&o.expr))
            .collect::<RwResult<Vec<_>>>()?;

        // Snapshot the input columns so each pattern variable can be registered as an alias over
        // them. After this, `A.col` (a pattern-variable-qualified reference) resolves to the input
        // column `col`. The variable association is preserved in the AST for execution; this step
        // only makes MEASURES/DEFINE type-check.
        let input_columns: Vec<(bool, Field)> = self
            .context
            .columns
            .iter()
            .map(|c| (c.is_hidden, c.field.clone()))
            .collect();

        let input_col_num = input_columns.len();
        let variables = collect_pattern_variables(pattern, symbols);
        for var in &variables {
            self.bind_table_to_context(input_columns.clone(), var.clone(), None, None)?;
        }

        // Each pattern variable was registered as an identical alias block over the input columns,
        // so a variable-qualified reference `A.col` binds to an InputRef into the extended context.
        // Collapse those back onto the real input columns (`i % input_col_num`): in the v1 subset a
        // DEFINE/MEASURES reference resolves to the current row's column. The variable label is kept
        // separately in `symbol`; cross-variable navigation is rejected above.
        let mut remap = PatternVarColRemap { input_col_num };

        // DEFINE predicates: <symbol> AS <condition>.
        let defines = symbols
            .iter()
            .map(|s| {
                reject_navigation(&s.definition)?;
                let definition = self.bind_expr(&s.definition)?;
                Ok(BoundSymbolDefinition {
                    symbol: s.symbol.real_value(),
                    definition: remap.rewrite_expr(definition),
                })
            })
            .collect::<RwResult<Vec<_>>>()?;

        // MEASURES: <expr> AS <alias>.
        let measures = measures
            .iter()
            .map(|m| {
                reject_navigation(&m.expr)?;
                let expr = self.bind_expr(&m.expr)?;
                Ok(BoundMeasure {
                    expr: remap.rewrite_expr(expr),
                    name: m.alias.real_value(),
                })
            })
            .collect::<RwResult<Vec<_>>>()?;

        self.pop_context()?;

        // Output schema (ONE ROW PER MATCH): the partition-by columns followed by the measures.
        let mut output_columns: Vec<(bool, Field)> = Vec::new();
        for (i, e) in partition_by.iter().enumerate() {
            output_columns.push((false, Field::with_name(e.return_type(), format!("partition_{i}"))));
        }
        for m in &measures {
            output_columns.push((false, Field::with_name(m.expr.return_type(), m.name.clone())));
        }

        let table_name = match alias {
            Some(TableAlias { name, .. }) => name.real_value(),
            None => "match_recognize".to_owned(),
        };
        self.bind_table_to_context(output_columns, table_name, None, alias)?;

        Ok(BoundMatchRecognize {
            input,
            partition_by,
            order_by,
            measures,
            rows_per_match: rows_per_match.clone(),
            after_match_skip: after_match_skip.clone(),
            pattern: pattern.clone(),
            defines,
        })
    }
}

/// Collapses pattern-variable-qualified column references onto the real input columns. Each
/// variable was registered as an identical alias block of width `input_col_num`, so `InputRef(i)`
/// maps to input column `i % input_col_num`.
struct PatternVarColRemap {
    input_col_num: usize,
}

impl ExprRewriter for PatternVarColRemap {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let index = input_ref.index() % self.input_col_num;
        InputRef::new(index, input_ref.data_type).into()
    }
}

/// Collect the distinct pattern-variable names appearing in a pattern, together with any defined in
/// `DEFINE` (a variable may be defined but, e.g., excluded, or referenced only in `MEASURES`).
fn collect_pattern_variables(
    pattern: &MatchRecognizePattern,
    symbols: &[SymbolDefinition],
) -> Vec<String> {
    let mut vars: BTreeSet<String> = BTreeSet::new();
    collect_from_pattern(pattern, &mut vars);
    for s in symbols {
        vars.insert(s.symbol.real_value());
    }
    vars.into_iter().collect()
}

fn collect_from_pattern(pattern: &MatchRecognizePattern, out: &mut BTreeSet<String>) {
    match pattern {
        MatchRecognizePattern::Symbol(MatchRecognizeSymbol::Named(ident))
        | MatchRecognizePattern::Exclude(MatchRecognizeSymbol::Named(ident)) => {
            out.insert(ident.real_value());
        }
        MatchRecognizePattern::Symbol(_) | MatchRecognizePattern::Exclude(_) => {}
        MatchRecognizePattern::Permute(symbols) => {
            for s in symbols {
                if let MatchRecognizeSymbol::Named(ident) = s {
                    out.insert(ident.real_value());
                }
            }
        }
        MatchRecognizePattern::Concat(patterns) | MatchRecognizePattern::Alternation(patterns) => {
            for p in patterns {
                collect_from_pattern(p, out);
            }
        }
        MatchRecognizePattern::Group(inner) => collect_from_pattern(inner, out),
        MatchRecognizePattern::Repetition(inner, _) => collect_from_pattern(inner, out),
    }
}

/// Row-pattern navigation operations (`FIRST`/`LAST`/`PREV`/`NEXT`) are not yet supported in
/// MEASURES/DEFINE. They parse as ordinary function calls, so reject them with a clear message
/// rather than letting binding fail with a confusing "function does not exist" error.
fn reject_navigation(expr: &AstExpr) -> RwResult<()> {
    if let AstExpr::Function(func) = expr
        && let Some(first) = func.name.0.first()
    {
        let name = first.real_value().to_ascii_lowercase();
        if matches!(name.as_str(), "first" | "last" | "prev" | "next") {
            bail_not_implemented!("row pattern navigation function {} in MATCH_RECOGNIZE", name);
        }
    }
    Ok(())
}
