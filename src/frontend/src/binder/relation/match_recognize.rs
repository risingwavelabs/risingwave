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
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::PbAggKind;
use risingwave_sqlparser::ast::{
    AfterMatchSkip, Expr as AstExpr, FunctionArg, FunctionArgExpr, MatchRecognizePattern,
    MatchRecognizeSymbol, Measure, OrderByExpr, RowsPerMatch, SymbolDefinition, TableAlias,
    TableFactor,
};

use super::{Binder, Relation};
use crate::error::Result as RwResult;
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef, Literal,
    OrderBy,
};
use crate::optimizer::plan_node::generic::PlanAggCall;
use crate::utils::Condition;

/// How a [`MeasureSlot`] resolves against the rows of a match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MeasureSlotKind {
    /// The column value of the first row labeled `var` within the match (`FIRST(var.col)`).
    First,
    /// The column value of the last row labeled `var` (`LAST(var.col)`, and the meaning of a bare
    /// `var.col` under ONE ROW PER MATCH FINAL semantics).
    Last,
    /// The pattern variable bound to the match's last row (`CLASSIFIER()`).
    Classifier,
    /// Number of rows in the whole match (`COUNT(*)`). `var` / `col_idx` unused.
    CountStar,
    /// Number of rows labeled `var` whose `col` is non-null (`COUNT(var.col)`).
    Count,
    /// Minimum `col` over rows labeled `var` (`MIN(var.col)`).
    Min,
    /// Maximum `col` over rows labeled `var` (`MAX(var.col)`).
    Max,
    /// Sum of `col` over rows labeled `var` (`SUM(var.col)`); evaluated by the slot's `agg`.
    /// `AVG` reuses this (plus a `Count` slot) rather than having its own kind.
    Sum,
}

/// One navigation input that a measure expression reads. A measure is lowered to an expression over
/// a synthetic row whose `i`-th column is produced by `slots[i]`; the executor materializes that row
/// per match (the column values are only knowable once the match and its per-row labels are found)
/// and then evaluates the expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MeasureSlot {
    pub kind: MeasureSlotKind,
    /// Pattern variable to navigate to. Empty for [`MeasureSlotKind::Classifier`].
    pub var: String,
    /// Input column index to read. Ignored for [`MeasureSlotKind::Classifier`].
    pub col_idx: usize,
    /// The slot's output type: the input column type for navigation, varchar for classifier.
    pub data_type: DataType,
    /// The aggregate to run for [`MeasureSlotKind::Sum`] / [`MeasureSlotKind::Avg`], over a single
    /// input column (the projected `col_idx`) of the rows labeled `var`. `None` for other kinds.
    pub agg: Option<PlanAggCall>,
}

/// A bound `MEASURES` item: an expression over the per-match synthetic row, its navigation slots,
/// and the output name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundMeasure {
    /// Expression over the synthetic row: `InputRef(i)` reads `slots[i]`.
    pub expr: ExprImpl,
    pub name: String,
    pub slots: Vec<MeasureSlot>,
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
            .map(|m| self.lower_measure(m, &variables, input_col_num))
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

    /// Lowers one `MEASURES` item to an expression over a synthetic per-match row plus the slots
    /// that produce that row. Pattern-variable column references become navigation slots: bare
    /// `var.col` and arithmetic over such references resolve to `LAST(var.col)` (FINAL semantics
    /// under ONE ROW PER MATCH); top-level `FIRST(var.col)` / `LAST(var.col)` and `CLASSIFIER()` are
    /// supported. Nesting `FIRST`/`LAST`/`CLASSIFIER` inside a larger expression is not yet
    /// supported (it falls through to ordinary binding and is rejected as an unknown function).
    fn lower_measure(
        &mut self,
        m: &Measure,
        variables: &[String],
        input_col_num: usize,
    ) -> RwResult<BoundMeasure> {
        let name = m.alias.real_value();

        // CLASSIFIER(): the pattern variable bound to the match's last row.
        if let AstExpr::Function(func) = &m.expr
            && func.name.0.len() == 1
            && func.name.0[0].real_value().eq_ignore_ascii_case("classifier")
        {
            if !func.arg_list.args.is_empty() {
                bail_not_implemented!("CLASSIFIER() with arguments in MATCH_RECOGNIZE");
            }
            return Ok(BoundMeasure {
                expr: InputRef::new(0, DataType::Varchar).into(),
                name,
                slots: vec![MeasureSlot {
                    kind: MeasureSlotKind::Classifier,
                    var: String::new(),
                    col_idx: 0,
                    data_type: DataType::Varchar,
                    agg: None,
                }],
            });
        }

        // Top-level aggregates over the matched rows: COUNT(*), COUNT/MIN/MAX/SUM/AVG(var.col).
        if let AstExpr::Function(func) = &m.expr
            && func.name.0.len() == 1
            && matches!(
                func.name.0[0].real_value().to_ascii_lowercase().as_str(),
                "count" | "min" | "max" | "sum" | "avg"
            )
        {
            let agg = func.name.0[0].real_value().to_ascii_lowercase();
            if func.arg_list.args.len() != 1 {
                bail_not_implemented!("{}() expects exactly one argument", agg.to_uppercase());
            }
            // COUNT(*): every row of the match.
            if agg == "count"
                && matches!(
                    &func.arg_list.args[0],
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard(_))
                )
            {
                return Ok(BoundMeasure {
                    expr: InputRef::new(0, DataType::Int64).into(),
                    name,
                    slots: vec![MeasureSlot {
                        kind: MeasureSlotKind::CountStar,
                        var: String::new(),
                        col_idx: 0,
                        data_type: DataType::Int64,
                        agg: None,
                    }],
                });
            }
            // agg(var.col): over the rows labeled `var`.
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &func.arg_list.args[0] else {
                bail_not_implemented!("{}() argument must be a pattern-variable column", agg.to_uppercase());
            };
            let ExprImpl::InputRef(r) = self.bind_expr(inner)? else {
                bail_not_implemented!("{}() argument must be a pattern-variable column", agg.to_uppercase());
            };
            let (var, col_idx) = decode_var_col(r.index(), input_col_num, variables)?;
            let col_type = r.data_type.clone();

            // COUNT / MIN / MAX fold directly over the matched rows in the executor.
            if let Some((kind, data_type)) = match agg.as_str() {
                "count" => Some((MeasureSlotKind::Count, DataType::Int64)),
                "min" => Some((MeasureSlotKind::Min, col_type.clone())),
                "max" => Some((MeasureSlotKind::Max, col_type.clone())),
                _ => None,
            } {
                return Ok(BoundMeasure {
                    expr: InputRef::new(0, data_type.clone()).into(),
                    name,
                    slots: vec![MeasureSlot {
                        kind,
                        var,
                        col_idx,
                        data_type,
                        agg: None,
                    }],
                });
            }

            // SUM reuses RisingWave's aggregate kernel so the numeric return type stays faithful. The
            // runtime feeds the kernel a single-column chunk (the projected col), so the call's
            // argument is an InputRef to column 0. AVG is built on top as cast(sum / count).
            let infer = |kind: PbAggKind| -> RwResult<DataType> {
                Ok(AggCall::new(
                    kind.into(),
                    vec![InputRef::new(0, col_type.clone()).into()],
                    false,
                    OrderBy::any(),
                    Condition::true_cond(),
                    vec![],
                )?
                .return_type)
            };
            let sum_type = infer(PbAggKind::Sum)?;
            let sum_slot = MeasureSlot {
                kind: MeasureSlotKind::Sum,
                var: var.clone(),
                col_idx,
                data_type: sum_type.clone(),
                agg: Some(PlanAggCall {
                    agg_type: PbAggKind::Sum.into(),
                    return_type: sum_type.clone(),
                    inputs: vec![InputRef::new(0, col_type.clone())],
                    distinct: false,
                    order_by: vec![],
                    filter: Condition::true_cond(),
                    direct_args: vec![],
                }),
            };

            if agg == "sum" {
                return Ok(BoundMeasure {
                    expr: InputRef::new(0, sum_type).into(),
                    name,
                    slots: vec![sum_slot],
                });
            }

            // AVG = CASE WHEN count = 0 THEN NULL ELSE cast(sum AS avg_type) / count END, mirroring
            // how RisingWave's planner rewrites avg. Slot 0 is the sum, slot 1 the (non-null) count.
            let avg_type = infer(PbAggKind::Avg)?;
            let count_slot = MeasureSlot {
                kind: MeasureSlotKind::Count,
                var,
                col_idx,
                data_type: DataType::Int64,
                agg: None,
            };
            let sum_ref: ExprImpl = InputRef::new(0, sum_type).into();
            let count_ref: ExprImpl = InputRef::new(1, DataType::Int64).into();
            let quotient: ExprImpl = FunctionCall::new(
                ExprType::Divide,
                vec![sum_ref.cast_explicit(&avg_type)?, count_ref.clone()],
            )?
            .into();
            let count_is_zero: ExprImpl =
                FunctionCall::new(ExprType::Equal, vec![count_ref, ExprImpl::literal_int(0)])?.into();
            let null: ExprImpl = Literal::new(None, avg_type).into();
            let expr: ExprImpl =
                FunctionCall::new(ExprType::Case, vec![count_is_zero, null, quotient])?.into();
            return Ok(BoundMeasure {
                expr,
                name,
                slots: vec![sum_slot, count_slot],
            });
        }

        // Top-level FIRST(var.col) / LAST(var.col).
        if let AstExpr::Function(func) = &m.expr
            && func.name.0.len() == 1
            && matches!(
                func.name.0[0].real_value().to_ascii_lowercase().as_str(),
                "first" | "last"
            )
        {
            let kind = if func.name.0[0].real_value().eq_ignore_ascii_case("first") {
                MeasureSlotKind::First
            } else {
                MeasureSlotKind::Last
            };
            if func.arg_list.args.len() != 1 {
                bail_not_implemented!("FIRST/LAST with an offset argument in MATCH_RECOGNIZE");
            }
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &func.arg_list.args[0] else {
                bail_not_implemented!("FIRST/LAST argument must be a pattern-variable column");
            };
            let ExprImpl::InputRef(r) = self.bind_expr(inner)? else {
                bail_not_implemented!("FIRST/LAST argument must be a pattern-variable column");
            };
            let (var, col_idx) = decode_var_col(r.index(), input_col_num, variables)?;
            let data_type = r.data_type.clone();
            return Ok(BoundMeasure {
                expr: InputRef::new(0, data_type.clone()).into(),
                name,
                slots: vec![MeasureSlot {
                    kind,
                    var,
                    col_idx,
                    data_type,
                    agg: None,
                }],
            });
        }

        // General case: bare `var.col` and arithmetic over such references. Binding succeeds via the
        // per-variable alias blocks; each resulting InputRef is then rewritten to a synthetic
        // LAST(var.col) slot.
        let expr = self.bind_expr(&m.expr)?;
        let mut check = InputRefBlockCheck {
            input_col_num,
            unqualified: false,
        };
        check.visit_expr(&expr);
        if check.unqualified {
            bail_not_implemented!(
                "unqualified or non-pattern-variable column reference in MATCH_RECOGNIZE MEASURES"
            );
        }
        let mut rewriter = SlotLoweringRewriter {
            input_col_num,
            variables,
            slots: Vec::new(),
        };
        let expr = rewriter.rewrite_expr(expr);
        Ok(BoundMeasure {
            expr,
            name,
            slots: rewriter.slots,
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

/// Decodes the pattern variable and input column behind a measure `InputRef`. Each variable was
/// registered as an alias block of width `input_col_num` after the input columns, so block 0 is the
/// raw input (an unqualified reference, unsupported here) and block `k + 1` is `variables[k]`.
fn decode_var_col(
    index: usize,
    input_col_num: usize,
    variables: &[String],
) -> RwResult<(String, usize)> {
    let block = index / input_col_num;
    if block == 0 {
        bail_not_implemented!(
            "unqualified or non-pattern-variable column reference in MATCH_RECOGNIZE MEASURES"
        );
    }
    let var = variables
        .get(block - 1)
        .expect("pattern variable block within range of registered variables")
        .clone();
    Ok((var, index % input_col_num))
}

/// Checks that every measure `InputRef` is pattern-variable-qualified (alias block >= 1). A
/// reference into block 0 is the raw input — an unqualified or table-qualified column with no
/// pattern-variable navigation meaning.
struct InputRefBlockCheck {
    input_col_num: usize,
    unqualified: bool,
}

impl ExprVisitor for InputRefBlockCheck {
    fn visit_input_ref(&mut self, input_ref: &InputRef) {
        if input_ref.index() < self.input_col_num {
            self.unqualified = true;
        }
    }
}

/// Rewrites each pattern-variable-qualified `InputRef` in a measure expression to an `InputRef` into
/// the synthetic per-match row, recording a deduplicated `LAST(var.col)` slot for it.
struct SlotLoweringRewriter<'a> {
    input_col_num: usize,
    variables: &'a [String],
    slots: Vec<MeasureSlot>,
}

impl ExprRewriter for SlotLoweringRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let block = input_ref.index() / self.input_col_num;
        let col_idx = input_ref.index() % self.input_col_num;
        let var = self.variables[block - 1].clone();
        let data_type = input_ref.data_type;
        let slot_idx = self
            .slots
            .iter()
            .position(|s| s.kind == MeasureSlotKind::Last && s.var == var && s.col_idx == col_idx)
            .unwrap_or_else(|| {
                self.slots.push(MeasureSlot {
                    kind: MeasureSlotKind::Last,
                    var,
                    col_idx,
                    data_type: data_type.clone(),
                    agg: None,
                });
                self.slots.len() - 1
            });
        InputRef::new(slot_idx, data_type).into()
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
