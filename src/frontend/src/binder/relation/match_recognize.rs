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
    AfterMatchSkip, Expr as AstExpr, Function, FunctionArg, FunctionArgExpr, Ident,
    MatchRecognizePattern, MatchRecognizeSymbol, Measure, OrderByExpr, RowsPerMatch,
    SubsetDefinition, SymbolDefinition, TableAlias, TableFactor, Value as AstValue,
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
    /// Pattern variables this slot navigates over: one for a plain variable, several for a `SUBSET`
    /// union variable. A row matches if its label is any of these. Empty for `CLASSIFIER`.
    pub vars: Vec<String>,
    /// Input column index to read. Ignored for [`MeasureSlotKind::Classifier`].
    pub col_idx: usize,
    /// The slot's output type: the input column type for navigation, varchar for classifier.
    pub data_type: DataType,
    /// The aggregate to run for [`MeasureSlotKind::Sum`], over a single input column (the projected
    /// `col_idx`) of the rows whose label is in `vars`. `None` for other kinds.
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

/// How a [`DefineSlot`] resolves against the row being tested for membership in a pattern variable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DefineSlotKind {
    /// The candidate row's own column (`var.col` for the variable being defined, or unqualified).
    SelfCol,
    /// A physical-offset column: `PREV(col, n)` reads `offset` rows earlier in the ordered partition.
    Prev,
    /// `NEXT(col, n)`: `offset` rows later.
    Next,
    /// `FIRST(var.col)`: the first row labeled `var` in the in-progress match.
    RunningFirst,
    /// `LAST(var.col)` / bare `var.col` of another variable: the last such row (running).
    RunningLast,
}

/// One input a `DEFINE` predicate reads. A predicate is lowered to an expression over a synthetic
/// row whose `i`-th column is produced by `slots[i]`; the executor materializes that row for each
/// candidate row from the sorted partition and the in-progress match's labels.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DefineSlot {
    pub kind: DefineSlotKind,
    /// Pattern variables for `RunningFirst`/`RunningLast` (several for a `SUBSET`); empty otherwise.
    pub vars: Vec<String>,
    /// Input column index to read.
    pub col_idx: usize,
    /// Physical offset for `Prev`/`Next` (>= 1); `0` for the other kinds.
    pub offset: usize,
}

/// A bound `DEFINE` item: a pattern variable, the predicate over its [`DefineSlot`]s, and the slots.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundSymbolDefinition {
    pub symbol: String,
    /// Predicate over the synthetic row: `InputRef(i)` reads `slots[i]`.
    pub definition: ExprImpl,
    pub slots: Vec<DefineSlot>,
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
    /// `WITHIN` span check, lowered to a predicate over a synthetic `[last_order_key,
    /// first_order_key]` row: `InputRef(0) - InputRef(1) <= <interval>`. `None` when omitted.
    pub within: Option<ExprImpl>,
}

impl Binder {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn bind_match_recognize(
        &mut self,
        table: &TableFactor,
        partition_by: &[AstExpr],
        order_by: &[OrderByExpr],
        measures: &[Measure],
        rows_per_match: &Option<RowsPerMatch>,
        after_match_skip: &Option<AfterMatchSkip>,
        pattern: &MatchRecognizePattern,
        within: &Option<AstExpr>,
        subsets: &[SubsetDefinition],
        symbols: &[SymbolDefinition],
        alias: Option<&TableAlias>,
    ) -> RwResult<BoundMatchRecognize> {
        // ALL ROWS PER MATCH is not in the v1 subset.
        if matches!(rows_per_match, Some(RowsPerMatch::AllRows)) {
            bail_not_implemented!("ALL ROWS PER MATCH");
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

        // WITHIN: lower `WITHIN <interval>` to a span check over a synthetic [last, first] row, i.e.
        // `InputRef(0) - InputRef(1) <= <interval>`, against the leading ORDER BY column's type. The
        // expression machinery handles the typed arithmetic (timestamp − timestamp → interval, etc).
        let within = match within {
            Some(e) => {
                let bound = self.bind_expr(e)?;
                let Some(order_key) = order_by.first() else {
                    bail_not_implemented!("WITHIN requires an ORDER BY column");
                };
                let ts_type = order_key.return_type();
                let last = ExprImpl::from(InputRef::new(0, ts_type.clone()));
                let first = ExprImpl::from(InputRef::new(1, ts_type));
                let diff = ExprImpl::from(FunctionCall::new(ExprType::Subtract, vec![last, first])?);
                Some(ExprImpl::from(FunctionCall::new(
                    ExprType::LessThanOrEqual,
                    vec![diff, bound],
                )?))
            }
            None => None,
        };

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

        // SUBSET union variables: each must be made of declared pattern variables, and is registered
        // as a further alias block (after the base variables) so `U.col` type-checks. `alias_names`
        // records the registration order so a measure InputRef can be decoded back to its variable.
        let mut alias_names = variables.clone();
        let mut subset_defs: Vec<(String, Vec<String>)> = Vec::with_capacity(subsets.len());
        for s in subsets {
            let name = s.name.real_value();
            let members: Vec<String> = s.members.iter().map(|m| m.real_value()).collect();
            for m in &members {
                if !variables.contains(m) {
                    bail_not_implemented!(
                        "SUBSET {} references unknown pattern variable {}",
                        name,
                        m
                    );
                }
            }
            self.bind_table_to_context(input_columns.clone(), name.clone(), None, None)?;
            alias_names.push(name.clone());
            subset_defs.push((name, members));
        }
        let resolver = VarResolver {
            input_col_num,
            alias_names: &alias_names,
            subset_defs: &subset_defs,
        };

        // AFTER MATCH SKIP TO FIRST/LAST <var> must name a pattern variable.
        if let Some(AfterMatchSkip::ToFirst(sym) | AfterMatchSkip::ToLast(sym)) = after_match_skip
            && !variables.contains(&sym.real_value())
        {
            bail_not_implemented!(
                "AFTER MATCH SKIP TO FIRST/LAST references unknown pattern variable {}",
                sym.real_value()
            );
        }

        // Each pattern variable was registered as an identical alias block over the input columns,
        // DEFINE predicates: <symbol> AS <condition>. Each is lowered to an expression over a
        // synthetic row of navigation slots (the candidate row's columns, physical PREV/NEXT, and
        // running references to other variables), evaluated per candidate during matching.
        let input_fields: Vec<Field> = input_columns.iter().map(|(_, f)| f.clone()).collect();
        let defines = symbols
            .iter()
            .map(|s| self.lower_define(s, &resolver, &input_fields))
            .collect::<RwResult<Vec<_>>>()?;

        // MEASURES: <expr> AS <alias>.
        let measures = measures
            .iter()
            .map(|m| self.lower_measure(m, &resolver))
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
            within,
        })
    }

    /// Lowers one `MEASURES` item to an expression over a synthetic per-match row plus the slots
    /// that produce that row. Pattern-variable column references become navigation slots: bare
    /// `var.col` and arithmetic over such references resolve to `LAST(var.col)` (FINAL semantics
    /// under ONE ROW PER MATCH); top-level `FIRST(var.col)` / `LAST(var.col)` and `CLASSIFIER()` are
    /// supported. Nesting `FIRST`/`LAST`/`CLASSIFIER` inside a larger expression is not yet
    /// supported (it falls through to ordinary binding and is rejected as an unknown function).
    fn lower_measure(&mut self, m: &Measure, resolver: &VarResolver<'_>) -> RwResult<BoundMeasure> {
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
                    vars: vec![],
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
                        vars: vec![],
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
            let (vars, col_idx) = resolver.resolve(r.index())?;
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
                        vars,
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
                vars: vars.clone(),
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
                vars,
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
            let (vars, col_idx) = resolver.resolve(r.index())?;
            let data_type = r.data_type.clone();
            return Ok(BoundMeasure {
                expr: InputRef::new(0, data_type.clone()).into(),
                name,
                slots: vec![MeasureSlot {
                    kind,
                    vars,
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
            input_col_num: resolver.input_col_num,
            unqualified: false,
        };
        check.visit_expr(&expr);
        if check.unqualified {
            bail_not_implemented!(
                "unqualified or non-pattern-variable column reference in MATCH_RECOGNIZE MEASURES"
            );
        }
        let mut rewriter = SlotLoweringRewriter {
            resolver,
            slots: Vec::new(),
        };
        let expr = rewriter.rewrite_expr(expr);
        Ok(BoundMeasure {
            expr,
            name,
            slots: rewriter.slots,
        })
    }

    /// Lowers one `DEFINE` predicate to an expression over a synthetic row of [`DefineSlot`]s.
    /// `PREV`/`NEXT`/`FIRST`/`LAST(...)` navigation functions are extracted into slots first (they do
    /// not bind as ordinary functions); the remaining variable-qualified columns bind via the alias
    /// blocks and are mapped to self slots (the defined variable / unqualified) or running slots.
    fn lower_define(
        &mut self,
        s: &SymbolDefinition,
        resolver: &VarResolver<'_>,
        input_fields: &[Field],
    ) -> RwResult<BoundSymbolDefinition> {
        let symbol = s.symbol.real_value();
        // A per-symbol prefix keeps the placeholder relation and columns unique across DEFINEs, which
        // all bind in the same context.
        let prefix = format!("{NAV_TABLE}_{symbol}");

        let mut cond = s.definition.clone();
        let mut extractor = NavExtractor {
            input_fields,
            resolver,
            prefix: &prefix,
            nav_slots: Vec::new(),
            nav_fields: Vec::new(),
        };
        extractor.rewrite(&mut cond)?;
        let NavExtractor {
            nav_slots,
            nav_fields,
            ..
        } = extractor;

        // Bring the navigation placeholders into scope as a synthetic relation so the predicate
        // type-checks. They are appended to the current context, so capture the base index first.
        let nav_base = self.context.columns.len();
        if !nav_fields.is_empty() {
            let cols: Vec<(bool, Field)> =
                nav_fields.iter().map(|f| (false, f.clone())).collect();
            self.bind_table_to_context(cols, prefix.clone(), None, None)?;
        }

        let expr = self.bind_expr(&cond)?;

        let (definition, slots) = {
            let mut rewriter = DefineSlotRewriter {
                resolver,
                defined_var: &symbol,
                nav_base,
                nav_slots: &nav_slots,
                slots: Vec::new(),
            };
            let definition = rewriter.rewrite_expr(expr);
            (definition, rewriter.slots)
        };
        Ok(BoundSymbolDefinition {
            symbol,
            definition,
            slots,
        })
    }
}

/// Name of the synthetic relation that backs a `DEFINE`'s navigation placeholders.
const NAV_TABLE: &str = "__mr_nav";

/// Extracts row-pattern navigation functions (`PREV`/`NEXT`/`FIRST`/`LAST`) from a `DEFINE`
/// predicate AST, replacing each with a synthetic placeholder column and recording the corresponding
/// [`DefineSlot`]. Functions are handled because they do not bind as ordinary scalar functions;
/// plain variable-qualified columns are left to bind normally and are mapped later.
struct NavExtractor<'a> {
    input_fields: &'a [Field],
    resolver: &'a VarResolver<'a>,
    /// Per-DEFINE prefix for the synthetic placeholder column names (kept unique across DEFINEs).
    prefix: &'a str,
    nav_slots: Vec<DefineSlot>,
    nav_fields: Vec<Field>,
}

impl NavExtractor<'_> {
    fn rewrite(&mut self, node: &mut AstExpr) -> RwResult<()> {
        if let AstExpr::Function(func) = node
            && func.name.0.len() == 1
            && matches!(
                func.name.0[0].real_value().to_ascii_lowercase().as_str(),
                "prev" | "next" | "first" | "last"
            )
        {
            let k = self.nav_slots.len();
            let slot = self.nav_slot(func)?;
            let data_type = self.input_fields[slot.col_idx].data_type();
            let col_name = format!("{}_{k}", self.prefix);
            self.nav_fields
                .push(Field::with_name(data_type, col_name.clone()));
            *node = AstExpr::Identifier(Ident::new_unchecked(col_name));
            self.nav_slots.push(slot);
            return Ok(());
        }
        match node {
            AstExpr::BinaryOp { left, right, .. } => {
                self.rewrite(left)?;
                self.rewrite(right)?;
            }
            AstExpr::UnaryOp { expr, .. }
            | AstExpr::Nested(expr)
            | AstExpr::IsNull(expr)
            | AstExpr::IsNotNull(expr)
            | AstExpr::IsTrue(expr)
            | AstExpr::IsNotTrue(expr)
            | AstExpr::IsFalse(expr)
            | AstExpr::IsNotFalse(expr)
            | AstExpr::Cast { expr, .. }
            | AstExpr::TryCast { expr, .. } => self.rewrite(expr)?,
            AstExpr::Between {
                expr, low, high, ..
            } => {
                self.rewrite(expr)?;
                self.rewrite(low)?;
                self.rewrite(high)?;
            }
            AstExpr::Function(func) => {
                for arg in &mut func.arg_list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        self.rewrite(e)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Builds the [`DefineSlot`] for a navigation function call.
    fn nav_slot(&self, func: &Function) -> RwResult<DefineSlot> {
        let name = func.name.0[0].real_value().to_ascii_lowercase();
        let args = &func.arg_list.args;
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))) = args.first() else {
            bail_not_implemented!("{}() argument must be a column in DEFINE", name.to_uppercase());
        };
        match name.as_str() {
            "prev" | "next" => {
                let col_idx = self.physical_col(inner)?;
                let offset = match args.get(1) {
                    Some(arg) => self.parse_offset(arg, &name)?,
                    None => 1,
                };
                let kind = if name == "prev" {
                    DefineSlotKind::Prev
                } else {
                    DefineSlotKind::Next
                };
                Ok(DefineSlot {
                    kind,
                    vars: vec![],
                    col_idx,
                    offset,
                })
            }
            _ => {
                if args.len() != 1 {
                    bail_not_implemented!("{}() with an offset in DEFINE", name.to_uppercase());
                }
                let (vars, col_idx) = self.var_col(inner)?;
                let kind = if name == "first" {
                    DefineSlotKind::RunningFirst
                } else {
                    DefineSlotKind::RunningLast
                };
                Ok(DefineSlot {
                    kind,
                    vars,
                    col_idx,
                    offset: 0,
                })
            }
        }
    }

    /// Resolves a physical-navigation argument (`col` or `var.col`) to its input column index; the
    /// variable qualifier, if any, does not matter for physical navigation.
    fn physical_col(&self, expr: &AstExpr) -> RwResult<usize> {
        let col = match expr {
            AstExpr::Identifier(c) => c.real_value(),
            AstExpr::CompoundIdentifier(parts) if parts.len() == 2 => parts[1].real_value(),
            _ => bail_not_implemented!("PREV/NEXT argument must be a column reference in DEFINE"),
        };
        self.col_idx(&col)
    }

    /// Resolves a logical-navigation argument (`var.col`) to its variable(s) and input column index.
    fn var_col(&self, expr: &AstExpr) -> RwResult<(Vec<String>, usize)> {
        let AstExpr::CompoundIdentifier(parts) = expr else {
            bail_not_implemented!("FIRST/LAST argument must be a pattern-variable column in DEFINE");
        };
        if parts.len() != 2 {
            bail_not_implemented!("FIRST/LAST argument must be a pattern-variable column in DEFINE");
        }
        let var = parts[0].real_value();
        if !self.resolver.alias_names.iter().any(|n| n == &var) {
            bail_not_implemented!("FIRST/LAST references unknown pattern variable {} in DEFINE", var);
        }
        Ok((self.resolver.members_of(&var), self.col_idx(&parts[1].real_value())?))
    }

    fn col_idx(&self, name: &str) -> RwResult<usize> {
        match self.input_fields.iter().position(|f| f.name == name) {
            Some(i) => Ok(i),
            None => bail_not_implemented!("navigation over unknown column {} in DEFINE", name),
        }
    }

    fn parse_offset(&self, arg: &FunctionArg, name: &str) -> RwResult<usize> {
        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(AstExpr::Value(AstValue::Number(s)))) = arg
            && let Ok(n) = s.parse::<usize>()
        {
            Ok(n)
        } else {
            bail_not_implemented!("{}() offset must be a non-negative integer literal", name.to_uppercase());
        }
    }
}

/// Maps each `InputRef` in a bound `DEFINE` predicate to a [`DefineSlot`]: navigation placeholders
/// (index `>= nav_base`) to their pre-resolved slot; variable-qualified columns to a self slot (the
/// defined variable, or an unqualified/raw-input reference) or a running slot (another variable).
struct DefineSlotRewriter<'a> {
    resolver: &'a VarResolver<'a>,
    defined_var: &'a str,
    nav_base: usize,
    nav_slots: &'a [DefineSlot],
    slots: Vec<DefineSlot>,
}

impl ExprRewriter for DefineSlotRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let index = input_ref.index();
        let data_type = input_ref.data_type;
        let slot = if index >= self.nav_base {
            self.nav_slots[index - self.nav_base].clone()
        } else {
            let n = self.resolver.input_col_num;
            let col_idx = index % n;
            let block = index / n;
            let self_slot = DefineSlot {
                kind: DefineSlotKind::SelfCol,
                vars: vec![],
                col_idx,
                offset: 0,
            };
            if block == 0 {
                self_slot
            } else {
                let name = &self.resolver.alias_names[block - 1];
                if name == self.defined_var {
                    self_slot
                } else {
                    DefineSlot {
                        kind: DefineSlotKind::RunningLast,
                        vars: self.resolver.members_of(name),
                        col_idx,
                        offset: 0,
                    }
                }
            }
        };
        let idx = self
            .slots
            .iter()
            .position(|s| *s == slot)
            .unwrap_or_else(|| {
                self.slots.push(slot);
                self.slots.len() - 1
            });
        InputRef::new(idx, data_type).into()
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

/// Decodes a measure `InputRef` back to the pattern variable(s) and input column it references.
/// Pattern variables and `SUBSET` names are each registered as an alias block of width
/// `input_col_num` after the input columns, in `alias_names` order; so block 0 is the raw input (an
/// unqualified reference, unsupported) and block `k + 1` is `alias_names[k]`. A `SUBSET` name
/// resolves to its member variables; a plain variable resolves to itself.
struct VarResolver<'a> {
    input_col_num: usize,
    alias_names: &'a [String],
    subset_defs: &'a [(String, Vec<String>)],
}

impl VarResolver<'_> {
    fn resolve(&self, index: usize) -> RwResult<(Vec<String>, usize)> {
        if index / self.input_col_num == 0 {
            bail_not_implemented!(
                "unqualified or non-pattern-variable column reference in MATCH_RECOGNIZE MEASURES"
            );
        }
        Ok(self.resolve_unchecked(index))
    }

    /// As [`VarResolver::resolve`] but assumes a pattern-variable-qualified reference (block >= 1),
    /// which [`InputRefBlockCheck`] guarantees before lowering.
    fn resolve_unchecked(&self, index: usize) -> (Vec<String>, usize) {
        let block = index / self.input_col_num;
        let name = self
            .alias_names
            .get(block - 1)
            .expect("alias block within range of registered variables/subsets");
        let vars = self.members_of(name);
        (vars, index % self.input_col_num)
    }

    /// The variables a name resolves to: a `SUBSET`'s members, or the variable itself.
    fn members_of(&self, name: &str) -> Vec<String> {
        self.subset_defs
            .iter()
            .find(|(n, _)| n == name)
            .map_or_else(|| vec![name.to_owned()], |(_, members)| members.clone())
    }
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
struct SlotLoweringRewriter<'a, 'b> {
    resolver: &'a VarResolver<'b>,
    slots: Vec<MeasureSlot>,
}

impl ExprRewriter for SlotLoweringRewriter<'_, '_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let (vars, col_idx) = self.resolver.resolve_unchecked(input_ref.index());
        let data_type = input_ref.data_type;
        let slot_idx = self
            .slots
            .iter()
            .position(|s| s.kind == MeasureSlotKind::Last && s.vars == vars && s.col_idx == col_idx)
            .unwrap_or_else(|| {
                self.slots.push(MeasureSlot {
                    kind: MeasureSlotKind::Last,
                    vars,
                    col_idx,
                    data_type: data_type.clone(),
                    agg: None,
                });
                self.slots.len() - 1
            });
        InputRef::new(slot_idx, data_type).into()
    }
}

