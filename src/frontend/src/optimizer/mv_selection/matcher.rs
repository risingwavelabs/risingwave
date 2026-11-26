// Copyright 2026 RisingWave Labs
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

//! Decides whether the result of a query block `Q` can be computed from the output of a view
//! block `V`, and if so, what has to be added on top of a scan of the view.
//!
//! Four checks run in order, and any failure means no match:
//!
//! 1. Input mapping: every input of `V` corresponds to an equivalent input of `Q`.
//! 2. Predicate containment: the predicate of `Q` implies the predicate of `V`. What `Q`
//!    requires on top is the compensating predicate.
//! 3. Derivability: everything the compensation needs can be computed from the output columns
//!    of `V` and the inputs of `Q` that `V` does not cover.
//! 4. Aggregation: the aggregation of `Q` can be taken from, or rolled up from, that of `V`.
//!
//! The matcher never answers "maybe": whatever it cannot prove is not a match.

use std::collections::{HashMap, HashSet};
use std::fmt;

use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::aggregate::{AggType, PbAggKind};

use super::summary::{
    AggCallSummary, AggSummary, BlockInput, BlockSummary, ColKey, ColumnRef, EqClass, Predicate,
    Term, canonical_set, is_exact_type,
};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, Literal};

/// At most this many input mappings are tried for one view. A table that appears 4 times gives
/// 24 mappings.
const MAX_MAPPINGS: usize = 64;

/// How deep a term may be replaced by an equal one while deriving it.
const MAX_SUBSTITUTION_DEPTH: usize = 4;

/// Why a view does not match a query block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchFailure {
    InputMapping,
    Predicate,
    NotDerivable,
    Aggregation,
}

impl fmt::Display for MatchFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MatchFailure::InputMapping => "its inputs do not correspond to inputs of the query",
            MatchFailure::Predicate => "the query predicate does not imply the view predicate",
            MatchFailure::NotDerivable => "the view does not output a column the query needs",
            MatchFailure::Aggregation => "the aggregation cannot be computed from the view",
        })
    }
}

/// What to add on top of a scan of the view. Expressions are over the visible columns of the
/// view followed by the columns of the extra inputs, unless noted otherwise.
#[derive(Debug)]
pub struct Compensation {
    /// Inputs of the query block (by index) that the view does not cover.
    pub extra_inputs: Vec<usize>,
    pub predicate: Vec<ExprImpl>,
    pub agg: Option<CompensationAgg>,
    /// Over the group keys followed by the aggregate calls if `agg` is set.
    pub output: Vec<ExprImpl>,
}

#[derive(Debug)]
pub struct CompensationAgg {
    pub group: Vec<ExprImpl>,
    pub calls: Vec<CompensationAggCall>,
}

#[derive(Debug)]
pub struct CompensationAggCall {
    pub agg_type: AggType,
    pub return_type: DataType,
    pub args: Vec<ExprImpl>,
    pub distinct: bool,
    pub order_by: Vec<(ExprImpl, OrderType)>,
    pub filter: Vec<ExprImpl>,
    pub direct_args: Vec<Literal>,
}

/// Maps the inputs of a view block to inputs of a query block, recursively for nested blocks.
#[derive(Clone, Debug)]
struct Mapping {
    inputs: Vec<usize>,
    nested: Vec<NestedMapping>,
}

#[derive(Clone, Debug)]
enum NestedMapping {
    Leaf,
    Block(Mapping),
    Join(Mapping, Mapping),
}

/// Rewrites a term of the view into the namespace of the query.
fn translate(term: &Term, mapping: &Mapping) -> Term {
    match term {
        Term::Column(col) => {
            let (Some(input), Some(nested)) =
                (mapping.inputs.get(col.input), mapping.nested.get(col.input))
            else {
                return term.clone();
            };
            let key = match (&col.key, nested) {
                (ColKey::Nested { side: 0, term }, NestedMapping::Block(m))
                | (ColKey::Nested { side: 0, term }, NestedMapping::Join(m, _)) => ColKey::Nested {
                    side: 0,
                    term: Box::new(translate(term, m)),
                },
                (ColKey::Nested { side: 1, term }, NestedMapping::Join(_, m)) => ColKey::Nested {
                    side: 1,
                    term: Box::new(translate(term, m)),
                },
                (key, _) => key.clone(),
            };
            Term::Column(ColumnRef {
                input: *input,
                key,
                ty: col.ty.clone(),
            })
        }
        Term::Const { .. } => term.clone(),
        // Rebuild through `Term::func`: the canonical operand order depends on the operands.
        Term::Func { func, ty, args } => Term::func(
            *func,
            ty.clone(),
            args.iter().map(|arg| translate(arg, mapping)).collect(),
        ),
        Term::Agg(call) => Term::Agg(Box::new(translate_agg_call(call, mapping))),
    }
}

fn translate_agg_call(call: &AggCallSummary, mapping: &Mapping) -> AggCallSummary {
    AggCallSummary {
        agg_type: call.agg_type.clone(),
        return_type: call.return_type.clone(),
        args: call.args.iter().map(|t| translate(t, mapping)).collect(),
        distinct: call.distinct,
        order_by: call
            .order_by
            .iter()
            .map(|(t, order)| (translate(t, mapping), *order))
            .collect(),
        filter: canonical_set(call.filter.iter().map(|t| translate(t, mapping)).collect()),
        direct_args: call.direct_args.clone(),
    }
}

/// Enumerates injective mappings from `view` inputs to equivalent `query` inputs.
fn enumerate_mappings(view: &[BlockInput], query: &[BlockInput], bijective: bool) -> Vec<Mapping> {
    if view.len() > query.len() || (bijective && view.len() != query.len()) {
        return vec![];
    }
    let candidates: Vec<Vec<(usize, NestedMapping)>> = view
        .iter()
        .enumerate()
        .map(|(v_idx, v_input)| {
            query
                .iter()
                .enumerate()
                .flat_map(|(q_idx, q_input)| {
                    equivalent_inputs(v_input, v_idx, q_input, q_idx)
                        .into_iter()
                        .map(move |nested| (q_idx, nested))
                })
                .collect()
        })
        .collect();

    fn search(
        candidates: &[Vec<(usize, NestedMapping)>],
        used: &mut Vec<bool>,
        current: &mut Vec<(usize, NestedMapping)>,
        out: &mut Vec<Mapping>,
    ) {
        if out.len() >= MAX_MAPPINGS {
            return;
        }
        let Some(options) = candidates.get(current.len()) else {
            let (inputs, nested) = current.iter().cloned().unzip();
            out.push(Mapping { inputs, nested });
            return;
        };
        for (q_idx, nested) in options {
            if used[*q_idx] {
                continue;
            }
            used[*q_idx] = true;
            current.push((*q_idx, nested.clone()));
            search(candidates, used, current, out);
            current.pop();
            used[*q_idx] = false;
        }
    }

    let mut out = vec![];
    search(
        &candidates,
        &mut vec![false; query.len()],
        &mut vec![],
        &mut out,
    );
    out
}

/// Returns every way the view input can correspond to the query input.
fn equivalent_inputs(
    view: &BlockInput,
    v_idx: usize,
    query: &BlockInput,
    q_idx: usize,
) -> Vec<NestedMapping> {
    match (view, query) {
        (BlockInput::Table { table_id: v }, BlockInput::Table { table_id: q }) if v == q => {
            vec![NestedMapping::Leaf]
        }
        (BlockInput::Block(v), BlockInput::Block(q)) => equivalent_blocks(v, q)
            .into_iter()
            .map(NestedMapping::Block)
            .collect(),
        (
            BlockInput::Join {
                join_type: v_type,
                on: v_on,
                left: v_left,
                right: v_right,
            },
            BlockInput::Join {
                join_type: q_type,
                on: q_on,
                left: q_left,
                right: q_right,
            },
        ) if v_type == q_type => {
            let mut result = vec![];
            let lefts = equivalent_blocks(v_left, q_left);
            let rights = equivalent_blocks(v_right, q_right);
            for left in &lefts {
                for right in &rights {
                    let nested = NestedMapping::Join(left.clone(), right.clone());
                    // The join condition is in the namespace of the owning block, where this
                    // join is input `v_idx` of the view and `q_idx` of the query.
                    let mut inputs = vec![usize::MAX; v_idx + 1];
                    inputs[v_idx] = q_idx;
                    let mut nested_list = vec![NestedMapping::Leaf; v_idx + 1];
                    nested_list[v_idx] = nested.clone();
                    let mapping = Mapping {
                        inputs,
                        nested: nested_list,
                    };
                    let on = canonical_set(v_on.iter().map(|t| translate(t, &mapping)).collect());
                    if on == *q_on {
                        result.push(nested);
                    }
                }
            }
            result
        }
        _ => vec![],
    }
}

/// Returns every input mapping under which two nested blocks compute the same rows: the same
/// inputs, predicate and grouping. Their outputs are not compared, because the columns of a
/// nested block are identified by the terms they compute.
fn equivalent_blocks(view: &BlockSummary, query: &BlockSummary) -> Vec<Mapping> {
    if view.agg.is_some() != query.agg.is_some() {
        return vec![];
    }
    enumerate_mappings(&view.inputs, &query.inputs, true)
        .into_iter()
        .filter(|mapping| {
            if view.predicate.map_terms(|t| translate(t, mapping)) != query.predicate {
                return false;
            }
            match (&view.agg, &query.agg) {
                (Some(v), Some(q)) => {
                    canonical_set(v.group.iter().map(|t| translate(t, mapping)).collect())
                        == canonical_set(q.group.clone())
                }
                _ => true,
            }
        })
        .collect()
}

/// Looks up the equivalence class of a term.
struct Classes<'a> {
    classes: &'a [EqClass],
    by_term: HashMap<&'a Term, usize>,
}

impl<'a> Classes<'a> {
    fn new(predicate: &'a Predicate) -> Self {
        let mut by_term = HashMap::new();
        for (idx, class) in predicate.classes.iter().enumerate() {
            for member in &class.members {
                by_term.insert(member, idx);
            }
        }
        Self {
            classes: &predicate.classes,
            by_term,
        }
    }

    fn class_of(&self, term: &Term) -> Option<usize> {
        self.by_term.get(term).copied()
    }
}

/// How a value is used, which decides whether an equal value may stand in for it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Use {
    /// The value itself is the result: an output column, a group key, the argument of
    /// `min`/`max`.
    Whole,
    /// The value is only compared: an operand of a comparison, the argument of
    /// `count(distinct)`.
    Compared,
    /// The value is an argument of a function or of another aggregate.
    Argument,
}

fn arg_use(func: ExprType) -> Use {
    match func {
        ExprType::Equal
        | ExprType::NotEqual
        | ExprType::LessThan
        | ExprType::LessThanOrEqual
        | ExprType::GreaterThan
        | ExprType::GreaterThanOrEqual
        | ExprType::IsNull
        | ExprType::IsNotNull
        | ExprType::IsDistinctFrom
        | ExprType::IsNotDistinctFrom
        | ExprType::In => Use::Compared,
        _ => Use::Argument,
    }
}

/// The interchange rule: a term may be replaced by an SQL-equal one if it is only compared, or
/// if equal values of its type are always the same value.
fn can_substitute(term: &Term, usage: Use) -> bool {
    usage == Use::Compared || is_exact_type(&term.ty())
}

fn agg_arg_use(call: &AggCallSummary) -> Use {
    match &call.agg_type {
        AggType::Builtin(PbAggKind::Count) if call.distinct => Use::Compared,
        AggType::Builtin(PbAggKind::Min | PbAggKind::Max) => Use::Whole,
        _ => Use::Argument,
    }
}

/// Whether two terms always have interchangeable values under the given equivalences.
fn terms_equivalent(a: &Term, b: &Term, usage: Use, classes: &Classes<'_>) -> bool {
    if a == b {
        return true;
    }
    if can_substitute(a, usage)
        && let (Some(x), Some(y)) = (classes.class_of(a), classes.class_of(b))
        && x == y
    {
        return true;
    }
    match (a, b) {
        (
            Term::Func {
                func: f1,
                ty: t1,
                args: a1,
            },
            Term::Func {
                func: f2,
                ty: t2,
                args: a2,
            },
        ) if f1 == f2 && t1 == t2 && a1.len() == a2.len() => {
            let usage = arg_use(*f1);
            let pairwise = |xs: &[Term], ys: &[Term]| {
                xs.iter()
                    .zip_eq_fast(ys)
                    .all(|(x, y)| terms_equivalent(x, y, usage, classes))
            };
            if pairwise(a1, a2) {
                return true;
            }
            // The operands of a commutative function are ordered by content, so equivalent
            // operands may end up on different sides.
            let commutative = matches!(
                f1,
                ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::IsDistinctFrom
                    | ExprType::IsNotDistinctFrom
                    | ExprType::Add
                    | ExprType::Multiply
            );
            commutative
                && a1.len() == 2
                && a1[0].ty() == a1[1].ty()
                && terms_equivalent(&a1[0], &a2[1], usage, classes)
                && terms_equivalent(&a1[1], &a2[0], usage, classes)
        }
        (Term::Agg(c1), Term::Agg(c2)) => agg_calls_equivalent(c1, c2, classes),
        _ => false,
    }
}

fn agg_calls_equivalent(a: &AggCallSummary, b: &AggCallSummary, classes: &Classes<'_>) -> bool {
    let usage = agg_arg_use(a);
    a.agg_type == b.agg_type
        && a.return_type == b.return_type
        && a.distinct == b.distinct
        && a.direct_args == b.direct_args
        && a.args.len() == b.args.len()
        && a.args
            .iter()
            .zip_eq_fast(&b.args)
            .all(|(x, y)| terms_equivalent(x, y, usage, classes))
        && a.order_by.len() == b.order_by.len()
        && a.order_by
            .iter()
            .zip_eq_fast(&b.order_by)
            .all(|((x, ox), (y, oy))| ox == oy && terms_equivalent(x, y, Use::Compared, classes))
        && a.filter.len() == b.filter.len()
        && a.filter.iter().all(|x| {
            b.filter
                .iter()
                .any(|y| terms_equivalent(x, y, Use::Whole, classes))
        })
}

/// A column the compensation may read.
#[derive(Clone)]
struct Available {
    expr: ExprImpl,
    /// A group key of an aggregating view whose type is not exact. The view stores an arbitrary
    /// one of the equal values of a group, so the column may be compared or passed through, but
    /// not fed to a function.
    restricted: bool,
}

/// Computes terms of the query from the available columns.
struct Deriver<'a> {
    available: &'a HashMap<Term, Available>,
    /// Aggregate calls among the available columns.
    available_aggs: &'a [(AggCallSummary, ExprImpl)],
    /// The equivalences that may be used to replace a term by an equal one.
    classes: &'a Classes<'a>,
    /// Whether a term restricted to a single value may be replaced by that value.
    use_constants: bool,
}

impl Deriver<'_> {
    fn derive(&self, term: &Term, usage: Use) -> Option<ExprImpl> {
        self.derive_at(term, usage, 0)
    }

    fn derive_at(&self, term: &Term, usage: Use, depth: usize) -> Option<ExprImpl> {
        if let Term::Const { value, ty, .. } = term {
            return Some(Literal::new(value.clone(), ty.clone()).into());
        }
        if let Some(expr) = self.direct(term, usage) {
            return Some(expr);
        }
        if depth < MAX_SUBSTITUTION_DEPTH
            && can_substitute(term, usage)
            && let Some(class) = self.classes.class_of(term)
        {
            let class = &self.classes.classes[class];
            if self.use_constants
                && let Some(point) = class.range.as_ref().and_then(|range| range.as_point())
            {
                return Some(Literal::new(Some(point.clone()), term.ty()).into());
            }
            for member in &class.members {
                if member == term {
                    continue;
                }
                if let Some(expr) = self
                    .direct(member, usage)
                    .or_else(|| self.decompose(member, depth + 1))
                {
                    return Some(expr);
                }
            }
        }
        self.decompose(term, depth)
    }

    fn direct(&self, term: &Term, usage: Use) -> Option<ExprImpl> {
        if let Term::Agg(call) = term {
            return self
                .available_aggs
                .iter()
                .find(|(available, _)| agg_calls_equivalent(available, call, self.classes))
                .map(|(_, expr)| expr.clone());
        }
        let available = self.available.get(term)?;
        if available.restricted && usage == Use::Argument {
            return None;
        }
        Some(available.expr.clone())
    }

    fn decompose(&self, term: &Term, depth: usize) -> Option<ExprImpl> {
        let Term::Func { func, ty, args } = term else {
            return None;
        };
        let usage = arg_use(*func);
        let args = args
            .iter()
            .map(|arg| self.derive_at(arg, usage, depth))
            .collect::<Option<Vec<_>>>()?;
        Some(FunctionCall::new_unchecked(*func, args, ty.clone()).into())
    }

    fn agg_call(&self, call: &AggCallSummary) -> Option<CompensationAggCall> {
        let usage = agg_arg_use(call);
        Some(CompensationAggCall {
            agg_type: call.agg_type.clone(),
            return_type: call.return_type.clone(),
            args: call
                .args
                .iter()
                .map(|arg| self.derive(arg, usage))
                .collect::<Option<_>>()?,
            distinct: call.distinct,
            order_by: call
                .order_by
                .iter()
                .map(|(term, order)| Some((self.derive(term, Use::Compared)?, *order)))
                .collect::<Option<_>>()?,
            filter: call
                .filter
                .iter()
                .map(|term| self.derive(term, Use::Whole))
                .collect::<Option<_>>()?,
            direct_args: call.direct_args.clone(),
        })
    }
}

/// Whether an output column of an aggregating view has a well-defined value: wherever a group
/// key of a non-exact type is fed to a function, the result depends on which of the equal
/// values the stream job happened to keep. Sets `has_agg` if the term reads an aggregate.
fn is_well_defined(term: &Term, usage: Use, groups: &HashSet<&Term>, has_agg: &mut bool) -> bool {
    if groups.contains(term) {
        return usage != Use::Argument || is_exact_type(&term.ty());
    }
    match term {
        Term::Const { .. } => true,
        Term::Agg(_) => {
            *has_agg = true;
            true
        }
        Term::Func { func, args, .. } => {
            let usage = arg_use(*func);
            args.iter()
                .all(|arg| is_well_defined(arg, usage, groups, has_agg))
        }
        Term::Column(_) => false,
    }
}

/// Rewrites an output term of an aggregating query block over the output of its aggregation:
/// the group keys followed by the aggregate calls.
fn output_over_agg(term: &Term, agg: &AggSummary) -> Option<ExprImpl> {
    if let Some(idx) = agg.group.iter().position(|group| group == term) {
        return Some(InputRef::new(idx, term.ty()).into());
    }
    match term {
        Term::Agg(call) => {
            let idx = agg.calls.iter().position(|c| c == &**call)?;
            Some(InputRef::new(agg.group.len() + idx, call.return_type.clone()).into())
        }
        Term::Const { value, ty, .. } => Some(Literal::new(value.clone(), ty.clone()).into()),
        Term::Func { func, ty, args } => {
            let args = args
                .iter()
                .map(|arg| output_over_agg(arg, agg))
                .collect::<Option<Vec<_>>>()?;
            Some(FunctionCall::new_unchecked(*func, args, ty.clone()).into())
        }
        Term::Column(_) => None,
    }
}

/// Matches the query block against the root block of a view.
///
/// `query_input_cols` holds the term of every output column of the plan of every query input,
/// which is how the columns of extra inputs are addressed.
pub fn match_block(
    query: &BlockSummary,
    query_input_cols: &[Vec<Term>],
    view: &BlockSummary,
) -> Result<Compensation, MatchFailure> {
    if view.agg.is_some() && query.agg.is_none() {
        // Every row of the query stands for several rows of the view's input.
        return Err(MatchFailure::Aggregation);
    }
    let mappings = enumerate_mappings(&view.inputs, &query.inputs, false);
    let mut failure = MatchFailure::InputMapping;
    for mapping in &mappings {
        match match_with_mapping(query, query_input_cols, view, mapping) {
            Ok(compensation) => return Ok(compensation),
            Err(err) => failure = err,
        }
    }
    Err(failure)
}

fn match_with_mapping(
    query: &BlockSummary,
    query_input_cols: &[Vec<Term>],
    view: &BlockSummary,
    mapping: &Mapping,
) -> Result<Compensation, MatchFailure> {
    // From here on everything of the view is in the namespace of the query.
    let view_predicate = view.predicate.map_terms(|t| translate(t, mapping));
    let view_output: Vec<Term> = view.output.iter().map(|t| translate(t, mapping)).collect();
    let view_agg = view.agg.as_ref().map(|agg| AggSummary {
        group: agg.group.iter().map(|t| translate(t, mapping)).collect(),
        calls: agg
            .calls
            .iter()
            .map(|call| translate_agg_call(call, mapping))
            .collect(),
    });
    let query_classes = Classes::new(&query.predicate);
    let view_classes = Classes::new(&view_predicate);

    let covered: HashSet<usize> = mapping.inputs.iter().copied().collect();
    let extra_inputs: Vec<usize> = (0..query.inputs.len())
        .filter(|idx| !covered.contains(idx))
        .collect();
    if view_agg.is_some() && !extra_inputs.is_empty() {
        // Joining more inputs to an aggregated view changes what is aggregated.
        return Err(MatchFailure::InputMapping);
    }

    // Check 2: the predicate of the query implies the predicate of the view.
    for class in &view_predicate.classes {
        let mut query_class = None;
        for member in &class.members {
            let found = query_classes
                .class_of(member)
                .ok_or(MatchFailure::Predicate)?;
            if query_class.is_some_and(|prev| prev != found) {
                return Err(MatchFailure::Predicate);
            }
            query_class = Some(found);
        }
        if let (Some(view_range), Some(query_class)) = (&class.range, query_class) {
            let implied = query.predicate.classes[query_class]
                .range
                .as_ref()
                .is_some_and(|query_range| query_range.is_subset_of(view_range));
            if !implied {
                return Err(MatchFailure::Predicate);
            }
        }
    }
    let mut extra_residuals: Vec<&Term> = query.predicate.residuals.iter().collect();
    for residual in &view_predicate.residuals {
        let found = extra_residuals
            .iter()
            .position(|q| terms_equivalent(residual, q, Use::Whole, &query_classes))
            .ok_or(MatchFailure::Predicate)?;
        extra_residuals.remove(found);
    }

    // The columns the compensation may read: the output of the view, then the extra inputs.
    // Below an aggregating view only its well-defined, aggregate-free columns can be filtered
    // and grouped on (`available`); its aggregates join them for the output (`available_out`).
    let mut available: HashMap<Term, Available> = HashMap::new();
    let mut available_out: HashMap<Term, Available> = HashMap::new();
    let mut available_aggs: Vec<(AggCallSummary, ExprImpl)> = vec![];
    let groups: HashSet<&Term> = view_agg.iter().flat_map(|agg| &agg.group).collect();
    for (idx, term) in view_output.iter().enumerate() {
        let expr: ExprImpl = InputRef::new(idx, term.ty()).into();
        if view_agg.is_none() {
            available.entry(term.clone()).or_insert(Available {
                expr,
                restricted: false,
            });
            continue;
        }
        let mut has_agg = false;
        if !is_well_defined(term, Use::Whole, &groups, &mut has_agg) {
            continue;
        }
        let column = Available {
            expr: expr.clone(),
            restricted: groups.contains(term) && !is_exact_type(&term.ty()),
        };
        if let Term::Agg(call) = term {
            available_aggs.push(((**call).clone(), expr));
        } else {
            available_out
                .entry(term.clone())
                .or_insert_with(|| column.clone());
            if !has_agg {
                available.entry(term.clone()).or_insert(column);
            }
        }
    }
    let mut offset = view_output.len();
    for input in &extra_inputs {
        for (idx, term) in query_input_cols[*input].iter().enumerate() {
            available.entry(term.clone()).or_insert(Available {
                expr: InputRef::new(offset + idx, term.ty()).into(),
                restricted: false,
            });
        }
        offset += query_input_cols[*input].len();
    }

    let no_aggs: &[(AggCallSummary, ExprImpl)] = &[];
    // Equalities that the query adds may only be checked on what the view itself guarantees.
    let by_view = Deriver {
        available: &available,
        available_aggs: no_aggs,
        classes: &view_classes,
        use_constants: false,
    };
    // A value set is checked on any member of the query's class, but never on the constant it
    // restricts that class to.
    let by_query_strict = Deriver {
        available: &available,
        available_aggs: no_aggs,
        classes: &query_classes,
        use_constants: false,
    };
    // Everything else is evaluated on rows that already satisfy the predicate of the query.
    let by_query = Deriver {
        available: &available,
        available_aggs: no_aggs,
        classes: &query_classes,
        use_constants: true,
    };

    // Checks 2 and 3: the compensating predicate, written over the available columns.
    let mut predicate = vec![];
    for class in &query.predicate.classes {
        // Split the class by what the view already knows to be equal.
        let mut parts: Vec<(Option<usize>, Vec<&Term>)> = vec![];
        for member in &class.members {
            match view_classes.class_of(member) {
                Some(view_class) => match parts.iter_mut().find(|(c, _)| *c == Some(view_class)) {
                    Some((_, members)) => members.push(member),
                    None => parts.push((Some(view_class), vec![member])),
                },
                None => parts.push((None, vec![member])),
            }
        }
        if parts.len() > 1 {
            let exprs = parts
                .iter()
                .map(|(_, members)| {
                    members
                        .iter()
                        .find_map(|member| by_view.derive(member, Use::Compared))
                })
                .collect::<Option<Vec<_>>>()
                .ok_or(MatchFailure::NotDerivable)?;
            for expr in &exprs[1..] {
                predicate.push(
                    FunctionCall::new_unchecked(
                        ExprType::Equal,
                        vec![exprs[0].clone(), expr.clone()],
                        DataType::Boolean,
                    )
                    .into(),
                );
            }
        }
        if let Some(query_range) = &class.range {
            let mut view_range = None;
            for (view_class, _) in &parts {
                let Some(range) = view_class.and_then(|c| view_predicate.classes[c].range.as_ref())
                else {
                    continue;
                };
                view_range = Some(match view_range {
                    None => range.clone(),
                    Some(prev) => range.intersect(&prev).ok_or(MatchFailure::Predicate)?,
                });
            }
            if view_range.as_ref() != Some(query_range) {
                let operand = class
                    .members
                    .iter()
                    .find_map(|member| by_query_strict.derive(member, Use::Compared))
                    .ok_or(MatchFailure::NotDerivable)?;
                predicate.push(query_range.to_expr(operand));
            }
        }
    }
    for residual in extra_residuals {
        predicate.push(
            by_query
                .derive(residual, Use::Whole)
                .ok_or(MatchFailure::NotDerivable)?,
        );
    }

    let derive_all = |deriver: &Deriver<'_>, terms: &[Term]| {
        terms
            .iter()
            .map(|term| deriver.derive(term, Use::Whole))
            .collect::<Option<Vec<_>>>()
            .ok_or(MatchFailure::NotDerivable)
    };
    let output_over = |agg: &AggSummary| {
        query
            .output
            .iter()
            .map(|term| output_over_agg(term, agg))
            .collect::<Option<Vec<_>>>()
            .ok_or(MatchFailure::NotDerivable)
    };

    // Check 4: aggregation.
    let (agg, output) = match (&view_agg, &query.agg) {
        (None, None) => (None, derive_all(&by_query, &query.output)?),
        // The view keeps every joined row, duplicates included, so the query aggregates it the
        // same way it aggregates the base tables.
        (None, Some(query_agg)) => {
            let group = derive_all(&by_query, &query_agg.group)?;
            let calls = query_agg
                .calls
                .iter()
                .map(|call| by_query.agg_call(call))
                .collect::<Option<Vec<_>>>()
                .ok_or(MatchFailure::NotDerivable)?;
            (
                Some(CompensationAgg { group, calls }),
                output_over(query_agg)?,
            )
        }
        (Some(_), None) => return Err(MatchFailure::Aggregation),
        (Some(view_agg), Some(query_agg)) => {
            if view_agg.group.is_empty() && !query_agg.group.is_empty() {
                // The view always has one row; the query has none without matching rows.
                return Err(MatchFailure::Aggregation);
            }
            // The compensating predicate above and the group keys here read group columns of
            // the view only, so filtering and grouping its rows is the same as filtering and
            // grouping the rows it aggregated.
            let group = derive_all(&by_query, &query_agg.group)?;

            // The groups of the query are exactly the remaining groups of the view if every
            // group key of the view is a group key of the query or fixed to one value. A query
            // without `GROUP BY` returns a row even when nothing matches, so it never takes the
            // rows of a grouped view as they are.
            let same_groups = view_agg.group.is_empty() == query_agg.group.is_empty()
                && view_agg.group.iter().all(|view_group| {
                    query_agg.group.iter().any(|query_group| {
                        terms_equivalent(view_group, query_group, Use::Compared, &query_classes)
                    }) || query_classes.class_of(view_group).is_some_and(|class| {
                        query.predicate.classes[class]
                            .range
                            .as_ref()
                            .is_some_and(|range| range.as_point().is_some())
                    })
                });
            let with_aggs = Deriver {
                available: &available_out,
                available_aggs: &available_aggs,
                classes: &query_classes,
                use_constants: true,
            };
            if same_groups {
                // Any aggregate function works: its result is read from the view as is.
                (None, derive_all(&with_aggs, &query.output)?)
            } else {
                let calls = query_agg
                    .calls
                    .iter()
                    .map(|call| rollup_call(call, &with_aggs, &by_query))
                    .collect::<Option<Vec<_>>>()
                    .ok_or(MatchFailure::Aggregation)?;
                (
                    Some(CompensationAgg { group, calls }),
                    output_over(query_agg)?,
                )
            }
        }
    };

    Ok(Compensation {
        extra_inputs,
        predicate,
        agg,
        output,
    })
}

/// Computes an aggregate call of the query from the rows of an aggregating view whose groups
/// are finer than those of the query.
fn rollup_call(
    call: &AggCallSummary,
    with_aggs: &Deriver<'_>,
    by_group_columns: &Deriver<'_>,
) -> Option<CompensationAggCall> {
    // Decomposable: the view has the same call, and its results can be aggregated again.
    if !call.distinct
        && call.order_by.is_empty()
        && let Some(total) = call.agg_type.partial_to_total()
        && let Some(partial) = with_aggs.direct(&Term::Agg(Box::new(call.clone())), Use::Argument)
    {
        return Some(CompensationAggCall {
            agg_type: total,
            return_type: call.return_type.clone(),
            args: vec![partial],
            distinct: false,
            order_by: vec![],
            filter: vec![],
            direct_args: vec![],
        });
    }
    // Insensitive to duplicates: the call reads group columns of the view only, and every
    // group of the view stands for one or more identical inputs.
    let duplicate_insensitive = call.distinct
        || matches!(
            call.agg_type,
            AggType::Builtin(PbAggKind::Min | PbAggKind::Max)
        );
    if duplicate_insensitive {
        return by_group_columns.agg_call(call);
    }
    None
}
