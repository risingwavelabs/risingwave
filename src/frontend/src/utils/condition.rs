// Copyright 2022 RisingWave Labs
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

use std::cell::OnceCell;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Debug};
use std::ops::Bound;
use std::sync::LazyLock;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::key_set::KeySet;
use risingwave_common::util::scan_range::ScanRange;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use crate::expr::{
    Expr, ExprDisplay, ExprImpl, ExprMutator, ExprRewriter, ExprType, ExprVisitor, FunctionCall,
    InequalityInputPair, InputRef, Literal, collect_input_refs, column_self_eq_eliminate,
    factorization_expr, fold_boolean_constant, push_down_not, to_conjunctions,
    try_get_bool_constant,
};
use crate::utils::condition::cast_compare::{
    ResultForCmp, ResultForEq, cast_compare_for_cmp, cast_compare_for_eq,
};
use crate::{OptimizerContext, TableCatalog};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Condition {
    /// Condition expressions in conjunction form (combined with `AND`)
    pub conjunctions: Vec<ExprImpl>,
}

impl IntoIterator for Condition {
    type IntoIter = std::vec::IntoIter<ExprImpl>;
    type Item = ExprImpl;

    fn into_iter(self) -> Self::IntoIter {
        self.conjunctions.into_iter()
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut conjunctions = self.conjunctions.iter();
        if let Some(expr) = conjunctions.next() {
            write!(f, "{:?}", expr)?;
        }
        if self.always_true() {
            write!(f, "true")?;
        } else {
            for expr in conjunctions {
                write!(f, " AND {:?}", expr)?;
            }
        }
        Ok(())
    }
}

impl Condition {
    pub fn with_expr(expr: ExprImpl) -> Self {
        let conjunctions = to_conjunctions(expr);

        Self { conjunctions }.simplify()
    }

    pub fn true_cond() -> Self {
        Self {
            conjunctions: vec![],
        }
    }

    pub fn false_cond() -> Self {
        Self {
            conjunctions: vec![ExprImpl::literal_bool(false)],
        }
    }

    pub fn always_true(&self) -> bool {
        self.conjunctions.is_empty()
    }

    pub fn always_false(&self) -> bool {
        static FALSE: LazyLock<ExprImpl> = LazyLock::new(|| ExprImpl::literal_bool(false));
        // There is at least one conjunction that is false.
        !self.conjunctions.is_empty() && self.conjunctions.contains(&*FALSE)
    }

    /// Convert condition to an expression. If always true, return `None`.
    pub fn as_expr_unless_true(&self) -> Option<ExprImpl> {
        if self.always_true() {
            None
        } else {
            Some(self.clone().into())
        }
    }

    #[must_use]
    pub fn and(self, other: Self) -> Self {
        let mut ret = self;
        ret.conjunctions.extend(other.conjunctions);
        ret.simplify()
    }

    #[must_use]
    pub fn or(self, other: Self) -> Self {
        let or_expr = ExprImpl::FunctionCall(
            FunctionCall::new_unchecked(
                ExprType::Or,
                vec![self.into(), other.into()],
                DataType::Boolean,
            )
            .into(),
        );
        let ret = Self::with_expr(or_expr);
        ret.simplify()
    }

    /// Split the condition expressions into 3 groups: left, right and others
    #[must_use]
    pub fn split(self, left_col_num: usize, right_col_num: usize) -> (Self, Self, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        self.group_by::<_, 3>(|expr| {
            let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
            if input_bits.is_subset(&left_bit_map) {
                0
            } else if input_bits.is_subset(&right_bit_map) {
                1
            } else {
                2
            }
        })
        .into_iter()
        .next_tuple()
        .unwrap()
    }

    /// Collect all `InputRef`s' indexes in the expressions.
    ///
    /// # Panics
    /// Panics if `input_ref >= input_col_num`.
    pub fn collect_input_refs(&self, input_col_num: usize) -> FixedBitSet {
        collect_input_refs(input_col_num, &self.conjunctions)
    }

    /// Split the condition expressions into (N choose 2) + 1 groups: those containing two columns
    /// from different buckets (and optionally, needing an equal condition between them), and
    /// others.
    ///
    /// `input_num_cols` are the number of columns in each of the input buckets. For instance, with
    /// bucket0: col0, col1, col2 | bucket1: col3, col4 | bucket2: col5
    /// `input_num_cols` = [3, 2, 1]
    ///
    /// Returns hashmap with keys of the form (col1, col2) where col1 < col2 in terms of their col
    /// index.
    ///
    /// `only_eq`: whether to only split those conditions with an eq condition predicate between two
    /// buckets.
    #[must_use]
    pub fn split_by_input_col_nums(
        self,
        input_col_nums: &[usize],
        only_eq: bool,
    ) -> (BTreeMap<(usize, usize), Self>, Self) {
        let mut bitmaps = Vec::with_capacity(input_col_nums.len());
        let mut cols_seen = 0;
        for cols in input_col_nums {
            bitmaps.push(FixedBitSet::from_iter(cols_seen..cols_seen + cols));
            cols_seen += cols;
        }

        let mut pairwise_conditions = BTreeMap::new();
        let mut non_eq_join = vec![];

        for expr in self.conjunctions {
            let input_bits = expr.collect_input_refs(cols_seen);
            let mut subset_indices = Vec::with_capacity(input_col_nums.len());
            for (idx, bitmap) in bitmaps.iter().enumerate() {
                if !input_bits.is_disjoint(bitmap) {
                    subset_indices.push(idx);
                }
            }
            if subset_indices.len() != 2 || (only_eq && expr.as_eq_cond().is_none()) {
                non_eq_join.push(expr);
            } else {
                // The key has the canonical ordering (lower, higher)
                let key = if subset_indices[0] < subset_indices[1] {
                    (subset_indices[0], subset_indices[1])
                } else {
                    (subset_indices[1], subset_indices[0])
                };
                let e = pairwise_conditions
                    .entry(key)
                    .or_insert_with(Condition::true_cond);
                e.conjunctions.push(expr);
            }
        }
        (
            pairwise_conditions,
            Condition {
                conjunctions: non_eq_join,
            },
        )
    }

    #[must_use]
    /// For [`EqJoinPredicate`], separate equality conditions which connect left columns and right
    /// columns from other conditions.
    ///
    /// The equality conditions are transformed into `(left_col_id, right_col_id, null_eq_null)` tuples.
    ///
    /// [`EqJoinPredicate`]: crate::optimizer::plan_node::EqJoinPredicate
    pub fn split_eq_keys(
        self,
        left_col_num: usize,
        right_col_num: usize,
    ) -> (Vec<(InputRef, InputRef, bool)>, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        let (mut eq_keys, mut others) = (vec![], vec![]);
        self.conjunctions.into_iter().for_each(|expr| {
            let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
            if input_bits.is_disjoint(&left_bit_map) || input_bits.is_disjoint(&right_bit_map) {
                others.push(expr)
            } else if let Some(columns) = expr.as_eq_cond() {
                eq_keys.push((columns.0, columns.1, false));
            } else if let Some(columns) = expr.as_is_not_distinct_from_cond() {
                eq_keys.push((columns.0, columns.1, true));
            } else {
                others.push(expr)
            }
        });

        (
            eq_keys,
            Condition {
                conjunctions: others,
            },
        )
    }

    /// For [`EqJoinPredicate`], extract inequality conditions which connect left columns and right
    /// columns from other conditions.
    ///
    /// Returns a list of `(conjunction_index, InequalityInputPair)` where the pair contains
    /// the left column index, right column index (NOT offset by `left_col_num`), and the comparison
    /// operator.
    ///
    /// Only pure `InputRef <op> InputRef` conditions are extracted (no offsets like `+ INTERVAL`).
    ///
    /// [`EqJoinPredicate`]: crate::optimizer::plan_node::EqJoinPredicate
    pub(crate) fn extract_inequality_keys(
        &self,
        left_col_num: usize,
        right_col_num: usize,
    ) -> Vec<(usize, InequalityInputPair)> {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        self.conjunctions
            .iter()
            .enumerate()
            .filter_map(|(conjunction_idx, expr)| {
                let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
                if input_bits.is_disjoint(&left_bit_map) || input_bits.is_disjoint(&right_bit_map) {
                    return None;
                }

                // Use as_comparison_cond which only matches pure InputRef <op> InputRef
                let (left_input, op, right_input) = expr.as_comparison_cond()?;

                // Ensure left is from left input and right is from right input
                // as_comparison_cond normalizes to left.index < right.index
                if left_input.index() < left_col_num
                    && right_input.index() >= left_col_num
                    && right_input.index() < left_col_num + right_col_num
                {
                    Some((
                        conjunction_idx,
                        InequalityInputPair::new(
                            left_input.index(),
                            right_input.index() - left_col_num, // Convert to right input index
                            op,
                        ),
                    ))
                } else {
                    None
                }
            })
            .collect_vec()
    }

    /// Split the condition expressions into 2 groups: those referencing `columns` and others which
    /// are disjoint with columns.
    #[must_use]
    pub fn split_disjoint(self, columns: &FixedBitSet) -> (Self, Self) {
        self.group_by::<_, 2>(|expr| {
            let input_bits = expr.collect_input_refs(columns.len());
            input_bits.is_disjoint(columns) as usize
        })
        .into_iter()
        .next_tuple()
        .unwrap()
    }

    /// x = 1 AND y = 2 AND z = 3 => [x, y, z]
    pub fn get_eq_const_input_refs(&self) -> Vec<InputRef> {
        self.conjunctions
            .iter()
            .filter_map(|expr| expr.as_eq_const().map(|(input_ref, _)| input_ref))
            .collect()
    }

    /// Splits the condition into the scan ranges of `table` it can select from, in key order, and
    /// what is left to filter. An empty list of scan ranges means the whole table, unless what is
    /// left is false.
    pub fn split_to_scan_ranges(
        self,
        table: &TableCatalog,
        ctx: &OptimizerContext,
    ) -> (Vec<ScanRange>, Self) {
        let extractor = KeySetExtractor::new(&table.pk, ctx);
        let (keys, tight) = extractor.extract(&self.conjunctions);
        if keys.is_empty() {
            return (vec![], Condition::false_cond());
        }
        let max_split_range_gap = ctx.session_ctx().config().max_split_range_gap() as u64;
        let scan_ranges = keys.into_scan_ranges(&extractor.order, max_split_range_gap);
        (scan_ranges, self.without_tight(tight))
    }

    /// The one scan range of `table` with all the rows the condition can select, for a reader of a
    /// single range. It can also have other rows, so the condition still has to be evaluated.
    pub fn covering_scan_range(
        &self,
        table: &TableCatalog,
        ctx: &OptimizerContext,
    ) -> Option<ScanRange> {
        let extractor = KeySetExtractor::new(&table.pk, ctx);
        let (keys, _) = extractor.extract(&self.conjunctions);
        keys.covering_scan_range(&extractor.order)
    }

    /// If equalities and lists such as `a IN (1, 2)` narrow the pk of `table` to several values,
    /// returns them and what is left to filter.
    pub fn split_to_pk_values(
        &self,
        table: &TableCatalog,
        ctx: &OptimizerContext,
    ) -> Option<(Vec<Vec<Datum>>, Self)> {
        let extractor = KeySetExtractor::values_only(&table.pk, ctx);
        let (keys, tight) = extractor.extract(&self.conjunctions);
        let values = keys.as_values()?;
        (values.len() > 1 && tight.iter().any(|tight| *tight))
            .then(|| (values, self.clone().without_tight(tight)))
    }

    fn without_tight(self, tight: Vec<bool>) -> Self {
        let conjunctions = self
            .conjunctions
            .into_iter()
            .zip_eq_fast(tight)
            .filter_map(|(expr, tight)| (!tight).then_some(expr))
            .collect();
        Self { conjunctions }
    }

    /// Split the condition expressions into `N` groups.
    /// An expression `expr` is in the `i`-th group if `f(expr)==i`.
    ///
    /// # Panics
    /// Panics if `f(expr)>=N`.
    #[must_use]
    pub fn group_by<F, const N: usize>(self, f: F) -> [Self; N]
    where
        F: Fn(&ExprImpl) -> usize,
    {
        const EMPTY: Vec<ExprImpl> = vec![];
        let mut groups = [EMPTY; N];
        for (key, group) in &self.conjunctions.into_iter().chunk_by(|expr| {
            // i-th group
            let i = f(expr);
            assert!(i < N);
            i
        }) {
            groups[key].extend(group);
        }

        groups.map(|group| Condition {
            conjunctions: group,
        })
    }

    #[must_use]
    pub fn rewrite_expr(self, rewriter: &mut (impl ExprRewriter + ?Sized)) -> Self {
        Self {
            conjunctions: self
                .conjunctions
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect(),
        }
        .simplify()
    }

    pub fn visit_expr<V: ExprVisitor + ?Sized>(&self, visitor: &mut V) {
        self.conjunctions
            .iter()
            .for_each(|expr| visitor.visit_expr(expr));
    }

    pub fn visit_expr_mut(&mut self, mutator: &mut (impl ExprMutator + ?Sized)) {
        self.conjunctions
            .iter_mut()
            .for_each(|expr| mutator.visit_expr(expr))
    }

    /// Simplify conditions
    /// It simplify conditions by applying constant folding and removing unnecessary conjunctions
    fn simplify(self) -> Self {
        // boolean constant folding
        let conjunctions: Vec<_> = self
            .conjunctions
            .into_iter()
            .map(push_down_not)
            .map(fold_boolean_constant)
            .map(column_self_eq_eliminate)
            .flat_map(to_conjunctions)
            .collect();
        let mut res: Vec<ExprImpl> = Vec::new();
        let mut visited: HashSet<ExprImpl> = HashSet::new();
        for expr in conjunctions {
            // factorization_expr requires hash-able ExprImpl
            if !expr.has_subquery() {
                let results_of_factorization = factorization_expr(expr);
                res.extend(
                    results_of_factorization
                        .clone()
                        .into_iter()
                        .filter(|expr| !visited.contains(expr)),
                );
                visited.extend(results_of_factorization);
            } else {
                // for subquery, simply give up factorization
                res.push(expr);
            }
        }
        // remove all constant boolean `true`
        res.retain(|expr| {
            if let Some(v) = try_get_bool_constant(expr)
                && v
            {
                false
            } else {
                true
            }
        });
        // if there is a `false` in conjunctions, the whole condition will be `false`
        for expr in &mut res {
            if let Some(v) = try_get_bool_constant(expr)
                && !v
            {
                res.clear();
                res.push(ExprImpl::literal_bool(false));
                break;
            }
        }
        Self { conjunctions: res }
    }
}

/// The most ranges to get by narrowing several values of one pk column with the next columns, as in
/// `a IN (...) AND b IN (...)`. Beyond it, the later conditions stay as a filter.
const MAX_REFINED_RANGES: usize = 1024;

/// A predicate in the form [`KeySetExtractor`] works on: row comparisons are expanded and constants
/// are folded.
struct KeyPredicate {
    kind: KeyPredicateKind,
    /// The keys of this predicate over the pk columns starting at each offset, and whether they are
    /// tight. A nested predicate that cannot be narrowed is asked again for each later pk column, so
    /// without this the work grows exponentially with the nesting.
    extracted: Vec<OnceCell<(KeySet, bool)>>,
}

enum KeyPredicateKind {
    And(Vec<KeyPredicate>),
    Or(Vec<KeyPredicate>),
    /// NULL is a value here, for `IS NULL`. With no values, this is never true.
    Values {
        column_index: usize,
        values: Vec<Datum>,
    },
    Between {
        column_index: usize,
        lower: Bound<ScalarImpl>,
        upper: Bound<ScalarImpl>,
    },
    Unknown,
}

/// Derives the keys a predicate can select, in one recursion over the expression like `CockroachDB`'s
/// `idxconstraint`. Two properties hold for every result:
/// - If the predicate is true for a row, the key of the row is in the set.
/// - If the result is tight, the converse also holds, so the predicate need not be evaluated again.
///
/// This is only an optimization, so anything it cannot understand gives the full set, not tight, and
/// the predicate stays as a filter. It works on copies of the expressions and does not depend on the
/// optimization phase of the caller.
struct KeySetExtractor<'a> {
    pk: &'a [ColumnOrder],
    order: Vec<OrderType>,
    ctx: &'a OptimizerContext,
    /// Only understand equalities and lists, so that the keys are all values of the pk.
    values_only: bool,
}

/// How a constant compares with a value of a column of another type.
enum ComparedValue {
    Value(ScalarImpl),
    /// Equal to no value of the column, or NULL, so the comparison is never true.
    Never,
    /// Cannot tell, so the comparison stays as a filter.
    Unknown,
}

impl<'a> KeySetExtractor<'a> {
    fn new(pk: &'a [ColumnOrder], ctx: &'a OptimizerContext) -> Self {
        Self {
            pk,
            order: pk.iter().map(|column| column.order_type).collect(),
            ctx,
            values_only: false,
        }
    }

    fn values_only(pk: &'a [ColumnOrder], ctx: &'a OptimizerContext) -> Self {
        Self {
            values_only: true,
            ..Self::new(pk, ctx)
        }
    }

    /// Returns the keys for the conjunction of `conjunctions`, and whether each of them is tight.
    fn extract(&self, conjunctions: &[ExprImpl]) -> (KeySet, Vec<bool>) {
        if self.pk.is_empty() {
            return (KeySet::full(), vec![false; conjunctions.len()]);
        }
        let predicates = conjunctions
            .iter()
            .map(|expr| self.build(expr))
            .collect_vec();
        self.extract_and(&predicates.iter().collect_vec(), 0)
    }

    fn build(&self, expr: &ExprImpl) -> KeyPredicate {
        let build_all = |exprs: &[ExprImpl]| exprs.iter().map(|expr| self.build(expr)).collect();
        let conjunctions = to_conjunctions(expr.clone());
        let kind = if let Some(disjunctions) = expr.as_or_disjunctions() {
            KeyPredicateKind::Or(build_all(&disjunctions))
        } else if conjunctions.len() > 1 {
            KeyPredicateKind::And(build_all(&conjunctions))
        } else if let Some(definition) = self.row_comparison_definition(expr) {
            return self.build(&definition);
        } else {
            self.build_leaf(expr)
        };
        KeyPredicate {
            kind,
            extracted: vec![OnceCell::new(); self.pk.len()],
        }
    }

    /// `(a, b) > (1, 5)` by definition: `a > 1 OR (a = 1 AND b > 5)`. A plan cannot use this form,
    /// because it evaluates the fields more than once, but it is exact for deriving keys, whatever
    /// the order of the pk columns and wherever the NULLs are.
    fn row_comparison_definition(&self, expr: &ExprImpl) -> Option<ExprImpl> {
        let call = expr.as_function_call()?;
        let (strict, last) = match call.func_type() {
            ExprType::RowLessThan => (ExprType::LessThan, ExprType::LessThan),
            ExprType::RowLessThanOrEqual => (ExprType::LessThan, ExprType::LessThanOrEqual),
            ExprType::RowGreaterThan => (ExprType::GreaterThan, ExprType::GreaterThan),
            ExprType::RowGreaterThanOrEqual => {
                (ExprType::GreaterThan, ExprType::GreaterThanOrEqual)
            }
            _ => return None,
        };
        let [left, right] = call.inputs() else {
            return None;
        };
        let left = left.as_function_call()?;
        if left.func_type() != ExprType::Row || !right.is_const() {
            return None;
        }
        let right_type = right.return_type();
        let right = self.fold(right.clone())??;
        let fields = left.inputs();
        let values = right.as_struct().fields();
        let field_types = right_type.as_struct().types().collect_vec();

        let compare = |func_type, i: usize| -> ExprImpl {
            let value = Literal::new(values[i].clone(), field_types[i].clone());
            FunctionCall::new_unchecked(
                func_type,
                vec![fields[i].clone(), value.into()],
                DataType::Boolean,
            )
            .into()
        };
        Some(ExprImpl::or((0..fields.len()).map(|i| {
            let func_type = if i + 1 == fields.len() { last } else { strict };
            ExprImpl::and(
                (0..i)
                    .map(|j| compare(ExprType::Equal, j))
                    .chain(std::iter::once(compare(func_type, i))),
            )
        })))
    }

    fn build_leaf(&self, expr: &ExprImpl) -> KeyPredicateKind {
        let on_pk = |input_ref: &InputRef| {
            self.pk
                .iter()
                .any(|column| column.column_index == input_ref.index)
        };
        let values_of = |input_ref: &InputRef, values| KeyPredicateKind::Values {
            column_index: input_ref.index,
            values,
        };

        if let Some((input_ref, value)) = expr.as_eq_const() {
            if !on_pk(&input_ref) {
                return KeyPredicateKind::Unknown;
            }
            match self.equal_value(value, &input_ref.data_type) {
                ComparedValue::Value(value) => values_of(&input_ref, vec![Some(value)]),
                ComparedValue::Never => values_of(&input_ref, vec![]),
                ComparedValue::Unknown => KeyPredicateKind::Unknown,
            }
        } else if let Some(input_ref) = expr.as_is_null() {
            values_of(&input_ref, vec![None])
        } else if let Some((input_ref, list)) = expr
            .as_in_const_list()
            .or_else(|| expr.as_some_eq_const_list())
        {
            if !on_pk(&input_ref) {
                return KeyPredicateKind::Unknown;
            }
            let mut values = Vec::with_capacity(list.len());
            for value in list {
                match self.equal_value(value, &input_ref.data_type) {
                    ComparedValue::Value(value) => values.push(Some(value)),
                    ComparedValue::Never => {}
                    ComparedValue::Unknown => return KeyPredicateKind::Unknown,
                }
            }
            values_of(&input_ref, values)
        } else if let Some((input_ref, func_type, value)) = expr.as_comparison_const() {
            if self.values_only || !on_pk(&input_ref) {
                return KeyPredicateKind::Unknown;
            }
            let value = match value.clone().cast_implicit(&input_ref.data_type) {
                Ok(value) => value,
                Err(_) => {
                    match cast_compare_for_cmp(value, input_ref.data_type.clone(), func_type) {
                        Ok(ResultForCmp::Success(value)) => value,
                        _ => return KeyPredicateKind::Unknown,
                    }
                }
            };
            let Some(value) = self.fold(value) else {
                return KeyPredicateKind::Unknown;
            };
            let Some(value) = value else {
                return values_of(&input_ref, vec![]);
            };
            let (lower, upper) = match func_type {
                ExprType::LessThan => (Bound::Unbounded, Bound::Excluded(value)),
                ExprType::LessThanOrEqual => (Bound::Unbounded, Bound::Included(value)),
                ExprType::GreaterThan => (Bound::Excluded(value), Bound::Unbounded),
                ExprType::GreaterThanOrEqual => (Bound::Included(value), Bound::Unbounded),
                _ => unreachable!(),
            };
            KeyPredicateKind::Between {
                column_index: input_ref.index,
                lower,
                upper,
            }
        } else {
            KeyPredicateKind::Unknown
        }
    }

    fn equal_value(&self, value: ExprImpl, data_type: &DataType) -> ComparedValue {
        let value = match value.clone().cast_implicit(data_type) {
            Ok(value) => value,
            Err(_) => match cast_compare_for_eq(value, data_type.clone()) {
                Ok(ResultForEq::Success(value)) => value,
                Ok(ResultForEq::NeverEqual) => return ComparedValue::Never,
                Err(_) => return ComparedValue::Unknown,
            },
        };
        match self.fold(value) {
            Some(Some(value)) => ComparedValue::Value(value),
            Some(None) => ComparedValue::Never,
            None => ComparedValue::Unknown,
        }
    }

    /// Folds a constant on a copy, with the session timezone inlined, so that it also works before
    /// that optimization phase. Returns `None` if it cannot be evaluated here.
    fn fold(&self, expr: ExprImpl) -> Option<Datum> {
        self.ctx
            .session_timezone()
            .rewrite_expr(expr)
            .fold_const()
            .ok()
    }

    /// Extracts over the pk columns starting at `offset`, for the conjunction of `predicates`.
    /// Returns whether each of them is tight.
    fn extract_and(&self, predicates: &[&KeyPredicate], offset: usize) -> (KeySet, Vec<bool>) {
        let order = &self.order[offset..];
        let mut keys = KeySet::full();
        let mut tight = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            let (predicate_keys, predicate_tight) = self.extract_predicate(predicate, offset);
            keys = keys.intersect(predicate_keys, order);
            tight.push(*predicate_tight);
        }

        if !keys.is_empty() && offset + 1 < self.pk.len() && keys.is_single_values() {
            let pending = (0..predicates.len()).filter(|&i| !tight[i]).collect_vec();
            if !pending.is_empty() {
                let next = pending.iter().map(|&i| predicates[i]).collect_vec();
                let (rest, rest_tight) = self.extract_and(&next, offset + 1);
                if !rest.is_full()
                    && (keys.len() == 1 || keys.len() * rest.len() <= MAX_REFINED_RANGES)
                {
                    keys = keys.refine(&rest);
                    for (i, rest_tight) in pending.into_iter().zip_eq_fast(rest_tight) {
                        tight[i] = rest_tight;
                    }
                }
            }
        }

        if keys.is_empty() {
            tight.fill(true);
        }
        (keys, tight)
    }

    fn extract_or(&self, predicates: &[KeyPredicate], offset: usize) -> (KeySet, bool) {
        let mut sets = Vec::with_capacity(predicates.len());
        let mut tight = true;
        for predicate in predicates {
            let (keys, predicate_tight) = self.extract_predicate(predicate, offset);
            if keys.is_full() {
                return (KeySet::full(), false);
            }
            sets.push(keys.clone());
            tight &= predicate_tight;
        }
        (KeySet::union_all(sets, &self.order[offset..]), tight)
    }

    fn extract_predicate<'p>(
        &self,
        predicate: &'p KeyPredicate,
        offset: usize,
    ) -> &'p (KeySet, bool) {
        predicate.extracted[offset].get_or_init(|| {
            let column = &self.pk[offset];
            match &predicate.kind {
                KeyPredicateKind::And(predicates) => {
                    let (keys, tight) = self.extract_and(&predicates.iter().collect_vec(), offset);
                    (keys, tight.into_iter().all(|tight| tight))
                }
                KeyPredicateKind::Or(predicates) => self.extract_or(predicates, offset),
                KeyPredicateKind::Values {
                    column_index,
                    values,
                } if *column_index == column.column_index => (
                    KeySet::any_of(values.iter().cloned(), column.order_type),
                    true,
                ),
                KeyPredicateKind::Between {
                    column_index,
                    lower,
                    upper,
                } if *column_index == column.column_index => (
                    KeySet::between(lower.clone(), upper.clone(), column.order_type),
                    true,
                ),
                _ => (KeySet::full(), false),
            }
        })
    }
}

pub struct ConditionDisplay<'a> {
    pub condition: &'a Condition,
    pub input_schema: &'a Schema,
}

impl ConditionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.condition.always_true() {
            write!(f, "true")
        } else {
            write!(
                f,
                "{}",
                self.condition
                    .conjunctions
                    .iter()
                    .format_with(" AND ", |expr, f| {
                        f(&ExprDisplay {
                            expr,
                            input_schema: self.input_schema,
                        })
                    })
            )
        }
    }
}

impl fmt::Display for ConditionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Debug for ConditionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

/// `cast_compare` can be summarized as casting to target type which can be compared but can't be
/// cast implicitly to, like:
/// 1. bigger range -> smaller range in same type, e.g. int64 -> int32
/// 2. different type, e.g. float type -> integral type
mod cast_compare {
    use risingwave_common::types::DataType;

    use crate::expr::{Expr, ExprImpl, ExprType};

    enum ShrinkResult {
        OutUpperBound,
        OutLowerBound,
        InRange(ExprImpl),
    }

    pub enum ResultForEq {
        Success(ExprImpl),
        NeverEqual,
    }

    pub enum ResultForCmp {
        Success(ExprImpl),
        OutUpperBound,
        OutLowerBound,
    }

    pub fn cast_compare_for_eq(const_expr: ExprImpl, target: DataType) -> Result<ResultForEq, ()> {
        match (const_expr.return_type(), &target) {
            (DataType::Int64, DataType::Int32)
            | (DataType::Int64, DataType::Int16)
            | (DataType::Int32, DataType::Int16) => match shrink_integral(const_expr, target)? {
                ShrinkResult::InRange(expr) => Ok(ResultForEq::Success(expr)),
                ShrinkResult::OutUpperBound | ShrinkResult::OutLowerBound => {
                    Ok(ResultForEq::NeverEqual)
                }
            },
            _ => Err(()),
        }
    }

    pub fn cast_compare_for_cmp(
        const_expr: ExprImpl,
        target: DataType,
        _op: ExprType,
    ) -> Result<ResultForCmp, ()> {
        match (const_expr.return_type(), &target) {
            (DataType::Int64, DataType::Int32)
            | (DataType::Int64, DataType::Int16)
            | (DataType::Int32, DataType::Int16) => match shrink_integral(const_expr, target)? {
                ShrinkResult::InRange(expr) => Ok(ResultForCmp::Success(expr)),
                ShrinkResult::OutUpperBound => Ok(ResultForCmp::OutUpperBound),
                ShrinkResult::OutLowerBound => Ok(ResultForCmp::OutLowerBound),
            },
            _ => Err(()),
        }
    }

    fn shrink_integral(const_expr: ExprImpl, target: DataType) -> Result<ShrinkResult, ()> {
        let (upper_bound, lower_bound) = match (const_expr.return_type(), &target) {
            (DataType::Int64, DataType::Int32) => (i32::MAX as i64, i32::MIN as i64),
            (DataType::Int64, DataType::Int16) | (DataType::Int32, DataType::Int16) => {
                (i16::MAX as i64, i16::MIN as i64)
            }
            _ => unreachable!(),
        };
        match const_expr.fold_const().map_err(|_| ())? {
            Some(scalar) => {
                let value = scalar.as_integral();
                if value > upper_bound {
                    Ok(ShrinkResult::OutUpperBound)
                } else if value < lower_bound {
                    Ok(ShrinkResult::OutLowerBound)
                } else {
                    Ok(ShrinkResult::InRange(
                        const_expr.cast_explicit(&target).unwrap(),
                    ))
                }
            }
            None => Ok(ShrinkResult::InRange(
                const_expr.cast_explicit(&target).unwrap(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_split() {
        let left_col_num = 3;
        let right_col_num = 2;

        let ty = DataType::Int32;

        let mut rng = rand::rng();

        let left: ExprImpl = FunctionCall::new(
            ExprType::LessThanOrEqual,
            vec![
                InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into(),
            ],
        )
        .unwrap()
        .into();

        let right: ExprImpl = FunctionCall::new(
            ExprType::LessThan,
            vec![
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let other: ExprImpl = FunctionCall::new(
            ExprType::GreaterThan,
            vec![
                InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty,
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let cond = Condition::with_expr(other.clone())
            .and(Condition::with_expr(right.clone()))
            .and(Condition::with_expr(left.clone()));

        let res = cond.split(left_col_num, right_col_num);

        assert_eq!(res.0.conjunctions, vec![left]);
        assert_eq!(res.1.conjunctions, vec![right]);
        assert_eq!(res.2.conjunctions, vec![other]);
    }

    #[test]
    fn test_self_eq_eliminate() {
        let left_col_num = 3;
        let right_col_num = 2;

        let ty = DataType::Int32;

        let mut rng = rand::rng();

        let x: ExprImpl = InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into();

        let left: ExprImpl = FunctionCall::new(ExprType::Equal, vec![x.clone(), x.clone()])
            .unwrap()
            .into();

        let right: ExprImpl = FunctionCall::new(
            ExprType::LessThan,
            vec![
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let other: ExprImpl = FunctionCall::new(
            ExprType::GreaterThan,
            vec![
                InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty,
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let cond = Condition::with_expr(other.clone())
            .and(Condition::with_expr(right.clone()))
            .and(Condition::with_expr(left));

        let res = cond.split(left_col_num, right_col_num);

        let left_res = FunctionCall::new(ExprType::IsNotNull, vec![x])
            .unwrap()
            .into();

        assert_eq!(res.0.conjunctions, vec![left_res]);
        assert_eq!(res.1.conjunctions, vec![right]);
        assert_eq!(res.2.conjunctions, vec![other]);
    }
}

#[cfg(test)]
mod key_set_extractor_tests {
    use std::time::{Duration, Instant};

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{Interval, StructType};
    use risingwave_common::util::sort_util::ColumnOrder;
    use risingwave_expr::expr::build_from_prost;

    use super::*;
    use crate::expr::Expr;

    const COLUMNS: usize = 3;

    fn column(index: usize) -> ExprImpl {
        InputRef::new(index, DataType::Int32).into()
    }

    fn literal(value: Option<i32>) -> ExprImpl {
        Literal::new(value.map(ScalarImpl::Int32), DataType::Int32).into()
    }

    fn call(func_type: ExprType, inputs: Vec<ExprImpl>) -> ExprImpl {
        FunctionCall::new(func_type, inputs).unwrap().into()
    }

    fn row(fields: Vec<ExprImpl>) -> ExprImpl {
        let struct_type = StructType::row_expr_type(fields.iter().map(|f| f.return_type()));
        FunctionCall::new_unchecked(ExprType::Row, fields, DataType::Struct(struct_type)).into()
    }

    fn random_value(rng: &mut StdRng) -> Option<i32> {
        if rng.random_bool(0.15) {
            None
        } else {
            Some(rng.random_range(0..4))
        }
    }

    fn random_leaf(rng: &mut StdRng) -> ExprImpl {
        let col = rng.random_range(0..COLUMNS);
        match rng.random_range(0..7) {
            0..=2 => {
                let func_type = [
                    ExprType::Equal,
                    ExprType::NotEqual,
                    ExprType::LessThan,
                    ExprType::LessThanOrEqual,
                    ExprType::GreaterThan,
                    ExprType::GreaterThanOrEqual,
                ][rng.random_range(0..6)];
                call(func_type, vec![column(col), literal(random_value(rng))])
            }
            3 => call(ExprType::IsNull, vec![column(col)]),
            4 => {
                let mut inputs = vec![column(col)];
                inputs.extend((0..rng.random_range(1..4)).map(|_| literal(random_value(rng))));
                call(ExprType::In, inputs)
            }
            5 => {
                let func_type = [
                    ExprType::RowLessThan,
                    ExprType::RowLessThanOrEqual,
                    ExprType::RowGreaterThan,
                    ExprType::RowGreaterThanOrEqual,
                ][rng.random_range(0..4)];
                let len = rng.random_range(1..=COLUMNS);
                let fields = (0..len).map(|_| column(rng.random_range(0..COLUMNS)));
                let fields = row(fields.collect());
                let values = row((0..len).map(|_| literal(random_value(rng))).collect());
                call(func_type, vec![fields, values])
            }
            _ => call(
                ExprType::LessThan,
                vec![column(col), column(rng.random_range(0..COLUMNS))],
            ),
        }
    }

    fn random_expr(rng: &mut StdRng, depth: usize) -> ExprImpl {
        if depth == 0 || rng.random_bool(0.3) {
            return random_leaf(rng);
        }
        let children = (0..rng.random_range(2..4)).map(|_| random_expr(rng, depth - 1));
        let children = children.collect_vec();
        if rng.random_bool(0.5) {
            ExprImpl::and(children)
        } else {
            ExprImpl::or(children)
        }
    }

    fn all_rows() -> Vec<OwnedRow> {
        let values = [None, Some(0), Some(1), Some(2), Some(3)];
        let mut rows = vec![];
        for a in values {
            for b in values {
                for c in values {
                    rows.push(OwnedRow::new(
                        [a, b, c]
                            .into_iter()
                            .map(|v| v.map(ScalarImpl::Int32))
                            .collect(),
                    ));
                }
            }
        }
        rows
    }

    /// Checks what [`KeySetExtractor`] promises, using the real evaluator: if the predicate is true
    /// for a row, its key is in the set, and if the result is tight, only then.
    #[tokio::test]
    async fn test_key_set_matches_the_evaluator() {
        let rows = all_rows();
        let orders = OrderType::all();
        let mut rng = StdRng::seed_from_u64(27783);
        let ctx = OptimizerContext::mock();
        for _ in 0..3000 {
            let pk_len = rng.random_range(1..=2);
            let pk = (0..pk_len)
                .map(|i| ColumnOrder::new(i, orders[rng.random_range(0..orders.len())]))
                .collect_vec();
            let order = pk.iter().map(|column| column.order_type).collect_vec();

            let conjunctions = to_conjunctions(random_expr(&mut rng, 3));
            let (keys, tight) = KeySetExtractor::new(&pk, &ctx).extract(&conjunctions);
            let predicate = ExprImpl::and(conjunctions.clone());
            let residual = ExprImpl::and(
                conjunctions
                    .iter()
                    .zip_eq_fast(&tight)
                    .filter(|(_, tight)| !**tight)
                    .map(|(expr, _)| expr.clone()),
            );
            let predicate_eval = build_from_prost(&predicate.to_expr_proto()).unwrap();
            let residual_eval = build_from_prost(&residual.to_expr_proto()).unwrap();

            for row in &rows {
                let key = row.as_inner()[..pk_len].to_vec();
                let in_keys = keys.contains(&key, &order);
                let is_true = |datum: Datum| datum == Some(ScalarImpl::Bool(true));
                let selected = is_true(predicate_eval.eval_row(row).await.unwrap());
                let kept = in_keys && is_true(residual_eval.eval_row(row).await.unwrap());
                assert!(
                    !selected || in_keys,
                    "{predicate:?} {pk:?} {row:?} {keys:?}"
                );
                assert_eq!(
                    selected, kept,
                    "{predicate:?} {tight:?} {pk:?} {row:?} {keys:?}"
                );
            }
        }
    }

    /// The limit on refined ranges is for the product of several values, and a list is as long as
    /// it is anyway.
    #[tokio::test]
    async fn test_one_value_is_narrowed_by_a_long_list() {
        let pk = (0..2)
            .map(|i| ColumnOrder::new(i, OrderType::ascending()))
            .collect_vec();
        let list = [column(1)]
            .into_iter()
            .chain((0..2000).map(|value| literal(Some(value))))
            .collect();
        let conjunctions = vec![
            call(ExprType::Equal, vec![column(0), literal(Some(1))]),
            call(ExprType::In, list),
        ];
        let ctx = OptimizerContext::mock();
        let (keys, tight) = KeySetExtractor::new(&pk, &ctx).extract(&conjunctions);
        assert_eq!(keys.len(), 2000);
        assert_eq!(tight, vec![true, true]);
    }

    /// Constants that compare equal can still give different results, so a predicate is not known
    /// by its value.
    #[tokio::test]
    async fn test_predicates_with_equal_constants_are_different() {
        let pk = [ColumnOrder::new(0, OrderType::ascending())];
        let column: ExprImpl = InputRef::new(0, DataType::Varchar).into();
        let equal_to_text_of = |months, days| {
            let interval = Interval::from_month_day_usec(months, days, 0);
            let interval = Literal::new(Some(ScalarImpl::Interval(interval)), DataType::Interval);
            let text = ExprImpl::from(interval)
                .cast_explicit(&DataType::Varchar)
                .unwrap();
            call(ExprType::Equal, vec![column.clone(), text])
        };
        let (one_month, thirty_days) = (equal_to_text_of(1, 0), equal_to_text_of(0, 30));
        assert_eq!(one_month, thirty_days);
        let predicate = ExprImpl::or([one_month, thirty_days]);
        let ctx = OptimizerContext::mock();
        let (keys, _) = KeySetExtractor::new(&pk, &ctx).extract(&[predicate]);
        for text in ["1 mon", "30 days"] {
            let key = [Some(ScalarImpl::Utf8(text.into()))];
            assert!(keys.contains(&key, &[pk[0].order_type]), "{text} {keys:?}");
        }
    }

    /// Takes minutes if a predicate is extracted once per path to it, instead of once per pk column.
    #[tokio::test]
    async fn test_nested_predicate_is_extracted_once_per_column() {
        const PK_LEN: usize = 10;

        fn nested(rng: &mut StdRng, depth: usize) -> ExprImpl {
            let mut conjunctions = (0..PK_LEN)
                .map(|i| {
                    let values = [Some(1), Some(rng.random())].map(literal);
                    call(
                        ExprType::In,
                        [column(i)].into_iter().chain(values).collect(),
                    )
                })
                .collect_vec();
            let not_on_pk = call(
                ExprType::GreaterThan,
                vec![column(PK_LEN), literal(Some(rng.random()))],
            );
            conjunctions.push(if depth == 0 {
                not_on_pk
            } else {
                // The last arm makes this the full set at every offset, so nothing gets narrowed.
                ExprImpl::or([nested(rng, depth - 1), nested(rng, depth - 1), not_on_pk])
            });
            ExprImpl::and(conjunctions)
        }

        let pk = (0..PK_LEN)
            .map(|i| ColumnOrder::new(i, OrderType::ascending()))
            .collect_vec();
        let conjunctions = to_conjunctions(nested(&mut StdRng::seed_from_u64(27783), 8));
        let ctx = OptimizerContext::mock();
        let start = Instant::now();
        let (keys, tight) = KeySetExtractor::new(&pk, &ctx).extract(&conjunctions);
        assert!(start.elapsed() < Duration::from_secs(20));
        assert!(!keys.is_empty());
        assert!(!tight.iter().all(|tight| *tight));
    }
}
