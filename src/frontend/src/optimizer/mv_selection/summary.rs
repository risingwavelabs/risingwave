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

//! The normal form of a query block.
//!
//! A *block* is a maximal select-project-join region of a plan, optionally topped by one
//! aggregation. Its summary is independent of the plan shape, the join order and where the
//! predicates sit, and it holds no plan node, so it can be cached across queries.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_common::types::{DataType, Datum, DefaultOrd, ScalarImpl, ToText};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::aggregate::AggType;
use risingwave_pb::plan_common::JoinType;

use crate::expr::{ExprImpl, ExprType, FunctionCall, Literal};

/// Identifies a column of a block input by where it comes from instead of by position, so that
/// column order and column pruning do not affect the comparison of two blocks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ColKey {
    /// A column of a table.
    Table(ColumnId),
    /// An output column of a nested block, identified by the term it computes in the nested
    /// block's own namespace. `side` is 1 for the right side of a join boundary and 0 otherwise.
    Nested { side: usize, term: Box<Term> },
    /// A column of an opaque input.
    Position(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Index into [`BlockSummary::inputs`].
    pub input: usize,
    pub key: ColKey,
    pub ty: DataType,
}

/// A column of an input, or a deterministic expression over such columns.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Term {
    Column(ColumnRef),
    Const {
        value: Datum,
        ty: DataType,
        /// Text form of the value, kept for types where SQL-equal values can still be told
        /// apart (`1.0` and `1.00`), so that two constants are the same term only if they are
        /// the same value.
        repr: Option<Box<str>>,
    },
    Func {
        func: ExprType,
        ty: DataType,
        args: Vec<Term>,
    },
    /// The result of an aggregate call. Only appears in the output of a block with aggregation.
    Agg(Box<AggCallSummary>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AggCallSummary {
    pub agg_type: AggType,
    pub return_type: DataType,
    pub args: Vec<Term>,
    pub distinct: bool,
    pub order_by: Vec<(Term, OrderType)>,
    /// Conjuncts of the `FILTER` clause.
    pub filter: Vec<Term>,
    pub direct_args: Vec<Literal>,
}

/// Types for which two SQL-equal values are always the same value. For the other types
/// (decimal `1.0 = 1.00`, float `0 = -0`, interval `'1 month' = '30 days'`, ...) a value may only
/// stand in for an equal one where it is merely compared.
pub fn is_exact_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Serial
            | DataType::Varchar
            | DataType::Bytea
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Timestamptz
    )
}

/// Types whose default order is the order of SQL comparison, so value sets can be kept on them.
fn is_rangeable_type(ty: &DataType) -> bool {
    is_exact_type(ty)
        || matches!(
            ty,
            DataType::Decimal | DataType::Float32 | DataType::Float64
        )
}

impl Term {
    pub fn constant(value: Datum, ty: DataType) -> Self {
        let repr = match &value {
            Some(scalar) if !is_exact_type(&ty) => {
                Some(scalar.as_scalar_ref_impl().to_text_with_type(&ty).into())
            }
            _ => None,
        };
        Term::Const { value, ty, repr }
    }

    /// Builds a function call in normal form, so that trivially different spellings of the same
    /// expression become the same term.
    pub fn func(func: ExprType, ty: DataType, mut args: Vec<Term>) -> Self {
        match func {
            ExprType::And | ExprType::Or => {
                let mut flat = Vec::with_capacity(args.len());
                for arg in args {
                    match arg {
                        Term::Func {
                            func: inner,
                            args: inner_args,
                            ..
                        } if inner == func => flat.extend(inner_args),
                        other => flat.push(other),
                    }
                }
                let mut flat = canonical_set(flat);
                if flat.len() == 1 {
                    return flat.pop().unwrap();
                }
                args = flat;
            }
            ExprType::Equal
            | ExprType::NotEqual
            | ExprType::IsDistinctFrom
            | ExprType::IsNotDistinctFrom
                if args.len() == 2 =>
            {
                sort_operands(&mut args);
            }
            ExprType::Add | ExprType::Multiply
                if args.len() == 2 && args[0].ty() == args[1].ty() =>
            {
                sort_operands(&mut args);
            }
            ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
                if args.len() == 2 && args[0].is_const() && !args[1].is_const() =>
            {
                args.swap(0, 1);
                let flipped = match func {
                    ExprType::LessThan => ExprType::GreaterThan,
                    ExprType::LessThanOrEqual => ExprType::GreaterThanOrEqual,
                    ExprType::GreaterThan => ExprType::LessThan,
                    _ => ExprType::LessThanOrEqual,
                };
                return Term::Func {
                    func: flipped,
                    ty,
                    args,
                };
            }
            _ => {}
        }
        Term::Func { func, ty, args }
    }

    pub fn ty(&self) -> DataType {
        match self {
            Term::Column(col) => col.ty.clone(),
            Term::Const { ty, .. } | Term::Func { ty, .. } => ty.clone(),
            Term::Agg(call) => call.return_type.clone(),
        }
    }

    pub fn is_const(&self) -> bool {
        matches!(self, Term::Const { .. })
    }

    /// A deterministic key used to put terms in a canonical order. Terms have no natural order
    /// because scalar values have none.
    fn order_key(&self) -> u64 {
        // `DefaultHasher::new` always starts from the same state.
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    /// Whether the term is `true`, `false`/`NULL`, or not a boolean constant.
    fn as_const_bool(&self) -> Option<bool> {
        match self {
            Term::Const { value, .. } => Some(matches!(value, Some(ScalarImpl::Bool(true)))),
            _ => None,
        }
    }
}

/// Orders the operands of a commutative binary function: non-constants first.
fn sort_operands(args: &mut [Term]) {
    args.sort_by_key(|t| (t.is_const(), t.order_key()));
}

/// Deduplicates the terms and puts them in a canonical order.
pub fn canonical_set(terms: Vec<Term>) -> Vec<Term> {
    let mut seen = HashSet::with_capacity(terms.len());
    let mut terms: Vec<Term> = terms
        .into_iter()
        .filter(|t| seen.insert(t.clone()))
        .collect();
    terms.sort_by_key(Term::order_key);
    terms
}

/// Splits a boolean term into its top-level conjuncts.
pub fn split_conjuncts(term: Term, out: &mut Vec<Term>) {
    match term {
        Term::Func {
            func: ExprType::And,
            args,
            ..
        } => {
            for arg in args {
                split_conjuncts(arg, out);
            }
        }
        other => out.push(other),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Bound {
    Unbounded,
    Included(ScalarImpl),
    Excluded(ScalarImpl),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Interval {
    pub low: Bound,
    pub high: Bound,
}

fn cmp_low(a: &Bound, b: &Bound) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(x), Bound::Included(y)) | (Bound::Excluded(x), Bound::Excluded(y)) => {
            x.default_cmp(y)
        }
        (Bound::Included(x), Bound::Excluded(y)) => x.default_cmp(y).then(Ordering::Less),
        (Bound::Excluded(x), Bound::Included(y)) => x.default_cmp(y).then(Ordering::Greater),
    }
}

fn cmp_high(a: &Bound, b: &Bound) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(x), Bound::Included(y)) | (Bound::Excluded(x), Bound::Excluded(y)) => {
            x.default_cmp(y)
        }
        (Bound::Included(x), Bound::Excluded(y)) => x.default_cmp(y).then(Ordering::Greater),
        (Bound::Excluded(x), Bound::Included(y)) => x.default_cmp(y).then(Ordering::Less),
    }
}

impl Interval {
    fn is_empty(&self) -> bool {
        match (&self.low, &self.high) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
            (Bound::Included(l), Bound::Included(h)) => l.default_cmp(h).is_gt(),
            (Bound::Included(l) | Bound::Excluded(l), Bound::Included(h) | Bound::Excluded(h)) => {
                l.default_cmp(h).is_ge()
            }
        }
    }

    fn as_point(&self) -> Option<&ScalarImpl> {
        match (&self.low, &self.high) {
            (Bound::Included(l), Bound::Included(h)) if l.default_cmp(h).is_eq() => Some(l),
            _ => None,
        }
    }

    fn contains(&self, other: &Interval) -> bool {
        cmp_low(&self.low, &other.low).is_le() && cmp_high(&other.high, &self.high).is_le()
    }

    fn intersect(&self, other: &Interval) -> Interval {
        let low = if cmp_low(&self.low, &other.low).is_ge() {
            &self.low
        } else {
            &other.low
        };
        let high = if cmp_high(&self.high, &other.high).is_le() {
            &self.high
        } else {
            &other.high
        };
        Interval {
            low: low.clone(),
            high: high.clone(),
        }
    }

    /// Whether `next`, which does not start before `self`, overlaps or touches `self`.
    fn connects(&self, next: &Interval) -> bool {
        match (&self.high, &next.low) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
            (Bound::Excluded(h), Bound::Excluded(l)) => l.default_cmp(h).is_lt(),
            (Bound::Included(h) | Bound::Excluded(h), Bound::Included(l) | Bound::Excluded(l)) => {
                l.default_cmp(h).is_le()
            }
        }
    }
}

/// The set of values a term may take: a union of disjoint intervals of non-null values.
///
/// A term without a value set is unconstrained and may also be `NULL`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ValueSet {
    ty: DataType,
    /// Sorted, pairwise disjoint and not touching.
    intervals: Vec<Interval>,
}

impl ValueSet {
    /// Every non-null value.
    pub fn not_null(ty: DataType) -> Self {
        Self {
            ty,
            intervals: vec![Interval {
                low: Bound::Unbounded,
                high: Bound::Unbounded,
            }],
        }
    }

    /// The values `v` with `v <cmp> value`. Returns `None` if the type has no usable order or
    /// `cmp` is not a comparison.
    pub fn from_comparison(ty: DataType, cmp: ExprType, value: ScalarImpl) -> Option<Self> {
        if !is_rangeable_type(&ty) {
            return None;
        }
        if cmp == ExprType::NotEqual {
            let below = Self::from_comparison(ty.clone(), ExprType::LessThan, value.clone())?;
            let above = Self::from_comparison(ty, ExprType::GreaterThan, value)?;
            return below.union(&above);
        }
        let (low, high) = match cmp {
            ExprType::Equal => (Bound::Included(value.clone()), Bound::Included(value)),
            ExprType::LessThan => (Bound::Unbounded, Bound::Excluded(value)),
            ExprType::LessThanOrEqual => (Bound::Unbounded, Bound::Included(value)),
            ExprType::GreaterThan => (Bound::Excluded(value), Bound::Unbounded),
            ExprType::GreaterThanOrEqual => (Bound::Included(value), Bound::Unbounded),
            _ => return None,
        };
        Some(Self {
            ty,
            intervals: vec![Interval { low, high }],
        })
    }

    fn normalize(ty: DataType, mut intervals: Vec<Interval>) -> Self {
        intervals.retain(|i| !i.is_empty());
        intervals.sort_by(|a, b| cmp_low(&a.low, &b.low));
        let mut merged: Vec<Interval> = Vec::with_capacity(intervals.len());
        for interval in intervals {
            match merged.last_mut() {
                Some(last) if last.connects(&interval) => {
                    if cmp_high(&interval.high, &last.high).is_gt() {
                        last.high = interval.high;
                    }
                }
                _ => merged.push(interval),
            }
        }
        Self {
            ty,
            intervals: merged,
        }
    }

    /// Returns `None` if the two sets are over different types.
    pub fn intersect(&self, other: &ValueSet) -> Option<ValueSet> {
        if self.ty != other.ty {
            return None;
        }
        let mut intervals = vec![];
        for a in &self.intervals {
            for b in &other.intervals {
                intervals.push(a.intersect(b));
            }
        }
        Some(Self::normalize(self.ty.clone(), intervals))
    }

    /// Returns `None` if the two sets are over different types.
    pub fn union(&self, other: &ValueSet) -> Option<ValueSet> {
        if self.ty != other.ty {
            return None;
        }
        let mut intervals = self.intervals.clone();
        intervals.extend(other.intervals.iter().cloned());
        Some(Self::normalize(self.ty.clone(), intervals))
    }

    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    pub fn is_subset_of(&self, other: &ValueSet) -> bool {
        self.ty == other.ty
            && self
                .intervals
                .iter()
                .all(|a| other.intervals.iter().any(|b| b.contains(a)))
    }

    /// The only value in the set, if there is exactly one.
    pub fn as_point(&self) -> Option<&ScalarImpl> {
        match self.intervals.as_slice() {
            [interval] => interval.as_point(),
            _ => None,
        }
    }

    /// The predicate "`operand` is in this set".
    pub fn to_expr(&self, operand: ExprImpl) -> ExprImpl {
        let literal =
            |v: &ScalarImpl| -> ExprImpl { Literal::new(Some(v.clone()), self.ty.clone()).into() };
        let call = |func: ExprType, args: Vec<ExprImpl>| -> ExprImpl {
            FunctionCall::new_unchecked(func, args, DataType::Boolean).into()
        };
        if let [interval] = self.intervals.as_slice()
            && interval.low == Bound::Unbounded
            && interval.high == Bound::Unbounded
        {
            return call(ExprType::IsNotNull, vec![operand]);
        }
        let points: Option<Vec<&ScalarImpl>> =
            self.intervals.iter().map(Interval::as_point).collect();
        if let Some(points) = points {
            return match points.as_slice() {
                [point] => call(ExprType::Equal, vec![operand, literal(point)]),
                _ => {
                    let mut args = vec![operand];
                    args.extend(points.into_iter().map(literal));
                    call(ExprType::In, args)
                }
            };
        }
        let mut disjuncts = self.intervals.iter().map(|interval| {
            if let Some(point) = interval.as_point() {
                return call(ExprType::Equal, vec![operand.clone(), literal(point)]);
            }
            let low = match &interval.low {
                Bound::Unbounded => None,
                Bound::Included(v) => Some(call(
                    ExprType::GreaterThanOrEqual,
                    vec![operand.clone(), literal(v)],
                )),
                Bound::Excluded(v) => Some(call(
                    ExprType::GreaterThan,
                    vec![operand.clone(), literal(v)],
                )),
            };
            let high = match &interval.high {
                Bound::Unbounded => None,
                Bound::Included(v) => Some(call(
                    ExprType::LessThanOrEqual,
                    vec![operand.clone(), literal(v)],
                )),
                Bound::Excluded(v) => {
                    Some(call(ExprType::LessThan, vec![operand.clone(), literal(v)]))
                }
            };
            match (low, high) {
                (Some(low), Some(high)) => call(ExprType::And, vec![low, high]),
                (Some(cond), None) | (None, Some(cond)) => cond,
                // Only a single unbounded interval has no bound, which is handled above.
                (None, None) => call(ExprType::IsNotNull, vec![operand.clone()]),
            }
        });
        let first = disjuncts.next().expect("an empty value set is never kept");
        disjuncts.fold(first, |acc, d| call(ExprType::Or, vec![acc, d]))
    }
}

/// A set of terms that are equal to each other, with the values they may take.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EqClass {
    /// In canonical order. A class with one member always has a value set.
    pub members: Vec<Term>,
    pub range: Option<ValueSet>,
}

/// The conjunction of all predicates of a block, in three parts: equivalence classes, a value
/// set per class, and the residual conjuncts that fit neither.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Predicate {
    pub classes: Vec<EqClass>,
    pub residuals: Vec<Term>,
}

enum Conjunct {
    True,
    False,
    Equal(Term, Term),
    Range(Term, ValueSet),
    Residual(Term),
}

fn classify_range(term: &Term) -> Option<(Term, ValueSet)> {
    let Term::Func { func, args, .. } = term else {
        return None;
    };
    match (func, args.as_slice()) {
        (ExprType::IsNotNull, [operand]) if !operand.is_const() => {
            Some((operand.clone(), ValueSet::not_null(operand.ty())))
        }
        (
            ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual,
            [
                operand,
                Term::Const {
                    value: Some(value),
                    ty,
                    ..
                },
            ],
        ) if !operand.is_const() && operand.ty() == *ty => Some((
            operand.clone(),
            ValueSet::from_comparison(ty.clone(), *func, value.clone())?,
        )),
        (ExprType::In, [operand, list @ ..]) if !operand.is_const() && !list.is_empty() => {
            let ty = operand.ty();
            let mut set: Option<ValueSet> = None;
            for item in list {
                let Term::Const {
                    value, ty: item_ty, ..
                } = item
                else {
                    return None;
                };
                if *item_ty != ty {
                    return None;
                }
                // `x IN (.., NULL)` is never true because of the `NULL`, so it adds no value.
                let Some(value) = value else { continue };
                let point = ValueSet::from_comparison(ty.clone(), ExprType::Equal, value.clone())?;
                set = Some(match set {
                    Some(set) => set.union(&point)?,
                    None => point,
                });
            }
            // A list of only `NULL`s is left to the residuals.
            Some((operand.clone(), set?))
        }
        // Conditions on one and the same term, combined by `OR`, or by an `AND` below an `OR`.
        (ExprType::Or | ExprType::And, parts) => {
            let mut result: Option<(Term, ValueSet)> = None;
            for part in parts {
                let (operand, set) = classify_range(part)?;
                result = Some(match result {
                    Some((prev, prev_set)) if prev == operand => {
                        let combined = if *func == ExprType::Or {
                            prev_set.union(&set)?
                        } else {
                            prev_set.intersect(&set)?
                        };
                        (prev, combined)
                    }
                    Some(_) => return None,
                    None => (operand, set),
                });
            }
            result
        }
        _ => None,
    }
}

fn classify(term: Term) -> Conjunct {
    if let Some(value) = term.as_const_bool() {
        return if value {
            Conjunct::True
        } else {
            Conjunct::False
        };
    }
    if let Term::Func {
        func: ExprType::Equal,
        args,
        ..
    } = &term
        && let [lhs, rhs] = args.as_slice()
    {
        if !lhs.is_const() && !rhs.is_const() {
            return Conjunct::Equal(lhs.clone(), rhs.clone());
        }
        if matches!(rhs, Term::Const { value: None, .. }) {
            // `x = NULL` is never true.
            return Conjunct::False;
        }
    }
    match classify_range(&term) {
        Some((operand, set)) => Conjunct::Range(operand, set),
        None => Conjunct::Residual(term),
    }
}

impl Predicate {
    /// Returns `None` if the predicate can never be true, or cannot be represented.
    pub fn from_conjuncts(conjuncts: impl IntoIterator<Item = Term>) -> Option<Self> {
        let mut flat = vec![];
        for conjunct in conjuncts {
            split_conjuncts(conjunct, &mut flat);
        }

        // Union-find over terms, with a value set per root.
        let mut class_of: HashMap<Term, usize> = HashMap::new();
        let mut members: Vec<Vec<Term>> = vec![];
        let mut ranges: Vec<Option<ValueSet>> = vec![];
        let mut residuals = vec![];

        fn class_id(
            term: &Term,
            class_of: &mut HashMap<Term, usize>,
            members: &mut Vec<Vec<Term>>,
            ranges: &mut Vec<Option<ValueSet>>,
        ) -> usize {
            if let Some(id) = class_of.get(term) {
                return *id;
            }
            let id = members.len();
            class_of.insert(term.clone(), id);
            members.push(vec![term.clone()]);
            ranges.push(None);
            id
        }
        fn restrict(slot: &mut Option<ValueSet>, set: ValueSet) -> Option<()> {
            *slot = Some(match slot.take() {
                Some(prev) => prev.intersect(&set)?,
                None => set,
            });
            Some(())
        }

        for conjunct in flat {
            match classify(conjunct) {
                Conjunct::True => {}
                Conjunct::False => return None,
                Conjunct::Equal(lhs, rhs) => {
                    let a = class_id(&lhs, &mut class_of, &mut members, &mut ranges);
                    let b = class_id(&rhs, &mut class_of, &mut members, &mut ranges);
                    if a == b {
                        continue;
                    }
                    let moved = std::mem::take(&mut members[b]);
                    for term in &moved {
                        class_of.insert(term.clone(), a);
                    }
                    members[a].extend(moved);
                    if let Some(set) = ranges[b].take() {
                        restrict(&mut ranges[a], set)?;
                    }
                }
                Conjunct::Range(operand, set) => {
                    let id = class_id(&operand, &mut class_of, &mut members, &mut ranges);
                    restrict(&mut ranges[id], set)?;
                }
                Conjunct::Residual(term) => residuals.push(term),
            }
        }

        let mut classes = vec![];
        for (members, range) in members.into_iter().zip_eq_fast(ranges) {
            if members.is_empty() {
                continue;
            }
            if range.as_ref().is_some_and(ValueSet::is_empty) {
                return None;
            }
            // Terms that are equal to something must not be `NULL`, which a value set records.
            classes.push(EqClass {
                members: canonical_set(members),
                range,
            });
        }
        classes.sort_by_key(|class| class.members[0].order_key());
        Some(Self {
            classes,
            residuals: canonical_set(residuals),
        })
    }

    /// Rebuilds the predicate after its terms were rewritten.
    pub fn map_terms(&self, mut f: impl FnMut(&Term) -> Term) -> Self {
        let mut classes: Vec<EqClass> = self
            .classes
            .iter()
            .map(|class| EqClass {
                members: canonical_set(class.members.iter().map(&mut f).collect()),
                range: class.range.clone(),
            })
            .collect();
        classes.sort_by_key(|class| class.members[0].order_key());
        Self {
            classes,
            residuals: canonical_set(self.residuals.iter().map(&mut f).collect()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggSummary {
    pub group: Vec<Term>,
    pub calls: Vec<AggCallSummary>,
}

/// What a block reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockInput {
    Table {
        table_id: TableId,
    },
    /// A nested block with aggregation.
    Block(Box<BlockSummary>),
    /// A non-inner join, with a block on each side. `on` is in the namespace of the block that
    /// owns this input.
    Join {
        join_type: JoinType,
        on: Vec<Term>,
        left: Box<BlockSummary>,
        right: Box<BlockSummary>,
    },
    /// Anything else. It never corresponds to an input of a view definition.
    Opaque,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSummary {
    pub inputs: Vec<BlockInput>,
    /// Over the columns of `inputs`.
    pub predicate: Predicate,
    pub agg: Option<AggSummary>,
    /// One term per output column. With aggregation, the terms are over the group terms and
    /// [`Term::Agg`].
    pub output: Vec<Term>,
}

impl BlockSummary {
    /// Collects every table read by the block, including by nested blocks.
    pub fn collect_tables(&self, tables: &mut HashSet<TableId>) {
        for input in &self.inputs {
            match input {
                BlockInput::Table { table_id } => {
                    tables.insert(*table_id);
                }
                BlockInput::Block(block) => block.collect_tables(tables),
                BlockInput::Join { left, right, .. } => {
                    left.collect_tables(tables);
                    right.collect_tables(tables);
                }
                BlockInput::Opaque => {}
            }
        }
    }

    pub fn has_opaque_input(&self) -> bool {
        self.inputs.iter().any(|input| match input {
            BlockInput::Table { .. } => false,
            BlockInput::Block(block) => block.has_opaque_input(),
            BlockInput::Join { left, right, .. } => {
                left.has_opaque_input() || right.has_opaque_input()
            }
            BlockInput::Opaque => true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(v: i32) -> ScalarImpl {
        ScalarImpl::Int32(v)
    }

    fn cmp(func: ExprType, v: i32) -> ValueSet {
        ValueSet::from_comparison(DataType::Int32, func, int(v)).unwrap()
    }

    fn column(input: usize, id: i32) -> Term {
        Term::Column(ColumnRef {
            input,
            key: ColKey::Table(ColumnId::new(id)),
            ty: DataType::Int32,
        })
    }

    fn compare(func: ExprType, lhs: Term, rhs: Term) -> Term {
        Term::func(func, DataType::Boolean, vec![lhs, rhs])
    }

    fn lit(v: i32) -> Term {
        Term::constant(Some(int(v)), DataType::Int32)
    }

    #[test]
    fn test_value_set_algebra() {
        let gt0 = cmp(ExprType::GreaterThan, 0);
        let gt10 = cmp(ExprType::GreaterThan, 10);
        assert!(gt10.is_subset_of(&gt0));
        assert!(!gt0.is_subset_of(&gt10));

        // (0, +inf) ∩ (-inf, 5] = (0, 5]
        let range = gt0.intersect(&cmp(ExprType::LessThanOrEqual, 5)).unwrap();
        assert!(range.is_subset_of(&gt0));
        assert!(cmp(ExprType::Equal, 5).is_subset_of(&range));
        assert!(!cmp(ExprType::Equal, 0).is_subset_of(&range));

        // Contradicting ranges are empty.
        assert!(
            gt10.intersect(&cmp(ExprType::LessThan, 10))
                .unwrap()
                .is_empty()
        );

        // `x < 5 OR x >= 5` covers every non-null value; `x < 5 OR x > 5` does not.
        let all = cmp(ExprType::LessThan, 5)
            .union(&cmp(ExprType::GreaterThanOrEqual, 5))
            .unwrap();
        assert_eq!(all, ValueSet::not_null(DataType::Int32));
        let hole = cmp(ExprType::LessThan, 5)
            .union(&cmp(ExprType::GreaterThan, 5))
            .unwrap();
        assert!(!cmp(ExprType::Equal, 5).is_subset_of(&hole));
        assert!(cmp(ExprType::Equal, 6).is_subset_of(&hole));

        assert_eq!(cmp(ExprType::Equal, 3).as_point(), Some(&int(3)));
        assert_eq!(range.as_point(), None);
    }

    #[test]
    fn test_value_set_of_decimal_ignores_scale() {
        let decimal = |s: &str| ScalarImpl::Decimal(s.parse().unwrap());
        let a = ValueSet::from_comparison(DataType::Decimal, ExprType::Equal, decimal("1.0"));
        let b = ValueSet::from_comparison(DataType::Decimal, ExprType::Equal, decimal("1.00"));
        assert!(a.unwrap().is_subset_of(&b.unwrap()));
        // The two constants are still different terms.
        assert_ne!(
            Term::constant(Some(decimal("1.0")), DataType::Decimal),
            Term::constant(Some(decimal("1.00")), DataType::Decimal)
        );
    }

    #[test]
    fn test_predicate_normal_form() {
        let (a, b, c) = (column(0, 1), column(1, 1), column(0, 2));
        // a = b AND a > 0 AND b <= 5 AND c IS NOT NULL AND a + c <> b
        let residual = compare(
            ExprType::NotEqual,
            Term::func(ExprType::Add, DataType::Int32, vec![a.clone(), c.clone()]),
            b.clone(),
        );
        let predicate = Predicate::from_conjuncts([
            compare(ExprType::Equal, a.clone(), b.clone()),
            compare(ExprType::LessThan, lit(0), a.clone()),
            compare(ExprType::LessThanOrEqual, b.clone(), lit(5)),
            Term::func(ExprType::IsNotNull, DataType::Boolean, vec![c.clone()]),
            residual.clone(),
        ])
        .unwrap();

        assert_eq!(predicate.residuals, vec![residual]);
        assert_eq!(predicate.classes.len(), 2);
        let ab = predicate
            .classes
            .iter()
            .find(|class| class.members.len() == 2)
            .unwrap();
        assert!(ab.members.contains(&a) && ab.members.contains(&b));
        let expected = cmp(ExprType::GreaterThan, 0)
            .intersect(&cmp(ExprType::LessThanOrEqual, 5))
            .unwrap();
        assert_eq!(ab.range, Some(expected));

        // The same predicate written in another order is the same normal form.
        let reordered = Predicate::from_conjuncts([
            compare(ExprType::NotEqual, b.clone(), {
                Term::func(ExprType::Add, DataType::Int32, vec![c.clone(), a.clone()])
            }),
            Term::func(ExprType::IsNotNull, DataType::Boolean, vec![c.clone()]),
            compare(ExprType::GreaterThanOrEqual, lit(5), b.clone()),
            compare(ExprType::GreaterThan, a.clone(), lit(0)),
            compare(ExprType::Equal, b, a.clone()),
        ])
        .unwrap();
        assert_eq!(predicate, reordered);

        // `(c IN (1, 2) OR (c >= 10 AND c <= 20)) AND c <> 15` is a value set of `c`.
        let or = Term::func(
            ExprType::Or,
            DataType::Boolean,
            vec![
                Term::func(
                    ExprType::In,
                    DataType::Boolean,
                    vec![c.clone(), lit(1), lit(2)],
                ),
                Term::func(
                    ExprType::And,
                    DataType::Boolean,
                    vec![
                        compare(ExprType::GreaterThanOrEqual, c.clone(), lit(10)),
                        compare(ExprType::LessThanOrEqual, c.clone(), lit(20)),
                    ],
                ),
            ],
        );
        let predicate =
            Predicate::from_conjuncts([or, compare(ExprType::NotEqual, c, lit(15))]).unwrap();
        assert!(predicate.residuals.is_empty());
        let range = predicate.classes[0].range.as_ref().unwrap();
        for (value, contained) in [(1, true), (3, false), (10, true), (15, false), (20, true)] {
            assert_eq!(
                cmp(ExprType::Equal, value).is_subset_of(range),
                contained,
                "{value}"
            );
        }

        // A predicate that can never hold has no normal form.
        assert!(
            Predicate::from_conjuncts([
                compare(ExprType::GreaterThan, a.clone(), lit(5)),
                compare(ExprType::LessThan, a, lit(5)),
            ])
            .is_none()
        );
    }
}
