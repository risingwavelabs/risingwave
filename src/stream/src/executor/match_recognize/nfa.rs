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

//! Row-pattern NFA for `MATCH_RECOGNIZE`.
//!
//! A `Pattern` (the supported v1 subset of the SQL `PATTERN` clause) is compiled to a
//! Thompson-construction NFA whose labelled transitions are pattern variables. The simulation
//! consumes a sequence of rows, where each row is summarised by the set of pattern variables whose
//! `DEFINE` predicate it satisfies, and finds the greedy longest match from a start position
//! (`ONE ROW PER MATCH` + `AFTER MATCH SKIP PAST LAST ROW`).
//!
//! Variable→predicate evaluation and the streaming/state layer live elsewhere; this module is pure
//! and deterministic so it can be unit-tested without a cluster.

// `BTreeSet` is used only by the test-only reference matchers and the unit tests; the streaming
// matcher walks transitions directly with [`Visited`] guards (a `u64` bitmask for automata of at
// most 64 states, a `HashSet` fallback beyond that).
#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::HashSet;

use async_recursion::async_recursion;

use crate::executor::error::StreamExecutorResult;

/// Decides whether the row at a physical position can be bound to a pattern variable, given the
/// variables already bound to the earlier rows of the in-progress match. This is how `DEFINE`
/// predicates are evaluated during matching: a predicate may reference the current row, its physical
/// neighbours (`PREV`/`NEXT`), and the running values of other pattern variables (e.g. `A.price`),
/// so membership cannot be precomputed independently of the match path.
pub trait CandidateMatcher {
    /// `labels[k]` is the variable bound to the match's `k`-th row; the candidate is the row at
    /// `pos = match_start + labels.len()`. The returned future is `Send` so the matcher composes
    /// with the (boxed, `Send`) executor stream.
    ///
    /// **Contract for callers:** membership MUST be queried *before* `var` is appended to `labels`,
    /// so `labels` covers only the already-bound rows and never the candidate. Consequently a matcher
    /// that resolves running navigation over `var` itself must treat `var` as the implicit trailing
    /// label. Every caller MUST follow the same rule: [the finder] and [the eviction walker] share one
    /// matcher, so a caller that pushed first would make the two disagree about which rows satisfy a
    /// variable — and eviction would then delete rows the matcher still needs.
    ///
    /// [the finder]: Nfa::find_matches_dynamic
    /// [the eviction walker]: Nfa::reaches_boundary_alive
    fn matches(
        &self,
        var: &str,
        pos: usize,
        labels: &[String],
    ) -> impl std::future::Future<Output = StreamExecutorResult<bool>> + Send;
}

/// A quantifier applied to a sub-pattern. Greedy semantics only (v1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Quantifier {
    /// `*`
    Star,
    /// `+`
    Plus,
    /// `?`
    Question,
    /// `{n}`, `{n,}`, `{n,m}`, `{,m}`. `min` defaults to 0, `max` is `None` for unbounded.
    Range { min: u32, max: Option<u32> },
}

/// The supported v1 subset of a row pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pattern {
    /// A pattern variable, e.g. `A`.
    Var(String),
    /// Concatenation, e.g. `A B C`.
    Concat(Vec<Pattern>),
    /// Alternation, e.g. `A | B`.
    Alt(Vec<Pattern>),
    /// A quantified sub-pattern, e.g. `A+`. The bool is `reluctant` (`A+?` prefers fewer matches).
    Quantified(Box<Pattern>, Quantifier, bool),
    /// `PERMUTE(a, b, ...)` — expanded to the alternation of all orderings.
    Permute(Vec<String>),
}

type StateId = usize;

#[derive(Debug, Clone)]
enum Transition {
    /// An ε-transition (consumes no row).
    Epsilon(StateId),
    /// Consume a row that satisfies pattern variable `var`, moving to `target`.
    OnVar { var: String, target: StateId },
}

/// A Thompson-construction NFA with a single start and single accept state.
#[derive(Debug, Clone)]
pub struct Nfa {
    states: Vec<Vec<Transition>>,
    start: StateId,
    accept: StateId,
    /// Per state: whether `accept` is reachable from it via a path containing at least one
    /// consuming transition, with every predicate assumed satisfiable. This is the static half of
    /// [`Nfa::may_extend`]: a state where this is `false` cannot contribute to any longer match no
    /// matter what rows arrive. Computed once at compile via two reverse BFS passes (plain
    /// accept-reachability, then ≥1-consumption reachability seeded by consuming edges into the
    /// former and propagated backward along ε-edges only — a consuming edge on the left needs no
    /// propagation, it supplies the consumption itself).
    extendable: Vec<bool>,
}

impl Nfa {
    /// Compile a [`Pattern`] into an NFA.
    pub fn compile(pattern: &Pattern) -> Self {
        let mut builder = NfaBuilder { states: Vec::new() };
        let frag = builder.build(pattern);
        let extendable = Self::compute_extendable(&builder.states, frag.accept);
        Nfa {
            states: builder.states,
            start: frag.start,
            accept: frag.accept,
            extendable,
        }
    }

    /// See the `extendable` field. Linear in states + transitions.
    fn compute_extendable(states: &[Vec<Transition>], accept: StateId) -> Vec<bool> {
        let n = states.len();
        // Reverse adjacency, split by transition kind.
        let mut rev_any: Vec<Vec<StateId>> = vec![Vec::new(); n];
        let mut rev_eps: Vec<Vec<StateId>> = vec![Vec::new(); n];
        for (s, ts) in states.iter().enumerate() {
            for t in ts {
                match t {
                    Transition::Epsilon(next) => {
                        rev_any[*next].push(s);
                        rev_eps[*next].push(s);
                    }
                    Transition::OnVar { target, .. } => rev_any[*target].push(s),
                }
            }
        }
        // reach[s]: `accept` reachable from `s` via any path (predicates assumed satisfiable).
        let mut reach = vec![false; n];
        let mut stack = vec![accept];
        reach[accept] = true;
        while let Some(s) = stack.pop() {
            for &p in &rev_any[s] {
                if !reach[p] {
                    reach[p] = true;
                    stack.push(p);
                }
            }
        }
        // extendable[s]: as `reach`, but the path must consume at least one row. Seeds are the
        // sources of consuming edges into reach-states; propagation is along reverse ε-edges only
        // (an ε into an extendable state stays extendable; a consuming edge needs only `reach` on
        // its target — it is itself the consumption).
        let mut extendable = vec![false; n];
        let mut stack: Vec<StateId> = Vec::new();
        for (s, ts) in states.iter().enumerate() {
            let seeded = ts
                .iter()
                .any(|t| matches!(t, Transition::OnVar { target, .. } if reach[*target]));
            if seeded {
                extendable[s] = true;
                stack.push(s);
            }
        }
        while let Some(s) = stack.pop() {
            for &p in &rev_eps[s] {
                if !extendable[p] {
                    extendable[p] = true;
                    stack.push(p);
                }
            }
        }
        extendable
    }

    /// The set of states reachable from `states` via ε-transitions (inclusive). Only the test-only
    /// reference matchers (e.g. [`Nfa::longest_match`]) use the explicit closure; the streaming
    /// matcher walks transitions directly, so this is gated out of the release binary.
    #[cfg(test)]
    fn epsilon_closure(&self, states: impl IntoIterator<Item = StateId>) -> BTreeSet<StateId> {
        let mut closure: BTreeSet<StateId> = BTreeSet::new();
        let mut stack: Vec<StateId> = states.into_iter().collect();
        while let Some(s) = stack.pop() {
            if !closure.insert(s) {
                continue;
            }
            for t in &self.states[s] {
                if let Transition::Epsilon(next) = t {
                    stack.push(*next);
                }
            }
        }
        closure
    }

    /// Greedy longest match starting at `rows[start]`. `rows[i]` is the set of pattern variables
    /// whose `DEFINE` predicate row `i` satisfies. Returns the exclusive end index of the longest
    /// match (so `start..end` are the matched rows), or `None` if no match starts at `start`.
    ///
    /// An empty match (the pattern accepts zero rows, e.g. `A*`) returns `Some(start)`.
    ///
    /// Test-only: the streaming executor matches via [`Nfa::find_matches_dynamic`]. This precomputed
    /// satisfied-set variant is kept as the simple reference the dynamic matcher is checked against,
    /// and to unit-test NFA construction directly. Gated out of the release binary.
    #[cfg(test)]
    pub fn longest_match(&self, rows: &[BTreeSet<String>], start: usize) -> Option<usize> {
        let mut current = self.epsilon_closure([self.start]);
        let mut longest = current.contains(&self.accept).then_some(start);

        let mut pos = start;
        while pos < rows.len() && !current.is_empty() {
            let row = &rows[pos];
            let mut next: BTreeSet<StateId> = BTreeSet::new();
            for &s in &current {
                for t in &self.states[s] {
                    if let Transition::OnVar { var, target } = t
                        && row.contains(var)
                    {
                        next.insert(*target);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            current = self.epsilon_closure(next);
            pos += 1;
            if current.contains(&self.accept) {
                longest = Some(pos);
            }
        }
        longest
    }
}

/// A single match span over the row sequence: `start..end` (end exclusive) are the matched rows.
/// Test-only: produced by the reference matcher [`Nfa::find_matches`].
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchSpan {
    pub start: usize,
    pub end: usize,
}

/// Where the scan resumes after a match (the `AFTER MATCH SKIP` strategy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipMode {
    /// `AFTER MATCH SKIP PAST LAST ROW`: resume past the match's last row (non-overlapping).
    PastLastRow,
    /// `AFTER MATCH SKIP TO NEXT ROW`: resume at the row after the match's first row (overlapping).
    ToNextRow,
    /// `AFTER MATCH SKIP TO FIRST <var>`: resume at the first row labeled `var`.
    ToFirst(String),
    /// `AFTER MATCH SKIP TO LAST <var>`: resume at the last row labeled `var`.
    ToLast(String),
}

/// Why a variable-targeted `AFTER MATCH SKIP` could not resume where the query asked, and which
/// weaker strategy the resume position fell back to. Returned by [`SkipMode::next_pos`] next to the
/// position instead of being reported here: this module stays pure (no error reporter, no executor
/// types), and the executor — which owns the actor's `EvalErrorReport` — decides what to do with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipDegradation {
    /// The target variable is bound to no row of the match, so there is no row to resume at. The
    /// scan resumed past the match's last row, i.e. as `SKIP PAST LAST ROW`.
    TargetAbsent,
    /// The target resolves to the match's own first row, so resuming there would re-find the same
    /// match forever. The scan resumed one row later, i.e. as `SKIP TO NEXT ROW`.
    TargetAtMatchStart,
}

impl SkipDegradation {
    /// The user-facing diagnostic for this degradation under `skip`: the target variable, what could
    /// not be resolved about it, and the strategy actually applied. The *clause* is not repeated here
    /// — the caller supplies it separately via [`SkipMode::clause_name`], so the rendered message
    /// names the mode once (see `report_skip_degradation_once` in `executor.rs`).
    ///
    /// Deliberately carries no row, match or partition identity: the cause is a property of the
    /// query, not of one row, which is what lets the executor report it once per watermark pass
    /// instead of once per match.
    pub fn describe(&self, skip: &SkipMode) -> String {
        // Unreachable for the variable-less modes: only the targeted arms of `next_pos` can produce a
        // degradation. A placeholder rather than an `unwrap` — a diagnostic path must not panic.
        let target = skip.target_var().unwrap_or("?");
        match self {
            SkipDegradation::TargetAbsent => format!(
                "target variable `{target}` is bound to no row of the match, so there is no row to \
                 resume at; the scan resumed past the match's last row instead (degraded to SKIP \
                 PAST LAST ROW)"
            ),
            SkipDegradation::TargetAtMatchStart => format!(
                "target variable `{target}` resolves to the match's own first row, so resuming there \
                 would re-find the same match forever; the scan resumed at the row after the match's \
                 first row instead (degraded to SKIP TO NEXT ROW)"
            ),
        }
    }
}

impl SkipMode {
    /// The `AFTER MATCH SKIP` clause as SQL spells it, *without* the target variable — the mode alone.
    /// `&'static str` so it can be the `name` of an `ExprError::InvalidParam`; the target variable is
    /// named by [`SkipDegradation::describe`] instead, so a rendered diagnostic states the mode once.
    pub fn clause_name(&self) -> &'static str {
        match self {
            SkipMode::PastLastRow => "AFTER MATCH SKIP PAST LAST ROW",
            SkipMode::ToNextRow => "AFTER MATCH SKIP TO NEXT ROW",
            SkipMode::ToFirst(_) => "AFTER MATCH SKIP TO FIRST",
            SkipMode::ToLast(_) => "AFTER MATCH SKIP TO LAST",
        }
    }

    /// The pattern variable a `SKIP TO FIRST|LAST` resumes at; `None` for the variable-less modes,
    /// which are also the only ones that can never degrade.
    pub fn target_var(&self) -> Option<&str> {
        match self {
            SkipMode::PastLastRow | SkipMode::ToNextRow => None,
            SkipMode::ToFirst(var) | SkipMode::ToLast(var) => Some(var),
        }
    }

    /// The position the scan resumes at after a match spanning `[start, end)` with per-row `labels`
    /// (`labels[i]` is the variable bound to `rows[start + i]`), plus a [`SkipDegradation`] when that
    /// position is not the one the query asked for. Always returns `> start` so the scan makes
    /// progress.
    ///
    /// Two cases have no valid resume row, and both are data-dependent — the same query degrades or
    /// not depending on which rows arrive:
    ///
    ///  * the target variable is bound to no row of this match (`(a b?)` matching only `a`, with
    ///    `SKIP TO LAST b`), so the resume position falls back to the match end — silently becoming
    ///    `SKIP PAST LAST ROW`;
    ///  * the target resolves to the match's own first row (`SKIP TO FIRST` of the pattern's leading
    ///    variable), which would re-find the same match forever, so it is clamped to `start + 1` —
    ///    silently becoming `SKIP TO NEXT ROW`.
    ///
    /// The SQL standard prescribes a runtime error for both (Oracle raises ORA-62511 / ORA-62512;
    /// Flink likewise). This implementation deliberately keeps the degradation and **reports** it
    /// instead of raising it: an error here would abort the actor over a data-dependent condition,
    /// and since the materialized view is already committed, every recovery attempt would replay the
    /// same rows and die again — a recoverable query turned into a crash loop. No RisingWave
    /// streaming operator fails an actor for a data-dependent condition; every hard error in this
    /// operator is a contract or plan violation (non-append-only input, an unknown slot kind), which
    /// recovery cannot fix either way. So the degradation is made *visible* rather than fatal: the
    /// executor routes the returned diagnostic to the actor's `EvalErrorReport`, the same surface
    /// expression evaluation errors already use (the `stream_expr_error` log and the
    /// `user_compute_error` metric). See `report_skip_degradation_once` in `executor.rs` for how the
    /// message is rendered — including the surface's fixed log prefix — and for the volume policy.
    pub fn next_pos(
        &self,
        start: usize,
        end: usize,
        labels: &[String],
    ) -> (usize, Option<SkipDegradation>) {
        // Resolve a variable-targeted skip: `found` is the target's index within `labels`. Only these
        // modes can degrade; the variable-less ones always have a valid resume row.
        let resolve = |found: Option<usize>| match found {
            // Index 0 is the match's own first row, so resuming there makes no progress.
            Some(0) => (start + 1, Some(SkipDegradation::TargetAtMatchStart)),
            Some(j) => (start + j, None),
            None => (end.max(start + 1), Some(SkipDegradation::TargetAbsent)),
        };
        match self {
            SkipMode::PastLastRow => (end.max(start + 1), None),
            SkipMode::ToNextRow => (start + 1, None),
            SkipMode::ToFirst(var) => resolve(labels.iter().position(|l| l == var)),
            SkipMode::ToLast(var) => resolve(labels.iter().rposition(|l| l == var)),
        }
    }
}

impl Nfa {
    /// Find all matches over `rows` under `ONE ROW PER MATCH` with the given `AFTER MATCH SKIP`
    /// strategy: scan left to right; at each position take the greedy longest match; on a non-empty
    /// match, record it and resume per `skip`; otherwise advance by one row.
    ///
    /// Empty matches (a pattern that accepts zero rows, e.g. `A*` on a non-matching row) are not
    /// emitted and advance the scan by one, so the scan always terminates.
    ///
    /// Test-only reference matcher (see [`Nfa::longest_match`]); gated out of the release binary.
    #[cfg(test)]
    pub fn find_matches(&self, rows: &[BTreeSet<String>], skip: &SkipMode) -> Vec<MatchSpan> {
        let mut matches = Vec::new();
        let mut i = 0;
        while i < rows.len() {
            if let Some(end) = self.longest_match(rows, i)
                && end > i
            {
                matches.push(MatchSpan { start: i, end });
                // `find_matches` is label-less; the variable-targeted skips resolve like
                // `PAST LAST ROW` here. `find_matches_labeled` applies them precisely.
                i = match skip {
                    SkipMode::ToNextRow => i + 1,
                    _ => end,
                };
            } else {
                i += 1;
            }
        }
        matches
    }
}

/// A match span together with the pattern variable assigned to each matched row.
/// `labels[i]` is the variable that `rows[start + i]` was matched as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabeledMatch {
    pub start: usize,
    pub end: usize,
    pub labels: Vec<String>,
}

/// Visited-state guard for one traversal position of the dynamic matcher. ε-transitions keep the
/// position, so each consumed row starts a fresh set (see `preferred_from_dynamic`); these sets are
/// created O(rows × branches) times per partition visit, so their allocation cost matters. The
/// common case — an automaton with at most 64 states — is a single `u64` bitmask (no allocation,
/// membership is a bit test); larger automata (deep `PERMUTE` expansions) fall back to a `HashSet`.
enum Visited {
    Small(u64),
    Large(HashSet<StateId>),
}

impl Visited {
    fn new(n_states: usize) -> Self {
        if n_states <= 64 {
            Visited::Small(0)
        } else {
            Visited::Large(HashSet::new())
        }
    }

    /// Marks `s` visited; returns whether it was newly inserted (mirrors `HashSet::insert`).
    fn insert(&mut self, s: StateId) -> bool {
        match self {
            Visited::Small(bits) => {
                let mask = 1u64 << s;
                let newly = *bits & mask == 0;
                *bits |= mask;
                newly
            }
            Visited::Large(set) => set.insert(s),
        }
    }

    fn remove(&mut self, s: StateId) {
        match self {
            Visited::Small(bits) => *bits &= !(1u64 << s),
            Visited::Large(set) => {
                set.remove(&s);
            }
        }
    }
}

impl Nfa {
    /// Greedy longest match starting at `rows[start]`, returning the per-row variable assignment
    /// along the chosen accepting path (the variable each consumed row was matched as). This is
    /// what `MEASURES` navigation (`FIRST`/`LAST`), `CLASSIFIER()`, and aggregates over matched
    /// rows consume. Returns `(end, labels)` where `labels.len() == end - start`, or `None`.
    #[cfg(test)]
    pub fn longest_match_labeled(
        &self,
        rows: &[BTreeSet<String>],
        start: usize,
    ) -> Option<(usize, Vec<String>)> {
        let mut visited: HashSet<(StateId, usize)> = HashSet::new();
        self.longest_from(rows, self.start, start, &mut visited)
    }

    /// Recursive longest-accepting-path search. `visited` guards against ε-cycles on the current
    /// path (it tracks `(state, pos)` and is unwound on backtrack). Among continuations the one
    /// reaching the furthest `end` wins; ties keep the first in transition order, making the label
    /// assignment deterministic.
    #[cfg(test)]
    fn longest_from(
        &self,
        rows: &[BTreeSet<String>],
        state: StateId,
        pos: usize,
        visited: &mut HashSet<(StateId, usize)>,
    ) -> Option<(usize, Vec<String>)> {
        if !visited.insert((state, pos)) {
            return None;
        }
        let mut best: Option<(usize, Vec<String>)> =
            (state == self.accept).then(|| (pos, Vec::new()));
        for t in &self.states[state] {
            let candidate = match t {
                Transition::Epsilon(next) => self.longest_from(rows, *next, pos, visited),
                Transition::OnVar { var, target } => {
                    if pos < rows.len() && rows[pos].contains(var) {
                        self.longest_from(rows, *target, pos + 1, visited).map(
                            |(end, mut labels)| {
                                labels.insert(0, var.clone());
                                (end, labels)
                            },
                        )
                    } else {
                        None
                    }
                }
            };
            if let Some((end, labels)) = candidate
                && best.as_ref().is_none_or(|(b, _)| end > *b)
            {
                best = Some((end, labels));
            }
        }
        visited.remove(&(state, pos));
        best
    }

    /// Like [`Nfa::find_matches`] but returns each match with its per-row variable labels.
    /// Test-only reference matcher; the streaming executor uses [`Nfa::find_matches_dynamic`].
    #[cfg(test)]
    pub fn find_matches_labeled(
        &self,
        rows: &[BTreeSet<String>],
        skip: &SkipMode,
    ) -> Vec<LabeledMatch> {
        let mut matches = Vec::new();
        let mut i = 0;
        while i < rows.len() {
            if let Some((end, labels)) = self.longest_match_labeled(rows, i)
                && end > i
            {
                let start = i;
                // Test-only matcher: the diagnostic is dropped (there is no actor to report to).
                (i, _) = skip.next_pos(start, end, &labels);
                matches.push(LabeledMatch { start, end, labels });
            } else {
                i += 1;
            }
        }
        matches
    }

    /// Like `find_matches_labeled`, but membership is decided by an async [`CandidateMatcher`]
    /// instead of precomputed satisfied-sets, so `DEFINE` predicates with row-pattern navigation can
    /// be evaluated against the running match. `n_rows` is the number of (sorted) rows to scan. This
    /// is the only matcher the streaming executor uses.
    pub async fn find_matches_dynamic(
        &self,
        n_rows: usize,
        matcher: &(impl CandidateMatcher + Sync),
        skip: &SkipMode,
    ) -> StreamExecutorResult<Vec<LabeledMatch>> {
        let mut matches = Vec::new();
        let mut i = 0;
        while i < n_rows {
            let mut path: Vec<String> = Vec::new();
            let mut visited = Visited::new(self.states.len());
            let found = self
                .preferred_from_dynamic(n_rows, self.start, i, &mut path, matcher, &mut visited)
                .await?;
            if let Some((end, labels)) = found
                && end > i
            {
                let start = i;
                // The diagnostic is dropped here on purpose: the executor recomputes the resume
                // position for the matches it actually *emits* and reports from there, so a match
                // that this scan finds but the emit path holds back or skips is not reported twice
                // (nor reported at all until it is emitted).
                (i, _) = skip.next_pos(start, end, &labels);
                matches.push(LabeledMatch { start, end, labels });
            } else {
                i += 1;
            }
        }
        Ok(matches)
    }

    /// Whether a match starting at `pos` is still *live* at the safe boundary `n_rows`: there exists
    /// a path that consumes the safe rows `pos..n_rows` and reaches the boundary while still inside
    /// the automaton (not yet accepted), so a future row could extend it into a complete match. Used
    /// to evict rows that can no longer be part of any match.
    ///
    /// This is strictly stronger than "can `pos` begin the pattern": for `(a b)` over `[a, x]` where
    /// `x` matches neither, the `a` *can* begin the pattern, but every path dies on `x` before the
    /// boundary, so the start is dead and must be evictable. A lone `[a]` (boundary right after `a`),
    /// in contrast, is kept because a future `b` may still complete it.
    pub async fn reaches_boundary_alive(
        &self,
        pos: usize,
        n_rows: usize,
        matcher: &(impl CandidateMatcher + Sync),
    ) -> StreamExecutorResult<bool> {
        let mut path: Vec<String> = Vec::new();
        let mut visited = Visited::new(self.states.len());
        self.live_to_boundary(n_rows, self.start, pos, &mut path, matcher, &mut visited)
            .await
    }

    /// Whether the match `[start, end)` could become *longer* given rows that are not yet decided:
    /// there exists a path from `start` that consumes the rows `start..end` (predicate-driven, any
    /// labeling) and lands at `end` in a state from which the automaton could still consume more
    /// and re-accept ([`Nfa::extendable`], predicates assumed satisfiable for the unknown rows).
    ///
    /// `false` means the accepting path is **terminal**: no future or undecided row can produce a
    /// longer match from this start, so a boundary match is final and must be emitted — holding it
    /// would starve an idle partition forever (the frontier recompute finds neither a future row
    /// nor, without `WITHIN`, a deadline, and drops the partition). `true` means the standard
    /// greedy-maximality wait applies.
    pub async fn may_extend(
        &self,
        start: usize,
        end: usize,
        matcher: &(impl CandidateMatcher + Sync),
    ) -> StreamExecutorResult<bool> {
        let mut path: Vec<String> = Vec::new();
        let mut visited = Visited::new(self.states.len());
        self.extendable_past(end, self.start, start, &mut path, matcher, &mut visited)
            .await
    }

    /// Path-carrying DFS backing [`Nfa::may_extend`]. Mirrors [`Nfa::live_to_boundary`]'s traversal
    /// exactly, with one different base case: reaching `pos == end` answers the *static* question
    /// ([`Nfa::extendable`]) instead of an unconditional "alive" — the rows past `end` are unknown,
    /// so any transition out of the landing state is assumed satisfiable. Reaching `accept` before
    /// `end` is a shorter match, not an extension of this one, and `accept` has no outgoing
    /// transitions, so that path contributes nothing.
    #[async_recursion]
    async fn extendable_past(
        &self,
        end: usize,
        state: StateId,
        pos: usize,
        path: &mut Vec<String>,
        matcher: &(impl CandidateMatcher + Sync),
        visited: &mut Visited,
    ) -> StreamExecutorResult<bool> {
        if pos == end {
            return Ok(self.extendable[state]);
        }
        if state == self.accept {
            return Ok(false);
        }
        if !visited.insert(state) {
            return Ok(false);
        }
        for t in &self.states[state] {
            let extendable = match t {
                Transition::Epsilon(next) => {
                    self.extendable_past(end, *next, pos, path, matcher, visited)
                        .await?
                }
                Transition::OnVar { var, target } => {
                    if matcher.matches(var, pos, path).await? {
                        path.push(var.clone());
                        let mut next_visited = Visited::new(self.states.len());
                        let r = self
                            .extendable_past(end, *target, pos + 1, path, matcher, &mut next_visited)
                            .await?;
                        path.pop();
                        r
                    } else {
                        false
                    }
                }
            };
            if extendable {
                visited.remove(state);
                return Ok(true);
            }
        }
        visited.remove(state);
        Ok(false)
    }

    /// Path-carrying DFS backing [`Nfa::reaches_boundary_alive`]. Mirrors the traversal of
    /// [`Nfa::preferred_from_dynamic`] (so `DEFINE` predicates see the running match), but instead of
    /// looking for an accepting path it asks whether the safe suffix `pos..n_rows` can be consumed
    /// without dying. Reaching `pos == n_rows` inside the automaton is "alive"; reaching `accept`
    /// before the boundary is a complete (already-finalized) match, not a live partial one, so it
    /// does not by itself keep the start alive.
    #[async_recursion]
    async fn live_to_boundary(
        &self,
        n_rows: usize,
        state: StateId,
        pos: usize,
        path: &mut Vec<String>,
        matcher: &(impl CandidateMatcher + Sync),
        visited: &mut Visited,
    ) -> StreamExecutorResult<bool> {
        if pos == n_rows {
            return Ok(true);
        }
        if state == self.accept {
            return Ok(false);
        }
        if !visited.insert(state) {
            return Ok(false);
        }
        for t in &self.states[state] {
            let alive = match t {
                Transition::Epsilon(next) => {
                    self.live_to_boundary(n_rows, *next, pos, path, matcher, visited)
                        .await?
                }
                Transition::OnVar { var, target } => {
                    if pos < n_rows && matcher.matches(var, pos, path).await? {
                        path.push(var.clone());
                        let mut next_visited = Visited::new(self.states.len());
                        let r = self
                            .live_to_boundary(
                                n_rows,
                                *target,
                                pos + 1,
                                path,
                                matcher,
                                &mut next_visited,
                            )
                            .await?;
                        path.pop();
                        r
                    } else {
                        false
                    }
                }
            };
            if alive {
                visited.remove(state);
                return Ok(true);
            }
        }
        visited.remove(state);
        Ok(false)
    }

    /// Async, path-carrying counterpart of `longest_from`. `path` is the variables bound to
    /// the match's rows so far (threaded *down* so the matcher can see the running match); the
    /// returned `labels` is the full assignment of the chosen accepting path. `visited` guards
    /// against ε-cycles *at the current position* — ε-transitions keep `pos`/`path`, so a fresh set
    /// is used once a row is consumed (which lets distinct variable assignments reach the same state
    /// at the next position).
    /// Returns the *first* accepting path in transition order, not the longest. Transitions are
    /// emitted by the builder in preference order — for a greedy quantifier the consume/loop edge
    /// precedes the exit edge (so the first accepting path is the longest match), and for a reluctant
    /// quantifier the exit edge precedes it (so the first accepting path is the shortest). This also
    /// gives alternation its standard ordered semantics (the first listed alternative that matches).
    #[async_recursion]
    async fn preferred_from_dynamic(
        &self,
        n_rows: usize,
        state: StateId,
        pos: usize,
        path: &mut Vec<String>,
        matcher: &(impl CandidateMatcher + Sync),
        visited: &mut Visited,
    ) -> StreamExecutorResult<Option<(usize, Vec<String>)>> {
        // The single accept state is terminal: reaching it completes the match here.
        if state == self.accept {
            return Ok(Some((pos, path.clone())));
        }
        if !visited.insert(state) {
            return Ok(None);
        }
        for t in &self.states[state] {
            let candidate = match t {
                Transition::Epsilon(next) => {
                    self.preferred_from_dynamic(n_rows, *next, pos, path, matcher, visited)
                        .await?
                }
                Transition::OnVar { var, target } => {
                    if pos < n_rows && matcher.matches(var, pos, path).await? {
                        path.push(var.clone());
                        let mut next_visited = Visited::new(self.states.len());
                        let r = self
                            .preferred_from_dynamic(
                                n_rows,
                                *target,
                                pos + 1,
                                path,
                                matcher,
                                &mut next_visited,
                            )
                            .await?;
                        path.pop();
                        r
                    } else {
                        None
                    }
                }
            };
            if candidate.is_some() {
                visited.remove(state);
                return Ok(candidate);
            }
        }
        visited.remove(state);
        Ok(None)
    }
}

/// A sub-NFA fragment with one entry and one exit state.
struct Fragment {
    start: StateId,
    accept: StateId,
}

struct NfaBuilder {
    states: Vec<Vec<Transition>>,
}

impl NfaBuilder {
    fn new_state(&mut self) -> StateId {
        self.states.push(Vec::new());
        self.states.len() - 1
    }

    fn add_epsilon(&mut self, from: StateId, to: StateId) {
        self.states[from].push(Transition::Epsilon(to));
    }

    fn add_on_var(&mut self, from: StateId, var: String, to: StateId) {
        self.states[from].push(Transition::OnVar { var, target: to });
    }

    /// LOCKSTEP: the per-construct state counts below are mirrored by `estimate_nfa_states` in
    /// `frontend/src/binder/relation/match_recognize.rs`, which rejects a pattern whose expansion
    /// would exhaust memory here. No test can span the two crates, so changing how many states a
    /// construct allocates requires updating that estimator in the same change.
    fn build(&mut self, pattern: &Pattern) -> Fragment {
        match pattern {
            Pattern::Var(v) => {
                let start = self.new_state();
                let accept = self.new_state();
                self.add_on_var(start, v.clone(), accept);
                Fragment { start, accept }
            }
            Pattern::Concat(parts) => {
                if parts.is_empty() {
                    let s = self.new_state();
                    return Fragment {
                        start: s,
                        accept: s,
                    };
                }
                let first = self.build(&parts[0]);
                let mut accept = first.accept;
                for p in &parts[1..] {
                    let frag = self.build(p);
                    self.add_epsilon(accept, frag.start);
                    accept = frag.accept;
                }
                Fragment {
                    start: first.start,
                    accept,
                }
            }
            Pattern::Alt(alts) => {
                let start = self.new_state();
                let accept = self.new_state();
                for a in alts {
                    let frag = self.build(a);
                    self.add_epsilon(start, frag.start);
                    self.add_epsilon(frag.accept, accept);
                }
                Fragment { start, accept }
            }
            Pattern::Quantified(inner, q, reluctant) => self.build_quantified(inner, q, *reluctant),
            Pattern::Permute(vars) => {
                // PERMUTE expands to the alternation of every ordering of the variables.
                let alts: Vec<Pattern> = permutations(vars)
                    .into_iter()
                    .map(|order| Pattern::Concat(order.into_iter().map(Pattern::Var).collect()))
                    .collect();
                self.build(&Pattern::Alt(alts))
            }
        }
    }

    /// LOCKSTEP with `estimate_nfa_states` — see [`Self::build`].
    fn build_quantified(&mut self, inner: &Pattern, q: &Quantifier, reluctant: bool) -> Fragment {
        match q {
            Quantifier::Star => self.build_star(inner, reluctant),
            Quantifier::Plus => {
                // inner followed by inner* (the repetition carries the reluctant preference)
                let first = self.build(inner);
                let star = self.build_star(inner, reluctant);
                self.add_epsilon(first.accept, star.start);
                Fragment {
                    start: first.start,
                    accept: star.accept,
                }
            }
            Quantifier::Question => {
                let start = self.new_state();
                let accept = self.new_state();
                let frag = self.build(inner);
                // Greedy orders take-the-inner before skip; reluctant orders skip first.
                if reluctant {
                    self.add_epsilon(start, accept); // skip first
                    self.add_epsilon(start, frag.start);
                } else {
                    self.add_epsilon(start, frag.start);
                    self.add_epsilon(start, accept); // skip
                }
                self.add_epsilon(frag.accept, accept);
                Fragment { start, accept }
            }
            Quantifier::Range { min, max } => self.build_range(inner, *min, *max, reluctant),
        }
    }

    /// LOCKSTEP with `estimate_nfa_states` — see [`Self::build`].
    fn build_star(&mut self, inner: &Pattern, reluctant: bool) -> Fragment {
        let start = self.new_state();
        let accept = self.new_state();
        let frag = self.build(inner);
        // The matcher takes the first accepting path in edge order. Greedy emits the consume/loop
        // edge before the exit edge (longest match first); reluctant emits the exit edge first
        // (shortest match first).
        if reluctant {
            self.add_epsilon(start, accept); // zero occurrences first
            self.add_epsilon(start, frag.start);
            self.add_epsilon(frag.accept, accept); // exit before loop
            self.add_epsilon(frag.accept, frag.start);
        } else {
            self.add_epsilon(start, frag.start);
            self.add_epsilon(start, accept); // zero occurrences
            self.add_epsilon(frag.accept, frag.start); // loop
            self.add_epsilon(frag.accept, accept);
        }
        Fragment { start, accept }
    }

    /// LOCKSTEP with `estimate_nfa_states` — see [`Self::build`]. The bounds reaching here are already
    /// capped at bind time precisely because this expansion is eager.
    fn build_range(
        &mut self,
        inner: &Pattern,
        min: u32,
        max: Option<u32>,
        reluctant: bool,
    ) -> Fragment {
        // Expand to `min` mandatory copies followed by either `*` (unbounded) or `max-min`
        // optional copies.
        let mut parts: Vec<Pattern> = Vec::new();
        for _ in 0..min {
            parts.push(inner.clone());
        }
        match max {
            None => parts.push(Pattern::Quantified(
                Box::new(inner.clone()),
                Quantifier::Star,
                reluctant,
            )),
            Some(max) => {
                for _ in min..max {
                    parts.push(Pattern::Quantified(
                        Box::new(inner.clone()),
                        Quantifier::Question,
                        reluctant,
                    ));
                }
            }
        }
        self.build(&Pattern::Concat(parts))
    }
}

/// All orderings of `items`. Only used for `PERMUTE`, which has a small arity in practice.
fn permutations(items: &[String]) -> Vec<Vec<String>> {
    if items.is_empty() {
        return vec![vec![]];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, head.clone());
            out.push(tail);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(s: &str) -> Pattern {
        Pattern::Var(s.to_owned())
    }

    /// Build a row sequence from a string where each char names the single variable that row
    /// satisfies, e.g. "abc" -> [{a}, {b}, {c}].
    fn rows(seq: &str) -> Vec<BTreeSet<String>> {
        seq.chars()
            .map(|c| BTreeSet::from([c.to_string()]))
            .collect()
    }

    #[test]
    fn concat_exact() {
        // A B C
        let p = Pattern::Concat(vec![vars("a"), vars("b"), vars("c")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("abc"), 0), Some(3));
        assert_eq!(nfa.longest_match(&rows("abx"), 0), None);
        assert_eq!(nfa.longest_match(&rows("ab"), 0), None);
    }

    #[test]
    fn plus_is_greedy() {
        // A B+ C  on  a b b b c
        let p = Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, false),
            vars("c"),
        ]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("abbbc"), 0), Some(5));
        // B+ requires at least one b.
        assert_eq!(nfa.longest_match(&rows("ac"), 0), None);
    }

    #[test]
    fn question_optional() {
        // A B? C  matches both "abc" and "ac"
        let p = Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Question, false),
            vars("c"),
        ]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("abc"), 0), Some(3));
        assert_eq!(nfa.longest_match(&rows("ac"), 0), Some(2));
    }

    #[test]
    fn star_greedy_longest() {
        // A*  on  a a a  -> greedy longest is 3
        let p = Pattern::Quantified(Box::new(vars("a")), Quantifier::Star, false);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("aaa"), 0), Some(3));
        // zero occurrences still matches (empty match).
        assert_eq!(nfa.longest_match(&rows("xyz"), 0), Some(0));
    }

    #[test]
    fn alternation() {
        // (A | B) C
        let p = Pattern::Concat(vec![Pattern::Alt(vec![vars("a"), vars("b")]), vars("c")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("ac"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("bc"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("cc"), 0), None);
    }

    #[test]
    fn range_bounds() {
        // A{2,3}
        let p = Pattern::Quantified(
            Box::new(vars("a")),
            Quantifier::Range {
                min: 2,
                max: Some(3),
            },
            false,
        );
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("a"), 0), None); // need >= 2
        assert_eq!(nfa.longest_match(&rows("aa"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("aaa"), 0), Some(3));
        assert_eq!(nfa.longest_match(&rows("aaaa"), 0), Some(3)); // capped at 3
    }

    #[test]
    fn permute_any_order() {
        // PERMUTE(a, b)
        let p = Pattern::Permute(vec!["a".to_owned(), "b".to_owned()]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("ab"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("ba"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("aa"), 0), None);
    }

    #[test]
    fn match_from_offset() {
        // A B starting at index 1 of  x a b
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("xab"), 1), Some(3));
        assert_eq!(nfa.longest_match(&rows("xab"), 0), None);
    }

    fn spans(v: &[(usize, usize)]) -> Vec<MatchSpan> {
        v.iter()
            .map(|&(start, end)| MatchSpan { start, end })
            .collect()
    }

    #[test]
    fn find_matches_skip_past_last_row() {
        // A B, repeated, with SKIP PAST LAST ROW -> non-overlapping matches.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.find_matches(&rows("ababab"), &SkipMode::PastLastRow),
            spans(&[(0, 2), (2, 4), (4, 6)])
        );
    }

    #[test]
    fn find_matches_skip_to_next_row_overlaps() {
        // A+ with SKIP TO NEXT ROW: matches may overlap (resume at start+1).
        let p = Pattern::Quantified(Box::new(vars("a")), Quantifier::Plus, false);
        let nfa = Nfa::compile(&p);
        // "aaa": greedy A+ at 0->(0,3); to-next resumes at 1->(1,3); 2->(2,3).
        assert_eq!(
            nfa.find_matches(&rows("aaa"), &SkipMode::ToNextRow),
            spans(&[(0, 3), (1, 3), (2, 3)])
        );
        // PAST LAST ROW on the same input: single match.
        assert_eq!(
            nfa.find_matches(&rows("aaa"), &SkipMode::PastLastRow),
            spans(&[(0, 3)])
        );
    }

    #[test]
    fn find_matches_greedy_then_resume() {
        // A B+ : greedy consumes all b's, then resumes past the match.
        let p = Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&p);
        // a b b | a b  -> (0,3) then (3,5)
        assert_eq!(
            nfa.find_matches(&rows("abbab"), &SkipMode::PastLastRow),
            spans(&[(0, 3), (3, 5)])
        );
    }

    #[test]
    fn find_matches_skips_non_matching_rows() {
        // A B with junk rows between matches.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        // x a b x x a b -> (1,3),(5,7)
        assert_eq!(
            nfa.find_matches(&rows("xabxxab"), &SkipMode::PastLastRow),
            spans(&[(1, 3), (5, 7)])
        );
    }

    #[test]
    fn find_matches_empty_pattern_terminates() {
        // A* matches empty everywhere; empty matches are not emitted and the scan terminates.
        let p = Pattern::Quantified(Box::new(vars("a")), Quantifier::Star, false);
        let nfa = Nfa::compile(&p);
        // "aa b aa" -> greedy A* consumes runs of a, emits non-empty ones.
        assert_eq!(
            nfa.find_matches(&rows("aabaa"), &SkipMode::PastLastRow),
            spans(&[(0, 2), (3, 5)])
        );
        // all-non-matching -> no matches, terminates.
        assert_eq!(
            nfa.find_matches(&rows("xxx"), &SkipMode::PastLastRow),
            spans(&[])
        );
    }

    fn lbl(s: &str) -> Vec<String> {
        s.chars().map(|c| c.to_string()).collect()
    }

    #[test]
    fn labeled_concat() {
        // A B -> rows labelled a, b.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.longest_match_labeled(&rows("ab"), 0),
            Some((2, lbl("ab")))
        );
    }

    #[test]
    fn labeled_plus_greedy() {
        // A B+ on a b b -> labels a, b, b (greedy consumes both b's).
        let p = Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.longest_match_labeled(&rows("abb"), 0),
            Some((3, lbl("abb")))
        );
    }

    #[test]
    fn labeled_alternation() {
        // (A | B) C on b c -> labels b, c.
        let p = Pattern::Concat(vec![Pattern::Alt(vec![vars("a"), vars("b")]), vars("c")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.longest_match_labeled(&rows("bc"), 0),
            Some((2, lbl("bc")))
        );
    }

    #[test]
    fn labeled_permute() {
        // PERMUTE(a, b) on b a -> labels b, a.
        let p = Pattern::Permute(vec!["a".to_owned(), "b".to_owned()]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.longest_match_labeled(&rows("ba"), 0),
            Some((2, lbl("ba")))
        );
    }

    #[test]
    fn find_matches_labeled_carries_labels() {
        // A B repeated -> two labelled matches.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.find_matches_labeled(&rows("abab"), &SkipMode::PastLastRow),
            vec![
                LabeledMatch {
                    start: 0,
                    end: 2,
                    labels: lbl("ab")
                },
                LabeledMatch {
                    start: 2,
                    end: 4,
                    labels: lbl("ab")
                },
            ]
        );
    }

    #[test]
    fn skip_to_first_last_var() {
        // Pattern (a b b) over five rows that each satisfy both `a` and `b`, so matches can overlap.
        // The skip strategy decides where each next match starts:
        //   PAST LAST ROW -> one match [0,3)
        //   SKIP TO LAST b -> [0,3), [2,5)      (resume at the match's last `b`)
        //   SKIP TO FIRST b -> [0,3), [1,4), [2,5)  (resume at the match's first `b`)
        let p = Pattern::Concat(vec![vars("a"), vars("b"), vars("b")]);
        let nfa = Nfa::compile(&p);
        let rows = vec![BTreeSet::from(["a".to_owned(), "b".to_owned()]); 5];

        let starts = |skip: &SkipMode| {
            nfa.find_matches_labeled(&rows, skip)
                .into_iter()
                .map(|m| m.start)
                .collect::<Vec<_>>()
        };
        assert_eq!(starts(&SkipMode::PastLastRow), vec![0]);
        assert_eq!(starts(&SkipMode::ToLast("b".to_owned())), vec![0, 2]);
        assert_eq!(starts(&SkipMode::ToFirst("b".to_owned())), vec![0, 1, 2]);
    }

    /// The two data-dependent cases where a variable-targeted skip has no valid resume row degrade
    /// (deliberately, instead of raising the runtime error the SQL standard prescribes — see
    /// [`SkipMode::next_pos`]). The degradation must be *visible*, so `next_pos` returns a
    /// diagnostic alongside the position, and the position itself must be unchanged: this is a
    /// visibility fix, not a behavior change.
    #[test]
    fn skip_target_degradations_are_reported() {
        // A match over rows 3..5, labelled `a` then `b`.
        let labels = lbl("ab");

        // (a) The target is bound to no row of this match: nothing to resume at, so the scan resumes
        //     past the match's last row — silently becoming SKIP PAST LAST ROW.
        assert_eq!(
            SkipMode::ToLast("c".to_owned()).next_pos(3, 5, &labels),
            (5, Some(SkipDegradation::TargetAbsent))
        );
        assert_eq!(
            SkipMode::ToFirst("c".to_owned()).next_pos(3, 5, &labels),
            (5, Some(SkipDegradation::TargetAbsent))
        );

        // (b) The target resolves to the match's own first row: resuming there would re-find the
        //     same match forever, so it is clamped one row on — silently becoming SKIP TO NEXT ROW.
        assert_eq!(
            SkipMode::ToFirst("a".to_owned()).next_pos(3, 5, &labels),
            (4, Some(SkipDegradation::TargetAtMatchStart))
        );
        assert_eq!(
            SkipMode::ToLast("a".to_owned()).next_pos(3, 5, &labels),
            (4, Some(SkipDegradation::TargetAtMatchStart))
        );

        // A resolvable target reports nothing — note it can land on the same position as the clamped
        // case above, so the diagnostic (not the position) is what distinguishes them.
        assert_eq!(
            SkipMode::ToLast("b".to_owned()).next_pos(3, 5, &labels),
            (4, None)
        );
        // The variable-less modes cannot degrade.
        assert_eq!(SkipMode::PastLastRow.next_pos(3, 5, &labels), (5, None));
        assert_eq!(SkipMode::ToNextRow.next_pos(3, 5, &labels), (4, None));
    }

    /// The diagnostic names the target variable and the strategy the resume position degraded to. The
    /// skip mode itself is named by the caller (`clause_name`), so it must NOT be repeated here — the
    /// rendered message would otherwise say `SKIP` twice.
    #[test]
    fn skip_degradation_describes_target_and_fallback() {
        let absent = SkipDegradation::TargetAbsent.describe(&SkipMode::ToLast("c".to_owned()));
        assert!(absent.contains("target variable `c`"), "{absent}");
        assert!(absent.contains("SKIP PAST LAST ROW"), "{absent}");
        assert!(!absent.contains("SKIP TO LAST"), "{absent}");

        let at_start =
            SkipDegradation::TargetAtMatchStart.describe(&SkipMode::ToFirst("a".to_owned()));
        assert!(at_start.contains("target variable `a`"), "{at_start}");
        assert!(at_start.contains("SKIP TO NEXT ROW"), "{at_start}");
        assert!(!at_start.contains("SKIP TO FIRST"), "{at_start}");
    }

    /// The clause name feeds the `ExprError` parameter name, so it must state the mode without the
    /// target variable (which `describe` names) — one mention of the mode per rendered message.
    #[test]
    fn skip_clause_name_and_target_var() {
        assert_eq!(
            SkipMode::ToLast("c".to_owned()).clause_name(),
            "AFTER MATCH SKIP TO LAST"
        );
        assert_eq!(
            SkipMode::ToFirst("a".to_owned()).clause_name(),
            "AFTER MATCH SKIP TO FIRST"
        );
        assert_eq!(
            SkipMode::PastLastRow.clause_name(),
            "AFTER MATCH SKIP PAST LAST ROW"
        );
        assert_eq!(
            SkipMode::ToNextRow.clause_name(),
            "AFTER MATCH SKIP TO NEXT ROW"
        );

        assert_eq!(SkipMode::ToLast("c".to_owned()).target_var(), Some("c"));
        assert_eq!(SkipMode::ToFirst("a".to_owned()).target_var(), Some("a"));
        // The variable-less modes have no target — and can never degrade.
        assert_eq!(SkipMode::PastLastRow.target_var(), None);
        assert_eq!(SkipMode::ToNextRow.target_var(), None);
    }

    #[test]
    fn row_satisfying_multiple_vars() {
        // Overlapping DEFINEs: a row can satisfy several variables.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        let rows = vec![
            BTreeSet::from(["a".to_owned(), "b".to_owned()]),
            BTreeSet::from(["b".to_owned()]),
        ];
        assert_eq!(nfa.longest_match(&rows, 0), Some(2));
    }

    /// A [`CandidateMatcher`] backed by precomputed satisfied-sets — the dynamic driver should then
    /// agree with the static [`Nfa::find_matches_labeled`].
    struct SetMatcher {
        rows: Vec<BTreeSet<String>>,
    }
    impl CandidateMatcher for SetMatcher {
        async fn matches(
            &self,
            var: &str,
            pos: usize,
            _labels: &[String],
        ) -> StreamExecutorResult<bool> {
            Ok(self.rows[pos].contains(var))
        }
    }

    #[tokio::test]
    async fn dynamic_matches_static_for_set_predicate() {
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        let r = rows("abab");
        let m = SetMatcher { rows: r.clone() };
        let dynamic = nfa
            .find_matches_dynamic(r.len(), &m, &SkipMode::PastLastRow)
            .await
            .unwrap();
        assert_eq!(
            dynamic,
            nfa.find_matches_labeled(&r, &SkipMode::PastLastRow)
        );
    }

    /// [`Nfa::may_extend`]: a boundary match on a terminal accepting path is final (emit now); one
    /// whose automaton can still consume — an open quantifier, a pending longer alternation branch,
    /// an unexhausted range, an optional tail — must be held.
    #[tokio::test]
    async fn may_extend_distinguishes_terminal_from_open_paths() {
        let m = |s: &str| SetMatcher { rows: rows(s) };

        // Fixed (a b): after consuming both, nothing can extend — terminal.
        let fixed = Nfa::compile(&Pattern::Concat(vec![vars("a"), vars("b")]));
        assert!(!fixed.may_extend(0, 2, &m("ab")).await.unwrap());

        // (a b+): a future `b` extends the greedy match — held.
        let open = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, false),
        ]));
        assert!(open.may_extend(0, 2, &m("ab")).await.unwrap());

        // (a (b | b c)): (a, b) accepts via the first branch, but the `b c` path is still open.
        let alt = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Alt(vec![
                vars("b"),
                Pattern::Concat(vec![vars("b"), vars("c")]),
            ]),
        ]));
        assert!(alt.may_extend(0, 2, &m("ab")).await.unwrap());

        // (a b{1,2}): one `b` leaves the range open; two exhaust it.
        let range = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(
                Box::new(vars("b")),
                Quantifier::Range {
                    min: 1,
                    max: Some(2),
                },
                false,
            ),
        ]));
        assert!(range.may_extend(0, 2, &m("ab")).await.unwrap());
        assert!(!range.may_extend(0, 3, &m("abb")).await.unwrap());

        // (a b?): the optional tail extends a bare [a]; a consumed (a, b) is terminal.
        let opt = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Question, false),
        ]));
        assert!(opt.may_extend(0, 1, &m("a")).await.unwrap());
        assert!(!opt.may_extend(0, 2, &m("ab")).await.unwrap());
    }

    #[tokio::test]
    async fn reaches_boundary_alive_evicts_dead_prefix() {
        // PATTERN (a b): a start is live only if a match from it can still reach the safe boundary.
        let nfa = Nfa::compile(&Pattern::Concat(vec![vars("a"), vars("b")]));

        // `[a]` with the boundary right after it: the `a` is a live partial match — a future `b` may
        // complete it — so it must be retained.
        let m = SetMatcher { rows: rows("a") };
        assert!(nfa.reaches_boundary_alive(0, 1, &m).await.unwrap());

        // `[a, x]` and `[a, x, x]` (x satisfies neither `a` nor `b`): the `a` can still *begin* the
        // pattern, but the following safe rows already block it from completing, so it is dead and
        // must be evictable. This is the case the previous `can_begin_at`-based predicate retained
        // forever.
        let m = SetMatcher { rows: rows("ax") };
        assert!(!nfa.reaches_boundary_alive(0, 2, &m).await.unwrap());
        let m = SetMatcher { rows: rows("axx") };
        assert!(!nfa.reaches_boundary_alive(0, 3, &m).await.unwrap());

        // A later start can be the live one: in `[x, a]` row 0 is dead but row 1 (the `a`) is live.
        let m = SetMatcher { rows: rows("xa") };
        assert!(!nfa.reaches_boundary_alive(0, 2, &m).await.unwrap());
        assert!(nfa.reaches_boundary_alive(1, 2, &m).await.unwrap());

        // A complete match sitting exactly at the boundary is not yet finalized (it needs a following
        // safe row to confirm maximality), so its start is still retained.
        let m = SetMatcher { rows: rows("ab") };
        assert!(nfa.reaches_boundary_alive(0, 2, &m).await.unwrap());
    }

    /// A path-dependent matcher: `b` only matches once an `a` has been bound earlier in the match.
    /// This exercises threading the running labels into the predicate.
    struct NeedsPrecedingA;
    impl CandidateMatcher for NeedsPrecedingA {
        async fn matches(
            &self,
            var: &str,
            _pos: usize,
            labels: &[String],
        ) -> StreamExecutorResult<bool> {
            Ok(match var {
                "a" => true,
                "b" => labels.iter().any(|l| l == "a"),
                _ => false,
            })
        }
    }

    #[tokio::test]
    async fn reluctant_quantifier_prefers_fewer() {
        // Three rows that each satisfy both `a` and `b`, so `a+ b` can stop early.
        let rows = vec![BTreeSet::from(["a".to_owned(), "b".to_owned()]); 3];
        let m = SetMatcher { rows: rows.clone() };

        // Greedy `a+ b`: consume as many `a` as possible -> [0, 3) (a a b).
        let greedy = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Quantified(Box::new(vars("a")), Quantifier::Plus, false),
            vars("b"),
        ]));
        assert_eq!(
            greedy
                .find_matches_dynamic(rows.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 3,
                labels: lbl("aab")
            }]
        );

        // Reluctant `a+? b`: take the fewest `a` -> [0, 2) (a b), then [2, ...) finds nothing more.
        let reluctant = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Quantified(Box::new(vars("a")), Quantifier::Plus, true),
            vars("b"),
        ]));
        assert_eq!(
            reluctant
                .find_matches_dynamic(rows.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 2,
                labels: lbl("ab")
            }]
        );
    }

    /// `n` rows that each satisfy both `a` and `b`, so quantifier preference (not the predicates)
    /// decides how a match is split between variables.
    fn ab_rows(n: usize) -> Vec<BTreeSet<String>> {
        vec![BTreeSet::from(["a".to_owned(), "b".to_owned()]); n]
    }

    fn plus(inner: Pattern, reluctant: bool) -> Pattern {
        Pattern::Quantified(Box::new(inner), Quantifier::Plus, reluctant)
    }

    fn star(inner: Pattern, reluctant: bool) -> Pattern {
        Pattern::Quantified(Box::new(inner), Quantifier::Star, reluctant)
    }

    #[tokio::test]
    async fn nested_reluctant_then_greedy_adjacent() {
        // `a*? a*` over three `a` rows. The reluctant first star takes as few as possible (zero) and
        // the greedy second star takes the rest, so the whole run is still consumed: [0, 3). This
        // guards against an empty-match or non-termination bug when two quantifiers over the same
        // variable sit adjacent with opposite preferences.
        let r = rows("aaa");
        let m = SetMatcher { rows: r.clone() };
        let nfa = Nfa::compile(&Pattern::Concat(vec![
            star(vars("a"), true),
            star(vars("a"), false),
        ]));
        assert_eq!(
            nfa.find_matches_dynamic(r.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 3,
                labels: lbl("aaa")
            }]
        );
    }

    #[tokio::test]
    async fn nested_quantifier_preference_flips_split() {
        // Four rows that each satisfy both `a` and `b`, matched by `(<a-quant> b+)+`. The inner
        // first-variable quantifier's preference decides the split; the rest is greedy `b+`.
        let r = ab_rows(4);
        let m = SetMatcher { rows: r.clone() };

        // Reluctant `a+?` takes the fewest `a` (one), then greedy `b+` takes the rest -> "abbb".
        let reluctant = Nfa::compile(&plus(
            Pattern::Concat(vec![plus(vars("a"), true), plus(vars("b"), false)]),
            false,
        ));
        assert_eq!(
            reluctant
                .find_matches_dynamic(r.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 4,
                labels: lbl("abbb")
            }]
        );

        // Greedy `a+` takes as many `a` as it can while still leaving one row for the mandatory
        // `b+`, so it backtracks from four to three -> "aaab".
        let greedy = Nfa::compile(&plus(
            Pattern::Concat(vec![plus(vars("a"), false), plus(vars("b"), false)]),
            false,
        ));
        assert_eq!(
            greedy
                .find_matches_dynamic(r.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 4,
                labels: lbl("aaab")
            }]
        );
    }

    #[tokio::test]
    async fn dynamic_threads_running_labels() {
        // (a b): `b` sees `a` in the running labels -> matches.
        let ab = Nfa::compile(&Pattern::Concat(vec![vars("a"), vars("b")]));
        let m = NeedsPrecedingA;
        assert_eq!(
            ab.find_matches_dynamic(2, &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 2,
                labels: lbl("ab")
            }]
        );

        // (b a): `b` is first, the running labels are empty, so it cannot match -> no match.
        let ba = Nfa::compile(&Pattern::Concat(vec![vars("b"), vars("a")]));
        assert_eq!(
            ba.find_matches_dynamic(2, &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![]
        );
    }
}
