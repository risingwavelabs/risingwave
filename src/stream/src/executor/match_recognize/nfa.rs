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

use std::collections::{BTreeSet, HashSet};

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
}

impl Nfa {
    /// Compile a [`Pattern`] into an NFA.
    pub fn compile(pattern: &Pattern) -> Self {
        let mut builder = NfaBuilder { states: Vec::new() };
        let frag = builder.build(pattern);
        Nfa {
            states: builder.states,
            start: frag.start,
            accept: frag.accept,
        }
    }

    /// The set of states reachable from `states` via ε-transitions (inclusive).
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

impl SkipMode {
    /// The position the scan resumes at after a match spanning `[start, end)` with per-row `labels`
    /// (`labels[i]` is the variable bound to `rows[start + i]`). Always returns `> start` so the scan
    /// makes progress: `SKIP TO FIRST` of the match's leading variable would not advance, which the
    /// SQL standard reports as an error; here it degrades to advancing one row instead of looping.
    pub fn next_pos(&self, start: usize, end: usize, labels: &[String]) -> usize {
        let target = match self {
            SkipMode::PastLastRow => end,
            SkipMode::ToNextRow => start + 1,
            SkipMode::ToFirst(var) => labels
                .iter()
                .position(|l| l == var)
                .map_or(end, |j| start + j),
            SkipMode::ToLast(var) => labels
                .iter()
                .rposition(|l| l == var)
                .map_or(end, |j| start + j),
        };
        target.max(start + 1)
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
                i = skip.next_pos(start, end, &labels);
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
            let mut visited: HashSet<StateId> = HashSet::new();
            let found = self
                .preferred_from_dynamic(n_rows, self.start, i, &mut path, matcher, &mut visited)
                .await?;
            if let Some((end, labels)) = found
                && end > i
            {
                let start = i;
                i = skip.next_pos(start, end, &labels);
                matches.push(LabeledMatch { start, end, labels });
            } else {
                i += 1;
            }
        }
        Ok(matches)
    }

    /// Whether the row at `pos` can be the *first* matched row of the pattern: some pattern variable
    /// reachable from the start state via ε-transitions accepts it under an empty running match.
    /// Used to evict finalized rows that can no longer begin a match (a match is contiguous from its
    /// start, so a row that cannot start the pattern and sits before every live match is dead).
    pub async fn can_begin_at(
        &self,
        pos: usize,
        matcher: &(impl CandidateMatcher + Sync),
    ) -> StreamExecutorResult<bool> {
        let mut first_vars: BTreeSet<&str> = BTreeSet::new();
        for s in self.epsilon_closure([self.start]) {
            for t in &self.states[s] {
                if let Transition::OnVar { var, .. } = t {
                    first_vars.insert(var.as_str());
                }
            }
        }
        for var in first_vars {
            if matcher.matches(var, pos, &[]).await? {
                return Ok(true);
            }
        }
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
        visited: &mut HashSet<StateId>,
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
                        let mut next_visited = HashSet::new();
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
                visited.remove(&state);
                return Ok(candidate);
            }
        }
        visited.remove(&state);
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
