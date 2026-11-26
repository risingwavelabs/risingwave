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

//! A tournament tree for incrementally selecting the next input in a k-way merge.

use std::cmp::Ordering;

/// Selects the least head of a fixed set of inputs. Equal heads are ordered by input index.
///
/// Initialization takes O(k) comparisons. Replacing the winning head takes O(log k)
/// comparisons; other heads must not change their relative order.
pub struct LoserTree<T, F> {
    heads: Vec<Option<T>>,
    compare: F,
    // Internal nodes are numbered from 1; leaves start at heads.len().
    losers: Vec<usize>,
    winner: usize,
}

impl<T, F: Fn(&T, &T) -> Ordering> LoserTree<T, F> {
    /// Creates a tree with one head per input, using None for an empty input.
    pub fn new(heads: Vec<Option<T>>, compare: F) -> Self {
        let mut tree = Self {
            losers: vec![0; heads.len()],
            heads,
            compare,
            winner: 0,
        };
        if !tree.heads.is_empty() {
            tree.winner = tree.play_initial(1);
        }
        tree
    }

    /// Visits the winning head and advances that input in place.
    ///
    /// Returning false from the callback, or returning an error, retires the input.
    /// The outer return value is false only when no head was available.
    pub fn advance<E>(
        &mut self,
        next: impl FnOnce(usize, &mut T) -> Result<bool, E>,
    ) -> Result<bool, E> {
        let source = self.winner;
        let Some(Some(head)) = self.heads.get_mut(source) else {
            return Ok(false);
        };
        let result = next(source, head);
        if !matches!(&result, Ok(true)) {
            self.heads[source] = None;
        }
        // Replay only the previous winner's path.
        let mut winner = source;
        let mut node = (self.heads.len() + source) / 2;
        while node != 0 {
            let loser = self.losers[node];
            if self.wins(loser, winner) {
                self.losers[node] = winner;
                winner = loser;
            }
            node /= 2;
        }
        self.winner = winner;
        result.map(|_| true)
    }

    fn play_initial(&mut self, node: usize) -> usize {
        if node >= self.heads.len() {
            return node - self.heads.len();
        }
        let left = self.play_initial(2 * node);
        let right = self.play_initial(2 * node + 1);
        let (winner, loser) = if self.wins(left, right) {
            (left, right)
        } else {
            (right, left)
        };
        self.losers[node] = loser;
        winner
    }

    fn wins(&self, a: usize, b: usize) -> bool {
        match (&self.heads[a], &self.heads[b]) {
            (Some(left), Some(right)) => {
                (self.compare)(left, right).then_with(|| a.cmp(&b)).is_lt()
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => a < b,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    #[test]
    fn merges_inputs_with_ties_and_uneven_lengths() {
        for count in 0..=16 {
            let inputs: Vec<Vec<_>> = (0..count)
                .map(|source| {
                    (0..(source * 7 % 12))
                        .map(|i| (i / 2 + source % 3, i))
                        .collect()
                })
                .collect();
            let mut expected: Vec<_> = inputs
                .iter()
                .enumerate()
                .flat_map(|(source, input)| {
                    input
                        .iter()
                        .map(move |&(key, ordinal)| (key, source, ordinal))
                })
                .collect();
            expected.sort_unstable();
            let mut inputs: Vec<_> = inputs.into_iter().map(Vec::into_iter).collect();
            let mut tree =
                LoserTree::new(inputs.iter_mut().map(Iterator::next).collect(), |a, b| {
                    a.0.cmp(&b.0)
                });
            let mut actual = vec![];
            while tree
                .advance(|source, head| {
                    actual.push((head.0, source, head.1));
                    let Some(next) = inputs[source].next() else {
                        return Ok::<_, Infallible>(false);
                    };
                    *head = next;
                    Ok(true)
                })
                .unwrap()
            {}
            assert_eq!(actual, expected, "{count} inputs");
        }
    }

    #[test]
    fn retires_a_failed_input() {
        let mut tree = LoserTree::new(vec![Some(2), Some(1), Some(3)], Ord::cmp);
        assert_eq!(
            tree.advance(|source, value| {
                assert_eq!((source, *value), (1, 1));
                Err("read failed")
            }),
            Err("read failed")
        );
        assert_eq!(
            tree.advance(|source, value| {
                assert_eq!((source, *value), (0, 2));
                Ok::<_, Infallible>(false)
            }),
            Ok(true)
        );
    }
}
