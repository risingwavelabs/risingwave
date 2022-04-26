// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use itertools::Itertools;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct JoinTable(pub usize);

/// Represents whether `left` and `right` can be joined using a condition.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JoinEdge {
    pub left: JoinTable,
    pub right: JoinTable,
    pub left_join_keys: Vec<usize>,
    pub right_join_keys: Vec<usize>,
}

impl JoinEdge {
    /// Reverse the order of the edge.
    pub fn reverse(&self) -> Self {
        Self {
            left: self.right,
            right: self.left,
            left_join_keys: self.right_join_keys.clone(),
            right_join_keys: self.left_join_keys.clone(),
        }
    }
}

/// Decides how to place arrangements over lookup nodes. Given a 3-way join example:
///
/// ```plain
/// a -> 1* -> 2* ->
/// b -> 3  -> 4* ->
/// c -> 5  -> 6  ->
/// ```
///
/// If user provides the multi-way join order of `(a join b) join c`, and set the strategy to be
/// [`ArrangeStrategy::LeftFirst`], and if three tables are of the same join key (the graph is
/// fully-connected and no shuffle needed), then we will place the arrangements over the lookup
/// nodes in the following way:
///
/// ```plain
/// a -> b* -> c* ->
/// b -> a  -> c* ->
/// c -> a  -> b  ->
/// ```
///
/// The left side of joins will be preferred for the left lookups when selecting arrangements.
///
/// If strategy is set to [`ArrangeStrategy::RightFirst`],
///
/// ```plain
/// a -> c* -> b* ->
/// b -> c  -> a* ->
/// c -> b  -> a  ->
/// ```
///
/// The right side of joins will be preferred for the left lookups when selecting arrangements.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArrangeStrategy {
    /// The left-most table will be preferred to be the first lookup table.
    LeftFirst,
    /// The right-most table will be preferred to be the first lookup table.
    RightFirst,
}

/// Decides how to place stream inputs. Given a 3-way join example:
///
/// ```plain
/// x -> 1* -> 2* ->
/// x -> 3  -> 4* ->
/// x -> 5  -> 6  ->
/// ```
///
/// ... where `n*` means lookup this epoch. If user provides the multi-way join order of `(a join b)
/// join c`, and set the strategy to be [`StreamStrategy::LeftThisEpoch`], then we will place the
/// stream side as:
///
/// ```plain
/// a -> 1* -> 2* ->
/// b -> 3  -> 4* ->
/// c -> 5  -> 6  ->
/// ```
///
/// ... where `a`, the left-most table in multi-way join, passes through most number of
/// lookup-this-epoch.
///
/// If strategy is set to [`StreamStrategy::RightThisEpoch`],
///
/// ```plain
/// c -> 1* -> 2* ->
/// b -> 3  -> 4* ->
/// a -> 5  -> 6  ->
/// ```
///
/// ... where `c`, the right-most table in multi-way join, passes through most number of
/// lookup-this-epoch.
///
/// More lookup-this-epochs in a lookup row means more latency when a barrier is flushed. If lookup
/// executor is set to lookup this epoch, it will wait until a barrier from the arrangement side
/// before actually starting lookups and joins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamStrategy {
    /// The left-most table will be preferred to be the stream to lookup this epoch.
    LeftThisEpoch,
    /// The right-most table will be preferred to be the stream to lookup this epoch.
    RightThisEpoch,
}

/// Given a multi-way inner join plan, the solver will produces the most efficient way of getting
/// those join done.
///
/// ## Input
///
/// [`JoinSolver`] needs the information of the cost of joining two tables. You'll need to provide
/// edges of the join graph. Each [`JoinEdge`] `(a, b) -> cost` describes the cost of inner join a
/// and b.
///
/// ## Output
///
/// The `solve` function will return a vector of [`LookupPath`]. See [`LookupPath`] for more.
pub struct JoinSolver {
    /// Strategy of arrangement placement, see docs of [`ArrangeStrategy`] for more details.
    pub arrange_strategy: ArrangeStrategy,

    /// Strategy of stream placement, see docs of [`StreamStrategy`] for more details.
    pub stream_strategy: StreamStrategy,

    /// Possible combination of joins. If `(a, b)` is in `edges`, then `a` and `b` can be joined.
    /// The edge is bi-directional, so only one pair (either `a, b` or `b, a`) needs to be present
    /// in this vector.
    pub edges: Vec<JoinEdge>,

    /// The recommended join order from optimizer in a multi-way join plan node.
    pub join_order: Vec<JoinTable>,
}

/// A row in the lookup plan, which includes the stream side arrangement, and all arrangements to be
/// placed in the lookup node.
///
/// For example, if we have `LookupPath(a, vec![b, c])`, then we have a row:
///
/// ```plain
/// a -> b -> c
/// ```
///
/// In `Vec<LookupPath>`, the first row always contains the most lookup-this-epochs. For example, if
/// we have:
///
/// ```plain
/// vec![
///   LookupPath(a, vec![b, c]),
///   LookupPath(b, vec![c, a]),
///   LookupPath(c, vec![a, b])
/// ]
/// ```
///
/// Then it means...
///
/// ```plain
/// a -> b* -> c* ->
/// b -> c  -> a* ->
/// c -> a  -> b  ->
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupPath(JoinTable, pub Vec<JoinTable>);

struct SolverEnv {
    /// Stores all join edges in hash map. [`JoinEdge`]'s left side is always the key in hash map.
    join_edge: HashMap<JoinTable, Vec<JoinEdge>>,

    /// Placement order of arrangements
    arrange_placement_order: Vec<JoinTable>,

    /// Placement order of streams
    stream_placement_order: Vec<JoinTable>,
}

impl SolverEnv {
    fn build_from(solver: &JoinSolver) -> Self {
        let mut join_edge = HashMap::new();

        for table in &solver.join_order {
            join_edge.insert(table.clone(), vec![]);
        }
        for edge in &solver.edges {
            join_edge.get_mut(&edge.left).unwrap().push(edge.clone());
            join_edge.get_mut(&edge.right).unwrap().push(edge.reverse());
        }

        Self {
            arrange_placement_order: match solver.arrange_strategy {
                ArrangeStrategy::LeftFirst => solver.join_order.clone(),
                ArrangeStrategy::RightFirst => {
                    solver.join_order.iter().copied().rev().collect_vec()
                }
            },
            stream_placement_order: match solver.stream_strategy {
                StreamStrategy::LeftThisEpoch => solver.join_order.clone(),
                StreamStrategy::RightThisEpoch => {
                    solver.join_order.iter().copied().rev().collect_vec()
                }
            },
            join_edge,
        }
    }
}

impl JoinSolver {
    /// Generate a lookup path using the user provided strategy.
    fn find_lookup_path(
        &self,
        solver_env: &SolverEnv,
        input_stream: JoinTable,
    ) -> Result<LookupPath> {
        // The table available to query
        let mut current_table_set = HashSet::new();
        current_table_set.insert(input_stream);

        // The distribution of the current lookup.
        let mut current_distribution = vec![];

        // The final lookup path.
        let mut path = vec![];

        fn satisfies_distribution(
            current_distribution: &[(JoinTable, Vec<usize>)],
            edge: &JoinEdge,
        ) -> bool {
            // TODO: if `current_distribution` is a `HashSet`, we can know if the distribution in
            // O(1). But as we generally have few tables, so we do a linear scan on tables.
            if current_distribution.is_empty() {
                return true;
            }

            for (table, join_keys) in current_distribution {
                if table == &edge.left && join_keys == &edge.left_join_keys {
                    return true;
                }
            }

            false
        }

        'next_table: loop {
            assert!(
                path.len() <= self.join_order.len() - 1,
                "internal error: infinite loop"
            );

            // step 1: find tables that can be joined with `current_table_set` and satisfies the
            // current distribution.
            let mut reachable_tables = HashMap::new();

            for current_table in &current_table_set {
                for edge in &solver_env.join_edge[current_table] {
                    if !current_table_set.contains(&edge.right) {
                        if satisfies_distribution(&current_distribution, &edge) {
                            reachable_tables.insert(edge.right, edge.clone());
                        }
                    }
                }
            }

            // step 2: place arrangements according to the arrange strategy, update current
            // distribution and current table set.
            for table in &solver_env.arrange_placement_order {
                if let Some(edge) = reachable_tables.get(table) {
                    current_table_set.insert(edge.right);
                    path.push(edge.right);
                    current_distribution.push((edge.right, edge.right_join_keys.clone()));
                    continue 'next_table;
                }
            }

            // not table can be joined using the same distribution at this point.

            // step 3: find all tables that can be joined with `current_table_set`, regardless of
            // distribution.
            let mut reachable_tables = HashMap::new();

            for current_table in &current_table_set {
                for edge in &solver_env.join_edge[current_table] {
                    if !current_table_set.contains(&edge.right) {
                        reachable_tables.insert(edge.right, edge.clone());
                    }
                }
            }

            // step 4: place arrangements according to the arrange strategy, update current
            // distribution and current table set.
            for table in &solver_env.arrange_placement_order {
                if let Some(edge) = reachable_tables.get(table) {
                    current_table_set.insert(edge.right);
                    path.push(edge.right);
                    current_distribution.clear();
                    current_distribution.push((edge.right, edge.right_join_keys.clone()));
                    continue 'next_table;
                }
            }

            // step 5: no tables can be joined any more, what happened?
            if self.join_order.len() - 1 == path.len() {
                break;
            } else {
                // no table can be joined, while path is still incomplete
                return Err(anyhow!(
                    "join plan cannot be generated, tables not connected."
                ));
            }
        }

        Ok(LookupPath(input_stream, path))
    }

    pub fn solve(&self) -> Result<Vec<LookupPath>> {
        let solver_env = SolverEnv::build_from(self);

        solver_env
            .stream_placement_order
            .iter()
            .map(|x| self.find_lookup_path(&solver_env, *x))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_2way_join() {
        let solver = JoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![2, 3],
                right_join_keys: vec![3, 2],
            }],
            join_order: vec![JoinTable(1), JoinTable(2)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(1), vec![JoinTable(2)]),
                LookupPath(JoinTable(2), vec![JoinTable(1)]),
            ]
        );

        let solver = JoinSolver {
            arrange_strategy: ArrangeStrategy::RightFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![2, 3],
                right_join_keys: vec![3, 2],
            }],
            join_order: vec![JoinTable(1), JoinTable(2)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(1), vec![JoinTable(2)]),
                LookupPath(JoinTable(2), vec![JoinTable(1)]),
            ]
        );

        let solver = JoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::RightThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![2, 3],
                right_join_keys: vec![3, 2],
            }],
            join_order: vec![JoinTable(1), JoinTable(2)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(2), vec![JoinTable(1)]),
                LookupPath(JoinTable(1), vec![JoinTable(2)]),
            ]
        );

        let solver = JoinSolver {
            arrange_strategy: ArrangeStrategy::RightFirst,
            stream_strategy: StreamStrategy::RightThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![2, 3],
                right_join_keys: vec![3, 2],
            }],
            join_order: vec![JoinTable(1), JoinTable(2)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(2), vec![JoinTable(1)]),
                LookupPath(JoinTable(1), vec![JoinTable(2)]),
            ]
        );
    }

    #[test]
    fn test_3way_join_one_key() {
        // Table 1: [2, 3] (composite key x)
        // Table 2: [3, 2] (composite key x)
        // Table 3: [1, 1] (composite key x)
        // t1.x == t2.x == t3.x

        let solver = JoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![
                JoinEdge {
                    left: JoinTable(1),
                    right: JoinTable(2),
                    left_join_keys: vec![2, 3],
                    right_join_keys: vec![3, 2],
                },
                JoinEdge {
                    left: JoinTable(2),
                    right: JoinTable(3),
                    left_join_keys: vec![3, 2],
                    right_join_keys: vec![1, 1],
                },
                JoinEdge {
                    left: JoinTable(3),
                    right: JoinTable(1),
                    left_join_keys: vec![1, 1],
                    right_join_keys: vec![2, 3],
                },
            ],
            join_order: vec![JoinTable(1), JoinTable(2), JoinTable(3)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(1), vec![JoinTable(2), JoinTable(3)]),
                LookupPath(JoinTable(2), vec![JoinTable(1), JoinTable(3)]),
                LookupPath(JoinTable(3), vec![JoinTable(1), JoinTable(2)])
            ]
        );
    }

    #[test]
    fn test_3way_join_two_keys() {
        // Table 1: [0] (key x)
        // Table 2: [0] (key x), [1] (key y)
        // Table 3: [1] (key y)
        // t1.x == t2.x and t2.y == t3.y
        //
        // t1 cannot directly join with t3 in this case, as they don't share the same key

        let solver = JoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![
                JoinEdge {
                    left: JoinTable(1),
                    right: JoinTable(2),
                    left_join_keys: vec![0],
                    right_join_keys: vec![0],
                },
                JoinEdge {
                    left: JoinTable(2),
                    right: JoinTable(3),
                    left_join_keys: vec![1],
                    right_join_keys: vec![1],
                },
            ],
            join_order: vec![JoinTable(1), JoinTable(2), JoinTable(3)],
        };

        let result = solver.solve().unwrap();

        assert_eq!(
            result,
            vec![
                LookupPath(JoinTable(1), vec![JoinTable(2), JoinTable(3)]),
                LookupPath(JoinTable(2), vec![JoinTable(1), JoinTable(3)]),
                LookupPath(JoinTable(3), vec![JoinTable(2), JoinTable(1)])
            ]
        );
    }

    #[test]
    fn test_invalid_plan() {
        let solver = JoinSolver {
            arrange_strategy: ArrangeStrategy::LeftFirst,
            stream_strategy: StreamStrategy::LeftThisEpoch,
            edges: vec![JoinEdge {
                left: JoinTable(1),
                right: JoinTable(2),
                left_join_keys: vec![0],
                right_join_keys: vec![0],
            }],
            join_order: vec![JoinTable(1), JoinTable(2), JoinTable(3)],
        };

        let result = solver.solve();

        assert!(result.is_err());
    }
}
