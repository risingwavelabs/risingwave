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

use itertools::Itertools;

/// The order in which a stream plan node outputs the rows of each vnode during backfill, as output
/// columns. An operator that needs its input replayed in the order of its state gets a locality
/// provider unless the input already replays rows in that order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ReplayOrder {
    columns: Vec<usize>,
    /// Whether the order also roughly holds for all rows of an actor. A table scan reads its vnodes
    /// side by side, while a locality provider replays them one after another. Only an order that
    /// holds across vnodes survives a shuffle.
    across_vnodes: bool,
}

impl ReplayOrder {
    /// The order of a table scan, which reads its vnodes side by side.
    pub fn across_vnodes(columns: Vec<usize>) -> Self {
        Self {
            columns,
            across_vnodes: true,
        }
    }

    /// The order of a locality provider, which replays its vnodes one after another.
    pub fn per_vnode(columns: Vec<usize>) -> Self {
        Self {
            columns,
            across_vnodes: false,
        }
    }

    pub fn columns(&self) -> &[usize] {
        &self.columns
    }

    /// Whether the rows are also in the order of `prefix`. A repeated column adds nothing to an
    /// order.
    pub fn starts_with(&self, prefix: &[usize]) -> bool {
        let mut columns = self.columns.iter().unique();
        prefix
            .iter()
            .unique()
            .all(|col| columns.next() == Some(col))
    }

    /// The order with other columns, e.g. mapped to the output of an operator.
    pub fn with_columns(&self, columns: Vec<usize>) -> Self {
        Self {
            columns,
            across_vnodes: self.across_vnodes,
        }
    }

    /// The longest prefix whose columns `f` maps.
    pub fn map_while(&self, f: impl FnMut(&usize) -> Option<usize>) -> Self {
        self.with_columns(self.columns.iter().map_while(f).collect())
    }

    /// The order a stateful operator keyed by `key` keeps: the longest prefix within the key, as
    /// the operator outputs the rows of each epoch clustered by its key in the order they arrive.
    pub fn within(&self, key: &[usize]) -> Self {
        self.with_columns(
            (self.columns.iter().copied().unique())
                .take_while(|col| key.contains(col))
                .collect(),
        )
    }

    /// The order after a shuffle, which interleaves the rows of all vnodes of each input actor.
    pub fn shuffled(&self) -> Self {
        if self.across_vnodes {
            self.clone()
        } else {
            Self::default()
        }
    }

    /// The order of interleaved inputs: the prefix they share.
    pub fn common<'a>(orders: impl IntoIterator<Item = &'a Self>) -> Self {
        orders
            .into_iter()
            .cloned()
            .reduce(|order, other| {
                let len = (order.columns.iter().enumerate())
                    .take_while(|&(i, col)| other.columns.get(i) == Some(col))
                    .count();
                Self {
                    columns: order.columns[..len].to_vec(),
                    across_vnodes: order.across_vnodes && other.across_vnodes,
                }
            })
            .unwrap_or_default()
    }
}
