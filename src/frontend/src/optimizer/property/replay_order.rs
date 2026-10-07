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

use super::Distribution;

/// The order in which a stream plan node outputs the rows of each vnode during backfill, as output
/// columns. An operator that needs its input replayed in the order of its state gets a locality
/// provider unless the input already replays each vnode in that order.
///
/// A table scan reads each vnode in primary key order, and a locality provider replays each vnode in
/// the order of its locality columns. A stateful operator keeps the part of its input's order within
/// its state key, see [`Self::within`], and an exchange keeps the order only if it keeps the vnode of
/// every row, see [`Self::exchanged`]. Other operators that keep the order of their input pass it
/// on, and the rest have none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ReplayOrder {
    columns: Vec<usize>,
}

impl ReplayOrder {
    pub fn new(columns: Vec<usize>) -> Self {
        Self { columns }
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

    /// The longest prefix whose columns `f` maps.
    pub fn map_while(&self, f: impl FnMut(&usize) -> Option<usize>) -> Self {
        Self::new(self.columns.iter().map_while(f).collect())
    }

    /// The order a stateful operator keyed by `key` keeps: the longest prefix within the key. The
    /// operator outputs the changes of the keys it received since its last flush, so the order
    /// holds across flushes, though not within one.
    pub fn within(&self, key: &[usize]) -> Self {
        Self::new(
            (self.columns.iter().copied().unique())
                .take_while(|col| key.contains(col))
                .collect(),
        )
    }

    /// The order after an exchange from `from` to `to`. A vnode keeps its order if all of its rows
    /// come from one input vnode: from a singleton, or through hashing by the input's own
    /// distribution key. The latter assumes the same vnode count on both sides, which meta only
    /// decides after planning; where they differ, an operator that relies on the order reads its
    /// state out of order, which costs locality but not correctness. Any other exchange
    /// interleaves the rows of several input vnodes in a vnode.
    pub fn exchanged(&self, from: &Distribution, to: &Distribution) -> Self {
        let keeps_vnodes = match (from, to) {
            (Distribution::Single, _) => true,
            (
                Distribution::HashShard(from_key) | Distribution::UpstreamHashShard(from_key, _),
                Distribution::HashShard(to_key),
            ) => from_key == to_key,
            _ => false,
        };
        if keeps_vnodes {
            self.clone()
        } else {
            Self::default()
        }
    }
}
