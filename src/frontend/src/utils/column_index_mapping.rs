// Copyright 2023 RisingWave Labs
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

use std::vec;

use fixedbitset::FixedBitSet;
pub use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::ColumnOrder;

use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::property::{
    Distribution, FunctionalDependency, FunctionalDependencySet, Order, RequiredDist,
};

/// Extension trait for [`ColIndexMapping`] to rewrite frontend structures.
#[easy_ext::ext(ColIndexMappingRewriteExt)]
impl ColIndexMapping {
    /// Rewrite the provided order's column index. It will try its best to give the most accurate
    /// order. Order(0,1,2) with mapping(0->1,1->0,2->2) will be rewritten to Order(1,0,2)
    /// Order(0,1,2) with mapping(0->1,2->0) will be rewritten to Order(1)
    pub fn rewrite_provided_order(&self, order: &Order) -> Order {
        let mut mapped_column_orders = vec![];
        for column_order in &order.column_orders {
            match self.try_map(column_order.column_index) {
                Some(mapped_index) => mapped_column_orders
                    .push(ColumnOrder::new(mapped_index, column_order.order_type)),
                None => break,
            }
        }
        Order {
            column_orders: mapped_column_orders,
        }
    }

    /// Rewrite the required order's field index. if it can't give a corresponding
    /// required order after the column index mapping, it will return None.
    /// Order(0,1,2) with mapping(0->1,1->0,2->2) will be rewritten to Order(1,0,2)
    /// Order(0,1,2) with mapping(0->1,2->0) will return None
    pub fn rewrite_required_order(&self, order: &Order) -> Option<Order> {
        order
            .column_orders
            .iter()
            .map(|o| {
                self.try_map(o.column_index)
                    .map(|mapped_index| ColumnOrder::new(mapped_index, o.order_type))
            })
            .collect::<Option<Vec<_>>>()
            .map(|mapped_column_orders| Order {
                column_orders: mapped_column_orders,
            })
    }

    /// Rewrite the distribution key and will return None if **any** index of the key disappear
    /// after the mapping.
    pub fn rewrite_dist_key(&self, key: &[usize]) -> Option<Vec<usize>> {
        self.try_map_all(key.iter().copied())
    }

    /// Rewrite the provided distribution's field index. It will try its best to give the most
    /// accurate distribution.
    /// HashShard(0,1,2), with mapping(0->1,1->0,2->2) will be rewritten to HashShard(1,0,2).
    /// HashShard(0,1,2), with mapping(0->1,2->0) will be rewritten to `SomeShard`.
    pub fn rewrite_provided_distribution(&self, dist: &Distribution) -> Distribution {
        let mapped_dist_key = self.rewrite_dist_key(dist.dist_column_indices());

        match (mapped_dist_key, dist) {
            (None, Distribution::HashShard(_)) | (None, Distribution::UpstreamHashShard(_, _)) => {
                Distribution::SomeShard
            }
            (Some(mapped_dist_key), Distribution::HashShard(_)) => {
                Distribution::HashShard(mapped_dist_key)
            }
            (Some(mapped_dist_key), Distribution::UpstreamHashShard(_, table_id)) => {
                Distribution::UpstreamHashShard(mapped_dist_key, *table_id)
            }
            _ => {
                assert!(dist.dist_column_indices().is_empty());
                dist.clone()
            }
        }
    }

    /// Rewrite the required distribution's field index. if it can't give a corresponding
    /// required distribution after the column index mapping, it will return None.
    /// ShardByKey(0,1,2), with mapping(0->1,1->0,2->2) will be rewritten to ShardByKey(1,0,2).
    /// ShardByKey(0,1,2), with mapping(0->1,2->0) will return ShardByKey(1,0).
    /// ShardByKey(0,1), with mapping(2->0) will return `Any`.
    pub fn rewrite_required_distribution(&self, dist: &RequiredDist) -> RequiredDist {
        match dist {
            RequiredDist::ShardByKey(keys) => {
                let keys = self.rewrite_bitset(keys);
                if keys.count_ones(..) == 0 {
                    RequiredDist::Any
                } else {
                    RequiredDist::ShardByKey(keys)
                }
            }
            RequiredDist::PhysicalDist(dist) => match dist {
                Distribution::HashShard(keys) => {
                    let keys = self.rewrite_dist_key(keys);
                    match keys {
                        Some(keys) => RequiredDist::PhysicalDist(Distribution::HashShard(keys)),
                        None => RequiredDist::Any,
                    }
                }
                _ => RequiredDist::PhysicalDist(dist.clone()),
            },
            _ => dist.clone(),
        }
    }

    /// Rewrite the indices in a functional dependency.
    ///
    /// If some columns in the `from` side are removed, then this fd is no longer valid. For
    /// example, for ABC --> D, it means that A, B, and C together can determine C. But if B is
    /// removed, this fd is not valid. For this case, we will return [`None`]
    ///
    /// Additionally, If the `to` side of a functional dependency becomes empty after rewriting, it
    /// means that this dependency is unneeded so we also return [`None`].
    pub fn rewrite_functional_dependency(
        &self,
        fd: &FunctionalDependency,
    ) -> Option<FunctionalDependency> {
        let new_from = self.rewrite_bitset(fd.from());
        let new_to = self.rewrite_bitset(fd.to());
        if new_from.count_ones(..) != fd.from().count_ones(..) || new_to.is_clear() {
            None
        } else {
            Some(FunctionalDependency::new(new_from, new_to))
        }
    }

    /// Rewrite functional dependencies in `fd_set` one by one, using
    /// `[ColIndexMapping::rewrite_functional_dependency]`.
    ///
    /// Note that this rewrite process handles each function dependency independently.
    /// Relationships within function dependencies are not considered.
    /// For example, if we have `fd_set` { AB --> C, A --> B }, and column B is removed.
    /// The result would be an empty `fd_set`, rather than { A --> C }.
    pub fn rewrite_functional_dependency_set(
        &self,
        fd_set: FunctionalDependencySet,
    ) -> FunctionalDependencySet {
        let mut new_fd_set = FunctionalDependencySet::new(self.target_size());
        for i in fd_set.into_dependencies() {
            if let Some(fd) = self.rewrite_functional_dependency(&i) {
                new_fd_set.add_functional_dependency(fd);
            }
        }
        new_fd_set
    }

    pub fn rewrite_bitset(&self, bitset: &FixedBitSet) -> FixedBitSet {
        assert_eq!(bitset.len(), self.source_size());
        let mut ret = FixedBitSet::with_capacity(self.target_size());
        for i in bitset.ones() {
            if let Some(i) = self.try_map(i) {
                ret.insert(i);
            }
        }
        ret
    }
}

impl ExprRewriter for ColIndexMapping {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(self.map(input_ref.index()), input_ref.return_type()).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_fd() {
        let mapping = ColIndexMapping::with_remaining_columns(&[1, 0], 4);
        let new_fd = |from, to| FunctionalDependency::with_indices(4, from, to);
        let fds_with_expected_res = vec![
            (new_fd(&[0, 1], &[2, 3]), None),
            (new_fd(&[2], &[0, 1]), None),
            (
                new_fd(&[1], &[0]),
                Some(FunctionalDependency::with_indices(2, &[0], &[1])),
            ),
        ];
        for (input, expected) in fds_with_expected_res {
            assert_eq!(mapping.rewrite_functional_dependency(&input), expected);
        }
    }

    #[test]
    fn test_rewrite_fd_set() {
        let new_fd = |from, to| FunctionalDependency::with_indices(4, from, to);
        let fd_set = FunctionalDependencySet::with_dependencies(
            4,
            vec![
                // removed
                new_fd(&[0, 1], &[2, 3]),
                new_fd(&[2], &[0, 1]),
                new_fd(&[0, 1, 2], &[3]),
                // empty mappings will be removed
                new_fd(&[], &[]),
                new_fd(&[1], &[]),
                // constant column mapping will be kept
                new_fd(&[], &[0]),
                // kept
                new_fd(&[1], &[0]),
            ],
        );
        let mapping = ColIndexMapping::with_remaining_columns(&[1, 0], 4);
        let result = mapping.rewrite_functional_dependency_set(fd_set);
        let expected = FunctionalDependencySet::with_dependencies(
            2,
            vec![
                FunctionalDependency::with_indices(2, &[], &[1]),
                FunctionalDependency::with_indices(2, &[0], &[1]),
            ],
        );
        assert_eq!(result, expected);
    }
}
