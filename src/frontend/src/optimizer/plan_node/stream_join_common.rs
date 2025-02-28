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

use itertools::Itertools;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::JoinType;

use super::{EqJoinPredicate, generic};
use crate::PlanRef;
use crate::optimizer::property::Distribution;
use crate::utils::ColIndexMappingRewriteExt;

pub struct StreamJoinCommon;

impl StreamJoinCommon {
    pub(super) fn get_dist_key_in_join_key(
        left_dk_indices: &[usize],
        right_dk_indices: &[usize],
        eq_join_predicate: &EqJoinPredicate,
    ) -> Vec<usize> {
        let left_jk_indices = eq_join_predicate.left_eq_indexes();
        let right_jk_indices = &eq_join_predicate.right_eq_indexes();
        assert_eq!(left_jk_indices.len(), right_jk_indices.len());
        let mut dk_indices_in_jk = vec![];
        for (l_dk_idx, r_dk_idx) in left_dk_indices.iter().zip_eq_fast(right_dk_indices.iter()) {
            for dk_idx_in_jk in left_jk_indices.iter().positions(|idx| idx == l_dk_idx) {
                if right_jk_indices[dk_idx_in_jk] == *r_dk_idx {
                    dk_indices_in_jk.push(dk_idx_in_jk);
                    break;
                }
            }
        }
        assert_eq!(dk_indices_in_jk.len(), left_dk_indices.len());
        dk_indices_in_jk
    }

    pub(super) fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        logical: &generic::Join<PlanRef>,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (Distribution::HashShard(_), Distribution::HashShard(_)) => {
                // we can not derive the hash distribution from the side where outer join can
                // generate a NULL row
                match logical.join_type {
                    JoinType::Unspecified => {
                        unreachable!()
                    }
                    JoinType::FullOuter => Distribution::SomeShard,
                    JoinType::Inner
                    | JoinType::LeftOuter
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::AsofInner
                    | JoinType::AsofLeftOuter => {
                        let l2o = logical
                            .l2i_col_mapping()
                            .composite(&logical.i2o_col_mapping());
                        l2o.rewrite_provided_distribution(left)
                    }
                    JoinType::RightSemi | JoinType::RightAnti | JoinType::RightOuter => {
                        let r2o = logical
                            .r2i_col_mapping()
                            .composite(&logical.i2o_col_mapping());
                        r2o.rewrite_provided_distribution(right)
                    }
                }
            }
            (_, _) => unreachable!(
                "suspicious distribution: left: {:?}, right: {:?}",
                left, right
            ),
        }
    }
}
