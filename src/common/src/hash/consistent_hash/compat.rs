// Copyright 2024 RisingWave Labs
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

use super::vnode::VirtualNode;

/// A trait for accessing the vnode count field with backward compatibility.
///
/// # `maybe_`?
///
/// The reason why there's a `maybe_` prefix on the protobuf field is that, a getter
/// method with the same name as the field will be generated for `prost` structs.
/// Directly naming it `vnode_count` will lead to the method `vnode_count()` returning
/// `0` when the field is unset, which can be misleading sometimes.
///
/// Instead, we name the field as `maybe_vnode_count` and provide the method `vnode_count`
/// through this trait, ensuring that backward compatibility is handled properly.
pub trait VnodeCountCompat {
    /// Returns the vnode count if it's set. Otherwise, returns [`VirtualNode::COUNT_FOR_COMPAT`]
    /// for distributed tables/fragments, and `1` for singleton tables/fragments, for backward
    /// compatibility.
    ///
    /// See the documentation on the field of the implementing type for more details.
    fn vnode_count(&self) -> usize;
}

impl VnodeCountCompat for risingwave_pb::meta::table_fragments::Fragment {
    fn vnode_count(&self) -> usize {
        use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;

        if let Some(vnode_count) = self.maybe_vnode_count {
            return vnode_count as _;
        }

        // Compatibility: derive vnode count from distribution.
        match self.distribution_type() {
            FragmentDistributionType::Unspecified => unreachable!(),
            FragmentDistributionType::Single => 1,
            FragmentDistributionType::Hash => VirtualNode::COUNT_FOR_COMPAT,
        }
    }
}

impl VnodeCountCompat for risingwave_pb::catalog::Table {
    fn vnode_count(&self) -> usize {
        if let Some(vnode_count) = self.maybe_vnode_count {
            return vnode_count as _;
        }

        // Compatibility: derive vnode count from distribution.
        if self.distribution_key.is_empty()
            && self.dist_key_in_pk.is_empty()
            && self.vnode_col_index.is_none()
        {
            // Singleton table.
            1
        } else {
            VirtualNode::COUNT_FOR_COMPAT
        }
    }
}

impl VnodeCountCompat for risingwave_pb::plan_common::StorageTableDesc {
    fn vnode_count(&self) -> usize {
        if let Some(vnode_count) = self.maybe_vnode_count {
            return vnode_count as _;
        }

        // Compatibility: derive vnode count from distribution.
        if self.dist_key_in_pk_indices.is_empty() && self.vnode_col_idx_in_pk.is_none() {
            // Singleton table.
            1
        } else {
            VirtualNode::COUNT_FOR_COMPAT
        }
    }
}
