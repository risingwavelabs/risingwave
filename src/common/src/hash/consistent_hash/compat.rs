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

use std::num::NonZeroUsize;

use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;

use super::vnode::VirtualNode;

/// The different cases of `maybe_vnode_count` field in the protobuf message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VnodeCount {
    /// The field is a placeholder and has to be filled first before using it.
    #[default]
    Placeholder,
    /// The field is set to a specific value.
    Set(NonZeroUsize),
    /// The field is unset because the table/fragment is persisted as hash-distributed
    /// in an older version.
    CompatHash,
    /// The field is unset because the table/fragment is persisted as singleton
    /// in an older version.
    CompatSingleton,
}

impl VnodeCount {
    /// Creates a `VnodeCount` set to the given value.
    pub fn set(v: impl TryInto<usize> + Copy + std::fmt::Debug) -> Self {
        let v = (v.try_into().ok())
            .filter(|v| (1..=VirtualNode::MAX_COUNT).contains(v))
            .unwrap_or_else(|| panic!("invalid vnode count {v:?}"));

        VnodeCount::Set(NonZeroUsize::new(v).unwrap())
    }

    /// Creates a `VnodeCount` set to the value for testing.
    ///
    /// Equivalent to `VnodeCount::set(VirtualNode::COUNT_FOR_TEST)`.
    pub fn for_test() -> Self {
        Self::set(VirtualNode::COUNT_FOR_TEST)
    }

    /// Converts to protobuf representation for `maybe_vnode_count`.
    pub fn to_protobuf(self) -> Option<u32> {
        // Effectively fills the compatibility cases with values.
        self.value_opt()
            .map_or(Some(0) /* placeholder */, |v| Some(v as _))
    }

    /// Returns the value of the vnode count, or `None` if it's a placeholder.
    pub fn value_opt(self) -> Option<usize> {
        match self {
            VnodeCount::Placeholder => None,
            VnodeCount::Set(v) => Some(v.get()),
            VnodeCount::CompatHash => Some(VirtualNode::COUNT_FOR_COMPAT),
            VnodeCount::CompatSingleton => Some(1),
        }
    }

    /// Returns the value of the vnode count. Panics if it's a placeholder.
    pub fn value(self) -> usize {
        self.value_opt()
            .expect("vnode count is a placeholder that must be filled by the meta service first")
    }
}

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
    /// Get the `maybe_vnode_count` field.
    fn vnode_count_inner(&self) -> VnodeCount;

    /// Returns the vnode count if it's set. Otherwise, returns [`VirtualNode::COUNT_FOR_COMPAT`]
    /// for distributed tables/fragments, and `1` for singleton tables/fragments, for backward
    /// compatibility. Panics if the field is a placeholder.
    ///
    /// See the documentation on the field of the implementing type for more details.
    fn vnode_count(&self) -> usize {
        self.vnode_count_inner().value()
    }
}

impl VnodeCountCompat for risingwave_pb::catalog::Table {
    fn vnode_count_inner(&self) -> VnodeCount {
        if let Some(vnode_count) = self.maybe_vnode_count {
            VnodeCount::set(vnode_count)
        } else
        // Compatibility: derive vnode count from distribution.
        if self.distribution_key.is_empty()
            && self.dist_key_in_pk.is_empty()
            && self.vnode_col_index.is_none()
        {
            VnodeCount::CompatSingleton
        } else {
            VnodeCount::CompatHash
        }
    }
}

impl VnodeCountCompat for risingwave_pb::plan_common::StorageTableDesc {
    fn vnode_count_inner(&self) -> VnodeCount {
        if let Some(vnode_count) = self.maybe_vnode_count {
            VnodeCount::set(vnode_count)
        } else
        // Compatibility: derive vnode count from distribution.
        if self.dist_key_in_pk_indices.is_empty() && self.vnode_col_idx_in_pk.is_none() {
            VnodeCount::CompatSingleton
        } else {
            VnodeCount::CompatHash
        }
    }
}

impl VnodeCountCompat for risingwave_pb::meta::table_fragments::Fragment {
    fn vnode_count_inner(&self) -> VnodeCount {
        if let Some(vnode_count) = self.maybe_vnode_count {
            VnodeCount::set(vnode_count)
        } else {
            // Compatibility: derive vnode count from distribution.
            match self.distribution_type() {
                FragmentDistributionType::Unspecified => unreachable!(),
                FragmentDistributionType::Single => VnodeCount::CompatSingleton,
                FragmentDistributionType::Hash => VnodeCount::CompatHash,
            }
        }
    }
}
