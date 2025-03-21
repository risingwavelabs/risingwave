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

use std::num::NonZeroUsize;

use super::vnode::VirtualNode;

/// The different cases of `maybe_vnode_count` field in the protobuf message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VnodeCount {
    /// The field is a placeholder and has to be filled first before using it.
    #[default]
    Placeholder,
    /// The table/fragment is a singleton, thus the value should always be interpreted as `1`.
    Singleton,
    /// The field is set to a specific value.
    Set(NonZeroUsize),
    /// The field is unset because the table/fragment is persisted as hash-distributed
    /// in an older version.
    CompatHash,
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

    /// Converts from protobuf representation of `maybe_vnode_count`.
    ///
    /// The value will be ignored if `is_singleton` returns `true`.
    pub fn from_protobuf(v: Option<u32>, is_singleton: impl FnOnce() -> bool) -> Self {
        match v {
            Some(0) => VnodeCount::Placeholder,
            _ => {
                if is_singleton() {
                    if let Some(v) = v
                        && v != 1
                    {
                        tracing::debug!(
                            vnode_count = v,
                            "singleton has vnode count set to non-1, \
                            ignoring as it could be due to backward compatibility"
                        );
                    }
                    VnodeCount::Singleton
                } else {
                    v.map_or(VnodeCount::CompatHash, VnodeCount::set)
                }
            }
        }
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
            VnodeCount::Singleton => Some(1),
            VnodeCount::Set(v) => Some(v.get()),
            VnodeCount::CompatHash => Some(VirtualNode::COUNT_FOR_COMPAT),
        }
    }

    /// Returns the value of the vnode count. Panics if it's a placeholder.
    pub fn value(self) -> usize {
        self.value_opt()
            .expect("vnode count is a placeholder that must be filled by the meta service first")
    }
}

/// A trait for checking whether a table/fragment is a singleton.
pub trait IsSingleton {
    /// Returns `true` if the table/fragment is a singleton.
    ///
    /// By singleton, we mean that all data read from or written to the storage belongs to
    /// the only `SINGLETON_VNODE`. This must be consistent with the behavior of
    /// [`TableDistribution`](crate::hash::table_distribution::TableDistribution::new).
    /// As a result, the `vnode_count` of such table/fragment can be `1`.
    fn is_singleton(&self) -> bool;
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

impl IsSingleton for risingwave_pb::catalog::Table {
    fn is_singleton(&self) -> bool {
        self.distribution_key.is_empty()
            && self.dist_key_in_pk.is_empty()
            && self.vnode_col_index.is_none()
    }
}
impl VnodeCountCompat for risingwave_pb::catalog::Table {
    fn vnode_count_inner(&self) -> VnodeCount {
        VnodeCount::from_protobuf(self.maybe_vnode_count, || self.is_singleton())
    }
}

impl IsSingleton for risingwave_pb::plan_common::StorageTableDesc {
    fn is_singleton(&self) -> bool {
        self.dist_key_in_pk_indices.is_empty() && self.vnode_col_idx_in_pk.is_none()
    }
}
impl VnodeCountCompat for risingwave_pb::plan_common::StorageTableDesc {
    fn vnode_count_inner(&self) -> VnodeCount {
        VnodeCount::from_protobuf(self.maybe_vnode_count, || self.is_singleton())
    }
}
