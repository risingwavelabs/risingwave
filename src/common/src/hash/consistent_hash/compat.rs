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

use super::vnode::VirtualNode;

/// The different cases of `maybe_vnode_count` field in the protobuf message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VnodeCount {
    /// The field is a placeholder and has to be filled first before using it.
    #[default]
    Placeholder,
    /// The field is set to a specific value.
    Set(NonZeroUsize),
    /// The field is unset because it's persisted in an older version.
    Compat,
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
        match self {
            VnodeCount::Placeholder => Some(0),
            VnodeCount::Set(v) => Some(v.get() as _),
            VnodeCount::Compat => None,
        }
    }

    /// Converts from protobuf representation of `maybe_vnode_count`.
    pub fn from_protobuf(v: Option<u32>) -> Self {
        match v {
            Some(0) => VnodeCount::Placeholder,
            Some(v) => VnodeCount::set(v as usize),
            None => VnodeCount::Compat,
        }
    }

    /// Returns the value of the vnode count, or `None` if it's a placeholder.
    pub fn value_opt(self) -> Option<usize> {
        match self {
            VnodeCount::Placeholder => None,
            VnodeCount::Set(v) => Some(v.get()),
            VnodeCount::Compat => Some(VirtualNode::COUNT_FOR_COMPAT),
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

/// Implement the trait for given types by delegating to the `maybe_vnode_count` field.
///
/// The reason why there's a `maybe_` prefix is that, a getter method with the same name
/// as the field will be generated for `prost` structs. Directly naming it `vnode_count`
/// will lead to the method `vnode_count()` returning `0` when the field is unset, which
/// can be misleading sometimes.
///
/// Instead, we name the field as `maybe_vnode_count` and provide the method `vnode_count`
/// through this trait, ensuring that backward compatibility is handled properly.
macro_rules! impl_maybe_vnode_count_compat {
    ($($ty:ty),* $(,)?) => {
        $(
            impl VnodeCountCompat for $ty {
                fn vnode_count_inner(&self) -> VnodeCount {
                    VnodeCount::from_protobuf(self.maybe_vnode_count)
                }
            }
        )*
    };
}

// TODO!!!!!!!!!!!!!!!!!

// impl VnodeCountCompat for risingwave_pb::catalog::Table {
//     fn vnode_count(&self) -> usize {
//         if let Some(vnode_count) = self.maybe_vnode_count {
//             return vnode_count as _;
//         }

//         // Compatibility: derive vnode count from distribution.
//         if self.distribution_key.is_empty()
//             && self.dist_key_in_pk.is_empty()
//             && self.vnode_col_index.is_none()
//         {
//             // Singleton table.
//             1
//         } else {
//             VirtualNode::COUNT_FOR_COMPAT
//         }
//     }
// }

// impl VnodeCountCompat for risingwave_pb::plan_common::StorageTableDesc {
//     fn vnode_count(&self) -> usize {
//         if let Some(vnode_count) = self.maybe_vnode_count {
//             return vnode_count as _;
//         }

//         // Compatibility: derive vnode count from distribution.
//         if self.dist_key_in_pk_indices.is_empty() && self.vnode_col_idx_in_pk.is_none() {
//             // Singleton table.
//             1
//         } else {
//             VirtualNode::COUNT_FOR_COMPAT
//         }
//     }
// }
