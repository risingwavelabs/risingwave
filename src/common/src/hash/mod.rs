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

mod consistent_hash; // TODO: move this to a separate module
mod dispatcher;
mod key;
mod key_v2;
pub mod table_distribution;

pub use consistent_hash::bitmap::*;
pub use consistent_hash::mapping::*;
pub use consistent_hash::vnode::*;
pub use consistent_hash::vnode_count::*;
pub use dispatcher::{HashKeyDispatcher, calc_hash_key_kind};
pub use key::{
    Crc32HashCode, HashCode, HashKeyDe, HashKeySer, HeapNullBitmap, MAX_GROUP_KEYS_ON_STACK,
    NullBitmap, PrecomputedBuildHasher, PrecomputedHasher, StackNullBitmap, XxHash64HashCode,
};
pub use key_v2::*; // TODO: reconstruct codes
