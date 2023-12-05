// Copyright 2023 RisingWave Labs
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

//! This is for arrow dependency named `arrow-xxx-deltalake` such as `arrow-array-deltalake`
//! in the cargo workspace.
//!
//! The corresponding version of arrow is currently used by `deltalake` sink.

pub use arrow_impl::to_record_batch_with_schema as to_deltalake_record_batch_with_schema;
use {
    arrow_array_deltalake as arrow_array, arrow_buffer_deltalake as arrow_buffer,
    arrow_cast_deltalake as arrow_cast, arrow_schema_deltalake as arrow_schema,
};

#[expect(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;
