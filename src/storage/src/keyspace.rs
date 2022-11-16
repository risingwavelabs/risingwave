// Copyright 2022 Singularity Data
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

use std::future::Future;
use std::ops::{Bound, RangeBounds};

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;

use crate::error::StorageResult;
use crate::store::{ReadOptions, StateStoreRead};
use crate::StateStoreIter;

/// Provides API to read key-value pairs of a prefix in the storage backend.
#[derive(Clone)]
pub struct Keyspace<S> {
    store: S,
}

// TODO: remove storage interface from keyspace, and and call it directly in storage_table
impl<S> Keyspace<S> {
    /// Creates a shared root [`Keyspace`] for all executors of the same operator.
    ///
    /// By design, all executors of the same operator should share the same keyspace in order to
    /// support scaling out, and ensure not to overlap with each other. So we use `table_id`
    /// here.

    /// Creates a root [`Keyspace`] for a table.
    pub fn table_root(store: S, _id: TableId) -> Self {
        Self { store }
    }

    /// Gets the underlying state store.
    pub fn state_store(&self) -> &S {
        &self.store
    }
}
