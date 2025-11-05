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

use anyhow::Result;
use async_trait::async_trait;

use crate::types::DataType;

/// Output entry for `internal_get_fragment_vnodes`.
#[derive(Debug, Clone)]
pub struct FragmentVNodeEntry {
    pub actor_id: u32,
    pub worker_id: u32,
    pub vnodes: Vec<i16>,
}

/// Collection of vnode information for a fragment.
#[derive(Debug, Clone)]
pub struct FragmentVNodeInfo {
    pub fragment_id: u32,
    pub vnode_count: usize,
    pub actors: Vec<FragmentVNodeEntry>,
}

/// Abstraction for retrieving fragment vnode information from meta.
#[async_trait]
pub trait FragmentVNodeReader: Send + Sync {
    async fn get_fragment_vnodes(&self, fragment_id: u32) -> Result<FragmentVNodeInfo>;
}

/// Column definition used by `internal_get_fragment_vnodes`.
pub fn fragment_vnode_columns() -> Vec<(String, DataType)> {
    vec![
        ("fragment_id".to_owned(), DataType::Int32),
        ("actor_id".to_owned(), DataType::Int32),
        ("worker_id".to_owned(), DataType::Int32),
        ("vnode_count".to_owned(), DataType::Int32),
        ("vnodes".to_owned(), DataType::Int16.list()),
    ]
}
