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

mod etcd_meta_store;
mod etcd_retry_client;
mod mem_meta_store;
pub mod meta_store;
#[cfg(test)]
mod tests;
mod transaction;

pub type ColumnFamily = String;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub use etcd_meta_store::*;
pub use mem_meta_store::*;
pub use meta_store::*;
pub use transaction::*;
