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

mod runner;
mod worker;

use std::ops::Bound;

#[cfg(test)]
use mockall::automock;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
pub use runner::*;
pub(crate) use worker::*;

use crate::error::Result;
use crate::{Record, TraceReadOptions, TraceWriteOptions};

type ReplayGroup = Record;

type WorkerResponse = ();

#[derive(Debug)]
pub(crate) enum ReplayRequest {
    Task(ReplayGroup),
    Fin,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub(crate) enum WorkerId {
    Local(u64),
    OneShot(u64),
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait Replayable: Send + Sync {
    async fn get(
        &self,
        key: Vec<u8>,
        epoch: u64,
        read_options: TraceReadOptions,
    ) -> Result<Option<Vec<u8>>>;
    async fn ingest(
        &self,
        kv_pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        delete_ranges: Vec<(Vec<u8>, Vec<u8>)>,
        write_options: TraceWriteOptions,
    ) -> Result<usize>;
    async fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: TraceReadOptions,
    ) -> Result<Box<dyn ReplayIter>>;
    async fn sync(&self, id: u64) -> Result<usize>;
    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
    async fn notify_hummock(&self, info: Info, op: RespOperation) -> Result<u64>;
    async fn new_local(&self, table_id: u32) -> Box<dyn Replayable>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayIter: Send + Sync {
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}

#[macro_export]
macro_rules! dispatch_replay {
    ($storage_type:ident, $replay:ident, $local_storages:ident, $table_id:expr) => {
        match $storage_type {
            StorageType::Global => $replay,
            StorageType::Local(_) => {
                if let Entry::Vacant(e) = $local_storages.entry($table_id) {
                    e.insert($replay.new_local($table_id).await)
                } else {
                    $local_storages.get(&$table_id).unwrap()
                }
            }
        }
    };
}
