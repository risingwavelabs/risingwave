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

mod replay_runner;
mod worker;

use std::ops::Bound;

#[cfg(test)]
use mockall::automock;
pub use replay_runner::*;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
pub(crate) use worker::*;

use crate::error::Result;
use crate::Record;

type ReplayGroup = Vec<Record>;

type WorkerResponse = ();

#[derive(Debug)]
pub(crate) enum ReplayRequest {
    Task(ReplayGroup),
    Fin,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
enum WorkerId {
    Actor(u64),
    Executor(u64),
    None(u64),
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait Replayable: Send + Sync {
    async fn sync(&self, id: u64) -> Result<usize>;
    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
    async fn notify_hummock(&self, info: Info, op: RespOperation) -> Result<u64>;
    async fn new_local(&self, table_id: u32) -> Box<dyn LocalReplay>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait LocalReplay: Send + Sync {
    async fn get(
        &self,
        key: Vec<u8>,
        check_bloom_filter: bool,
        epoch: u64,
        prefix_hint: Option<Vec<u8>>,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Result<Option<Vec<u8>>>;
    async fn ingest(
        &self,
        kv_pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        delete_ranges: Vec<(Vec<u8>, Vec<u8>)>,
        epoch: u64,
        table_id: u32,
    ) -> Result<usize>;
    async fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        prefix_hint: Option<Vec<u8>>,
        check_bloom_filter: bool,
        retention_seconds: Option<u32>,
        table_id: u32,
    ) -> Result<Box<dyn ReplayIter>>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayIter: Send + Sync {
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}
