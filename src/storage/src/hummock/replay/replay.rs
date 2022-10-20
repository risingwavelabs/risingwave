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

use std::fs::File;
use std::path::Path;

use risingwave_hummock_trace::{HummockReplay, Result, TraceReaderImpl};

use super::ReplayHummock;
use crate::hummock::HummockStorage;

async fn run_replay(path: &Path) -> Result<()> {
    let f = File::open(path)?;
    let reader = TraceReaderImpl::new(f)?;
    let replay_object = create_hummock().await.expect("fail to create hummock");
    let replay_object = Box::new(ReplayHummock::new(replay_object));
    let (mut replay, handle) = HummockReplay::new(reader, replay_object);

    replay.run().unwrap();

    handle.await.expect("fail to wait replaying thread");
    Ok(())
}

async fn create_hummock() -> Result<HummockStorage> {
    // HummockStorage::new(options, sstable_store, hummock_meta_client, notification_client, stats,
    // compaction_group_client).await.expect("fail to start hummock")
    todo!()
}
