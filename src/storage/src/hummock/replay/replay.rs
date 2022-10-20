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
