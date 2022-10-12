use std::fs::File;
use std::io::BufReader;

use anyhow::Result;
use risingwave_hummock_trace::{HummockReplay, TraceReaderImpl};

#[tokio::main]
async fn main() -> Result<()> {
    let f = File::open("hummock.trace")?;
    let reader = TraceReaderImpl::new(BufReader::new(f))?;
    let (mut replay, join) = HummockReplay::new(reader);
    replay.run()?;
    join.await.unwrap();
    Ok(())
}
