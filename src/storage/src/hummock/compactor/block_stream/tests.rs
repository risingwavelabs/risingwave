// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;

use bytes::Bytes;
use risingwave_object_store::object::{MonitoredStreamingReader, ObjectError, ObjectResult};

use super::SstableBlockStream;
use crate::hummock::HummockValue;
use crate::hummock::block_stream::BlockDataStream;
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::test_utils::{
    default_builder_opt_for_test, default_writer_opt_for_test, gen_test_sstable_data, put_sst,
    test_key_of, test_value_of,
};
use crate::monitor::{ObjectStoreMetrics, StoreLocalStatistic};

async fn test_stream(max_io_retry_times: usize) -> (SstableBlockStream, Bytes) {
    let store = mock_sstable_store().await;
    let mut options = default_builder_opt_for_test();
    options.block_capacity = 128;
    let (data, meta) = gen_test_sstable_data(
        options,
        (0..100).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
    )
    .await;
    assert!(meta.block_metas.len() > 5);
    let info = put_sst(
        0,
        data.clone(),
        meta,
        store.clone(),
        default_writer_opt_for_test(),
        vec![0],
    )
    .await
    .unwrap();
    let table = store
        .sstable(&info, &mut StoreLocalStatistic::default())
        .await
        .unwrap();
    // Both ends exclude physical SST blocks, so recovery must preserve the selected range.
    (
        SstableBlockStream::new(table, 1..5, info, store, max_io_retry_times),
        data,
    )
}

fn inject_stream(stream: &mut SstableBlockStream, packets: Vec<ObjectResult<Bytes>>) {
    let reader = MonitoredStreamingReader::new(
        "test",
        Box::pin(futures::stream::iter(packets)),
        Arc::new(ObjectStoreMetrics::unused()),
        None,
    );
    stream.block_stream = Some(BlockDataStream::new(
        reader,
        &stream.sstable.meta.block_metas[stream.remaining_blocks.clone()],
    ));
}

async fn assert_next_block(stream: &mut SstableBlockStream, original: &Bytes, index: usize) {
    let meta = stream.sstable.meta.block_metas[index].clone();
    let (data, uncompressed_size) = stream.next_block().await.unwrap().unwrap();
    assert_eq!(
        data,
        original.slice(meta.offset as usize..(meta.offset + meta.len) as usize)
    );
    assert_eq!(uncompressed_size, meta.uncompressed_size as usize);
    assert_eq!(stream.next_block_index(), index + 1);
}

#[tokio::test]
async fn test_compactor_block_stream_partial_read() {
    for unexpected_eof in [false, true] {
        let (mut stream, original) = test_stream(1).await;
        let first = &stream.sstable.meta.block_metas[1];
        let second = &stream.sstable.meta.block_metas[2];
        let cutoff = second.offset as usize + second.len as usize / 2;
        let mut packets = vec![Ok(original.slice(first.offset as usize..cutoff))];
        if !unexpected_eof {
            packets.push(Err(ObjectError::internal("injected after partial block")));
        }
        inject_stream(&mut stream, packets);
        assert_next_block(&mut stream, &original, 1).await;
        assert_eq!(stream.io_retry_times, 0);
        // Block 2 begins in the buffered packet. Its remaining bytes fail to arrive, so it
        // must be reread in full from the backing object, with no duplicate or skipped block.
        for index in 2..5 {
            assert_next_block(&mut stream, &original, index).await;
        }
        assert_eq!(stream.io_retry_times, 1);
        assert!(stream.next_block().await.unwrap().is_none());
        assert!(stream.next_block().await.unwrap().is_none());
        assert_eq!(stream.next_block_index(), 5);
    }
}

#[tokio::test]
async fn test_compactor_block_stream_retry_budget() {
    for budget in [0, 1, 2] {
        let (mut stream, original) = test_stream(budget).await;
        assert_next_block(&mut stream, &original, 1).await;
        for attempt in 0..budget {
            inject_stream(
                &mut stream,
                vec![Err(ObjectError::internal("injected read failure"))],
            );
            assert_next_block(&mut stream, &original, 2 + attempt).await;
            assert_eq!(stream.io_retry_times, attempt + 1);
        }
        let failed_index = stream.next_block_index();
        inject_stream(
            &mut stream,
            vec![Err(ObjectError::internal("retry budget exhausted"))],
        );
        assert!(stream.next_block().await.unwrap_err().is_object_error());
        assert_eq!(stream.next_block_index(), failed_index);
        assert_eq!(stream.io_retry_times, budget);
    }
}
