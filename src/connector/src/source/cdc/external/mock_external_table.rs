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

use std::sync::atomic::AtomicUsize;

use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use futures_async_stream::try_stream;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::ScalarImpl;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption, ExternalTableReader, MySqlOffset,
    SchemaTableName,
};
#[derive(Debug)]
pub struct MockExternalTableReader {
    binlog_watermarks: Vec<MySqlOffset>,
    snapshot_cnt: AtomicUsize,
}

impl MockExternalTableReader {
    pub fn new() -> Self {
        let binlog_file = String::from("1.binlog");
        // mock binlog watermarks for backfill
        // initial low watermark: 1.binlog, pos=2 and expected behaviors:
        // - ignore events before (1.binlog, pos=2);
        // - apply events in the range of (1.binlog, pos=2, 1.binlog, pos=4) to the snapshot
        let binlog_watermarks = vec![
            MySqlOffset::new(binlog_file.clone(), 2), // binlog low watermark
            MySqlOffset::new(binlog_file.clone(), 4),
            MySqlOffset::new(binlog_file.clone(), 6),
            MySqlOffset::new(binlog_file.clone(), 8),
            MySqlOffset::new(binlog_file.clone(), 10),
        ];
        Self {
            binlog_watermarks,
            snapshot_cnt: AtomicUsize::new(0),
        }
    }

    pub fn get_normalized_table_name(_table_name: &SchemaTableName) -> String {
        "`mock_table`".to_owned()
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::MySql(MySqlOffset::parse_debezium_offset(
                offset,
            )?))
        })
    }

    /// The snapshot will emit to downstream all in once, because it is too small.
    /// After that we will emit the buffered upstream chunks all in one.
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(&self) {
        let snap_idx = self
            .snapshot_cnt
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        println!("snapshot read: idx {}", snap_idx);

        let snap0 = vec![
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(1)),
                Some(ScalarImpl::Float64(1.0001.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(1)),
                Some(ScalarImpl::Float64(11.00.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(2)),
                Some(ScalarImpl::Float64(22.00.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(5)),
                Some(ScalarImpl::Float64(1.0005.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(6)),
                Some(ScalarImpl::Float64(1.0006.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(8)),
                Some(ScalarImpl::Float64(1.0008.into())),
            ]),
        ];

        let snapshots = [snap0];
        if snap_idx >= snapshots.len() {
            return Ok(());
        }

        for row in &snapshots[snap_idx] {
            yield row.clone();
        }
    }
}

impl ExternalTableReader for MockExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        static IDX: AtomicUsize = AtomicUsize::new(0);

        let idx = IDX.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if idx < self.binlog_watermarks.len() {
            Ok(CdcOffset::MySql(self.binlog_watermarks[idx].clone()))
        } else {
            Ok(CdcOffset::MySql(MySqlOffset {
                filename: "1.binlog".to_owned(),
                position: u64::MAX,
            }))
        }
    }

    fn snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _start_pk: Option<OwnedRow>,
        _primary_keys: Vec<String>,
        _limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner()
    }

    fn get_parallel_cdc_splits(
        &self,
        _options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        // Mock doesn't support parallelized backfill.
        stream::empty::<ConnectorResult<CdcTableSnapshotSplit>>().boxed()
    }
}
