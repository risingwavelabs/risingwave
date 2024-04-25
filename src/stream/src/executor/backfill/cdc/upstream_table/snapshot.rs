// Copyright 2024 RisingWave Labs
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

use std::future::Future;

use futures::{pin_mut, Stream};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::source::cdc::external::{CdcOffset, ExternalTableReader};

use super::external::ExternalStorageTable;
use crate::executor::backfill::utils::{get_new_pos, iter_chunks};
use crate::executor::{StreamExecutorError, StreamExecutorResult, INVALID_EPOCH};

pub trait UpstreamTableRead {
    fn snapshot_read_full_table(
        &self,
        args: SnapshotReadArgs,
        batch_size: u32,
    ) -> impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send + '_;

    fn current_cdc_offset(
        &self,
    ) -> impl Future<Output = StreamExecutorResult<Option<CdcOffset>>> + Send + '_;
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotReadArgs {
    pub epoch: u64,
    pub current_pos: Option<OwnedRow>,
    pub ordered: bool,
    pub chunk_size: usize,
    pub pk_in_output_indices: Vec<usize>,
}

impl SnapshotReadArgs {
    pub fn new(
        current_pos: Option<OwnedRow>,
        chunk_size: usize,
        pk_in_output_indices: Vec<usize>,
    ) -> Self {
        Self {
            epoch: INVALID_EPOCH,
            current_pos,
            ordered: false,
            chunk_size,
            pk_in_output_indices,
        }
    }
}

/// A wrapper of upstream table for snapshot read
/// because we need to customize the snapshot read for managed upstream table (e.g. mv, index)
/// and external upstream table.
pub struct UpstreamTableReader<T> {
    inner: T,
}

impl<T> UpstreamTableReader<T> {
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn new(table: T) -> Self {
        Self { inner: table }
    }
}

impl UpstreamTableRead for UpstreamTableReader<ExternalStorageTable> {
    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read_full_table(&self, read_args: SnapshotReadArgs, batch_size: u32) {
        let primary_keys = self
            .inner
            .pk_indices()
            .iter()
            .map(|idx| {
                let f = &self.inner.schema().fields[*idx];
                f.name.clone()
            })
            .collect_vec();

        let mut read_args = read_args;
        loop {
            tracing::debug!(
                "snapshot_read primary keys: {:?}, current_pos: {:?}",
                primary_keys,
                read_args.current_pos
            );
            let mut read_count: usize = 0;
            let row_stream = self.inner.table_reader().snapshot_read(
                self.inner.schema_table_name(),
                read_args.current_pos.clone(),
                primary_keys.clone(),
                batch_size,
            );

            pin_mut!(row_stream);
            let mut builder =
                DataChunkBuilder::new(self.inner.schema().data_types(), read_args.chunk_size);
            let chunk_stream = iter_chunks(row_stream, &mut builder);
            let mut current_pk_pos = read_args.current_pos.clone().unwrap_or_default();

            #[for_await]
            for chunk in chunk_stream {
                let chunk = chunk?;
                read_count += chunk.cardinality();
                current_pk_pos = get_new_pos(&chunk, &read_args.pk_in_output_indices);
                yield Some(chunk);
            }

            // check read_count if the snapshot batch is finished
            if read_count < batch_size as _ {
                tracing::debug!("finished loading of full table snapshot");
                yield None;
                unreachable!()
            } else {
                // update PK position and continue to read the table
                read_args.current_pos = Some(current_pk_pos);
            }
        }
    }

    async fn current_cdc_offset(&self) -> StreamExecutorResult<Option<CdcOffset>> {
        let binlog = self.inner.table_reader().current_cdc_offset();
        let binlog = binlog.await?;
        Ok(Some(binlog))
    }
}
#[cfg(test)]
mod tests {

    use futures::pin_mut;
    use futures_async_stream::{for_await, try_stream};
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_connector::source::cdc::external::{
        ExternalTableReader, MySqlExternalTableReader, SchemaTableName,
    };

    use crate::executor::backfill::utils::{get_new_pos, iter_chunks};

    #[ignore]
    #[tokio::test]
    async fn test_mysql_table_reader() {
        let columns = vec![
            ColumnDesc::named("o_orderkey", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("o_custkey", ColumnId::new(2), DataType::Int64),
            ColumnDesc::named("o_orderstatus", ColumnId::new(3), DataType::Varchar),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };
        let props = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8306",
                "username" => "root",
                "password" => "123456",
                "database.name" => "mydb",
                "table.name" => "orders_rw"));

        let reader = MySqlExternalTableReader::new(props, rw_schema.clone())
            .await
            .unwrap();

        let mut cnt: usize = 0;
        let mut start_pk = Some(OwnedRow::new(vec![Some(ScalarImpl::Int64(0))]));
        loop {
            let row_stream = reader.snapshot_read(
                SchemaTableName {
                    schema_name: "mydb".to_string(),
                    table_name: "orders_rw".to_string(),
                },
                start_pk.clone(),
                vec!["o_orderkey".to_string()],
                1000,
            );
            let mut builder = DataChunkBuilder::new(rw_schema.clone().data_types(), 256);
            let chunk_stream = iter_chunks(row_stream, &mut builder);
            let pk_indices = vec![0];
            pin_mut!(chunk_stream);
            #[for_await]
            for chunk in chunk_stream {
                let chunk = chunk.expect("data");
                start_pk = Some(get_new_pos(&chunk, &pk_indices));
                cnt += chunk.capacity();
                // println!("chunk: {:#?}", chunk);
                println!("cnt: {}", cnt);
            }
            if cnt >= 1499900 {
                println!("bye!");
                break;
            }
        }
    }
}
