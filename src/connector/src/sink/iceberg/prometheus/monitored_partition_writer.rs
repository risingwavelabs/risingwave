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

use iceberg::Result;
use iceberg::spec::DataFile;
use iceberg::writer::function_writer::fanout_partition_writer::{
    FanoutPartitionWriter, FanoutPartitionWriterBuilder,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use risingwave_common::array::arrow::arrow_array_iceberg;
use risingwave_common::metrics::LabelGuardedIntGauge;

#[derive(Clone)]
pub struct MonitoredFanoutPartitionedWriterBuilder<B: IcebergWriterBuilder> {
    inner: FanoutPartitionWriterBuilder<B>,
    partition_num_metrics: LabelGuardedIntGauge,
}

impl<B: IcebergWriterBuilder> MonitoredFanoutPartitionedWriterBuilder<B> {
    #[expect(dead_code)]
    pub fn new(
        inner: FanoutPartitionWriterBuilder<B>,
        partition_num: LabelGuardedIntGauge,
    ) -> Self {
        Self {
            inner,
            partition_num_metrics: partition_num,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for MonitoredFanoutPartitionedWriterBuilder<B> {
    type R = MonitoredFanoutPartitionedWriter<B>;

    async fn build(self) -> Result<Self::R> {
        let writer = self.inner.build().await?;
        Ok(MonitoredFanoutPartitionedWriter {
            inner: writer,
            partition_num_metrics: self.partition_num_metrics,
            last_partition_num: 0,
        })
    }
}

pub struct MonitoredFanoutPartitionedWriter<B: IcebergWriterBuilder> {
    inner: FanoutPartitionWriter<B>,
    partition_num_metrics: LabelGuardedIntGauge,
    last_partition_num: usize,
}

impl<B: IcebergWriterBuilder> MonitoredFanoutPartitionedWriter<B> {
    pub fn update_metrics(&mut self) {
        let current_partition_num = self.inner.partition_num();
        let delta = current_partition_num as i64 - self.last_partition_num as i64;
        self.partition_num_metrics.add(delta);
        self.last_partition_num = current_partition_num;
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for MonitoredFanoutPartitionedWriter<B> {
    async fn write(&mut self, batch: arrow_array_iceberg::RecordBatch) -> Result<()> {
        self.inner.write(batch).await?;
        self.update_metrics();
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        self.update_metrics();
        let res = self.inner.close().await?;
        Ok(res)
    }
}
