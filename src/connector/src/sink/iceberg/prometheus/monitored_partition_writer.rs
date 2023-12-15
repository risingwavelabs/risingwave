// Copyright 2023 RisingWave Labs
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

use arrow_schema::SchemaRef;
use icelake::io_v2::{
    FanoutPartitionedWriter, FanoutPartitionedWriterBuilder, FanoutPartitionedWriterMetrics,
    IcebergWriter, IcebergWriterBuilder,
};
use icelake::Result;
use risingwave_common::metrics::LabelGuardedIntGauge;

#[derive(Clone)]
pub struct MonitoredFanoutPartitionedWriterBuilder<B: IcebergWriterBuilder> {
    inner: FanoutPartitionedWriterBuilder<B>,
    partition_num: LabelGuardedIntGauge<2>,
}

impl<B: IcebergWriterBuilder> MonitoredFanoutPartitionedWriterBuilder<B> {
    pub fn new(
        inner: FanoutPartitionedWriterBuilder<B>,
        partition_num: LabelGuardedIntGauge<2>,
    ) -> Self {
        Self {
            inner,
            partition_num,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for MonitoredFanoutPartitionedWriterBuilder<B> {
    type R = MonitoredFanoutPartitionedWriter<B>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let writer = self.inner.build(schema).await?;
        Ok(MonitoredFanoutPartitionedWriter {
            inner: writer,
            partition_num: self.partition_num,
            current_metrics: FanoutPartitionedWriterMetrics { partition_num: 0 },
        })
    }
}

pub struct MonitoredFanoutPartitionedWriter<B: IcebergWriterBuilder> {
    inner: FanoutPartitionedWriter<B>,
    partition_num: LabelGuardedIntGauge<2>,
    current_metrics: FanoutPartitionedWriterMetrics,
}

impl<B: IcebergWriterBuilder> MonitoredFanoutPartitionedWriter<B> {
    pub fn update_metrics(&mut self) -> Result<()> {
        let last_metrics = std::mem::replace(&mut self.current_metrics, self.inner.metrics());
        {
            let delta =
                self.current_metrics.partition_num as i64 - last_metrics.partition_num as i64;
            self.partition_num.add(delta);
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for MonitoredFanoutPartitionedWriter<B> {
    type R = <FanoutPartitionedWriter<B> as IcebergWriter>::R;

    async fn write(&mut self, batch: arrow_array::RecordBatch) -> Result<()> {
        self.inner.write(batch).await?;
        self.update_metrics()?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<Vec<Self::R>> {
        let res = self.inner.flush().await?;
        self.update_metrics()?;
        Ok(res)
    }
}
