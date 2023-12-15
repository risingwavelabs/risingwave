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

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use icelake::io_v2::{IcebergWriter, IcebergWriterBuilder};
use icelake::Result;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};

#[derive(Clone)]
pub struct MonitoredWriteWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
    write_qps: LabelGuardedIntCounter<2>,
    write_latency: LabelGuardedHistogram<2>,
}

impl<B: IcebergWriterBuilder> MonitoredWriteWriterBuilder<B> {
    /// Create writer context.
    pub fn new(
        inner: B,
        write_qps: LabelGuardedIntCounter<2>,
        write_latency: LabelGuardedHistogram<2>,
    ) -> Self {
        Self {
            inner,
            write_qps,
            write_latency,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for MonitoredWriteWriterBuilder<B> {
    type R = MonitoredWriteWriter<B::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let appender = self.inner.build(schema).await?;
        Ok(MonitoredWriteWriter {
            appender,
            write_qps: self.write_qps,
            write_latency: self.write_latency,
        })
    }
}

pub struct MonitoredWriteWriter<F: IcebergWriter> {
    appender: F,
    write_qps: LabelGuardedIntCounter<2>,
    write_latency: LabelGuardedHistogram<2>,
}

#[async_trait]
impl<F: IcebergWriter> IcebergWriter for MonitoredWriteWriter<F> {
    type R = F::R;

    async fn write(&mut self, record: RecordBatch) -> Result<()> {
        self.write_qps.inc();
        let _timer = self.write_latency.start_timer();
        self.appender.write(record).await
    }

    async fn flush(&mut self) -> Result<Vec<Self::R>> {
        self.appender.flush().await
    }
}
