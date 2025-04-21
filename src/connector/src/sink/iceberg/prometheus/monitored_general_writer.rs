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
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use risingwave_common::array::arrow::arrow_array_iceberg::RecordBatch;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};

#[derive(Clone)]
pub struct MonitoredGeneralWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
    write_qps: LabelGuardedIntCounter,
    write_latency: LabelGuardedHistogram,
}

impl<B: IcebergWriterBuilder> MonitoredGeneralWriterBuilder<B> {
    pub fn new(
        inner: B,
        write_qps: LabelGuardedIntCounter,
        write_latency: LabelGuardedHistogram,
    ) -> Self {
        Self {
            inner,
            write_qps,
            write_latency,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for MonitoredGeneralWriterBuilder<B> {
    type R = MonitoredGeneralWriter<B::R>;

    async fn build(self) -> Result<MonitoredGeneralWriter<B::R>> {
        let inner = self.inner.build().await?;
        Ok(MonitoredGeneralWriter {
            inner,
            write_qps: self.write_qps,
            write_latency: self.write_latency,
        })
    }
}

pub struct MonitoredGeneralWriter<F: IcebergWriter> {
    inner: F,
    write_qps: LabelGuardedIntCounter,
    write_latency: LabelGuardedHistogram,
}

#[async_trait::async_trait]
impl<F: IcebergWriter> IcebergWriter for MonitoredGeneralWriter<F> {
    async fn write(&mut self, record: RecordBatch) -> Result<()> {
        self.write_qps.inc();
        let _timer = self.write_latency.start_timer();
        self.inner.write(record).await
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        self.inner.close().await
    }
}
