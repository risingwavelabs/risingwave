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
use iceberg::writer::base_writer::sort_position_delete_writer::{
    PositionDeleteInput, SortPositionDeleteWriter, SortPositionDeleteWriterBuilder,
};
use iceberg::writer::file_writer::FileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use risingwave_common::metrics::LabelGuardedIntGauge;

#[derive(Clone)]
pub struct MonitoredPositionDeleteWriterBuilder<B: FileWriterBuilder> {
    cache_row_metrics: LabelGuardedIntGauge,
    inner: SortPositionDeleteWriterBuilder<B>,
}

impl<B: FileWriterBuilder> MonitoredPositionDeleteWriterBuilder<B> {
    pub fn new(
        inner: SortPositionDeleteWriterBuilder<B>,
        cache_row_metrics: LabelGuardedIntGauge,
    ) -> Self {
        Self {
            cache_row_metrics,
            inner,
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder<PositionDeleteInput>
    for MonitoredPositionDeleteWriterBuilder<B>
{
    type R = MonitoredPositionDeleteWriter<B>;

    async fn build(self) -> Result<Self::R> {
        let writer = self.inner.build().await?;
        Ok(MonitoredPositionDeleteWriter {
            writer,
            cache_row_metrics: self.cache_row_metrics,
            last_cache_row: 0,
        })
    }
}

pub struct MonitoredPositionDeleteWriter<B: FileWriterBuilder> {
    writer: SortPositionDeleteWriter<B>,

    // metrics
    cache_row_metrics: LabelGuardedIntGauge,
    last_cache_row: usize,
}

impl<B: FileWriterBuilder> MonitoredPositionDeleteWriter<B> {
    fn update_metrics(&mut self) {
        self.cache_row_metrics
            .add(self.writer.current_cache_number() as i64 - self.last_cache_row as i64);
        self.last_cache_row = self.writer.current_cache_number();
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter<PositionDeleteInput> for MonitoredPositionDeleteWriter<B> {
    async fn write(&mut self, input: PositionDeleteInput) -> Result<()> {
        self.writer.write(input).await?;
        self.update_metrics();
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        self.update_metrics();
        let res = self.writer.close().await?;
        Ok(res)
    }
}
