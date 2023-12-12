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

use icelake::io_v2::{
    FileWriterBuilder, IcebergWriter, IcebergWriterBuilder, PositionDeleteInput,
    PositionDeleteMetrics, PositionDeleteWriter, PositionDeleteWriterBuilder,
};
use icelake::Result;
use risingwave_common::metrics::LabelGuardedIntGauge;

#[derive(Clone)]
pub struct MonitoredPositionDeleteWriterBuilder<B: FileWriterBuilder> {
    current_cache_number: LabelGuardedIntGauge<2>,
    inner: PositionDeleteWriterBuilder<B>,
}

impl<B: FileWriterBuilder> MonitoredPositionDeleteWriterBuilder<B> {
    pub fn new(
        inner: PositionDeleteWriterBuilder<B>,
        current_cache_number: LabelGuardedIntGauge<2>,
    ) -> Self {
        Self {
            current_cache_number,
            inner,
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder<PositionDeleteInput>
    for MonitoredPositionDeleteWriterBuilder<B>
{
    type R = MonitoredPositionDeleteWriter<B>;

    async fn build(self, schema: &arrow_schema::SchemaRef) -> Result<Self::R> {
        let writer = self.inner.build(schema).await?;
        Ok(MonitoredPositionDeleteWriter {
            writer,
            cache_number: self.current_cache_number,
            current_metrics: PositionDeleteMetrics {
                current_cache_number: 0,
            },
        })
    }
}

pub struct MonitoredPositionDeleteWriter<B: FileWriterBuilder> {
    writer: PositionDeleteWriter<B>,

    // metrics
    cache_number: LabelGuardedIntGauge<2>,
    current_metrics: PositionDeleteMetrics,
}

impl<B: FileWriterBuilder> MonitoredPositionDeleteWriter<B> {
    fn update_metrics(&mut self) -> Result<()> {
        let last_metrics = std::mem::replace(&mut self.current_metrics, self.writer.metrics());
        {
            let delta = self.current_metrics.current_cache_number as i64
                - last_metrics.current_cache_number as i64;
            self.cache_number.add(delta);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter<PositionDeleteInput> for MonitoredPositionDeleteWriter<B> {
    type R = <PositionDeleteWriter<B> as IcebergWriter<PositionDeleteInput>>::R;

    async fn write(&mut self, input: PositionDeleteInput) -> Result<()> {
        self.writer.write(input).await?;
        self.update_metrics()?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<Vec<Self::R>> {
        let res = self.writer.flush().await?;
        self.update_metrics()?;
        Ok(res)
    }
}
