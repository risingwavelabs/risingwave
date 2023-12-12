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
use icelake::io_v2::{
    BaseFileWriter, BaseFileWriterBuilder, BaseFileWriterMetrics, CurrentFileStatus, FileWriter,
    FileWriterBuilder,
};
use icelake::Result;
use risingwave_common::metrics::LabelGuardedIntGauge;

#[derive(Clone)]
pub struct MonitoredBaseFileWriterBuilder<B: FileWriterBuilder> {
    inner: BaseFileWriterBuilder<B>,
    // metrics
    unflush_data_file: LabelGuardedIntGauge<2>,
}

impl<B: FileWriterBuilder> MonitoredBaseFileWriterBuilder<B> {
    pub fn new(
        inner: BaseFileWriterBuilder<B>,
        unflush_data_file: LabelGuardedIntGauge<2>,
    ) -> Self {
        Self {
            inner,
            unflush_data_file,
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> FileWriterBuilder for MonitoredBaseFileWriterBuilder<B> {
    type R = MonitoredBaseFileWriter<B>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        Ok(MonitoredBaseFileWriter {
            inner: self.inner.build(schema).await?,
            unflush_data_file: self.unflush_data_file,
            cur_metrics: BaseFileWriterMetrics {
                unflush_data_file: 0,
            },
        })
    }
}

pub struct MonitoredBaseFileWriter<B: FileWriterBuilder> {
    inner: BaseFileWriter<B>,

    // metrics
    unflush_data_file: LabelGuardedIntGauge<2>,

    cur_metrics: BaseFileWriterMetrics,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> FileWriter for MonitoredBaseFileWriter<B> {
    type R = <<B as FileWriterBuilder>::R as FileWriter>::R;

    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch).await?;
        let last_metrics = std::mem::replace(&mut self.cur_metrics, self.inner.metrics());
        {
            let delta =
                (self.cur_metrics.unflush_data_file - last_metrics.unflush_data_file) as i64;
            assert!(delta >= 0);
            self.unflush_data_file.add(delta);
        }
        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    async fn close(self) -> Result<Vec<Self::R>> {
        let res = self.inner.close().await?;
        let delta = (res.len() - self.cur_metrics.unflush_data_file) as i64;
        assert!(delta >= 0);
        self.unflush_data_file.add(delta);
        Ok(res)
    }
}

impl<B: FileWriterBuilder> CurrentFileStatus for MonitoredBaseFileWriter<B> {
    fn current_file_path(&self) -> String {
        self.inner.current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner.current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner.current_written_size()
    }
}
