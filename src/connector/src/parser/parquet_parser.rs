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

/// Parser for CSV format
#[derive(Debug)]
pub struct ParquetParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl ParquetParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        csv_props: CsvProperties,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            rw_columns,

            source_ctx,
        })
    }

    pub fn record_batch_into_stream(
        self,
        record_batch_stream: std::pin::Pin<
            Box<parquet::arrow::async_reader::ParquetRecordBatchStream<Reader>>,
        >,
    ) -> ParsedStreamImpl {
        todo!()
    }
}
