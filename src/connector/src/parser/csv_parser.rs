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

use std::collections::HashMap;
use std::str::FromStr;

use anyhow::anyhow;
use futures_async_stream::try_stream;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_expr::vector_op::cast::{
    str_to_date, str_to_timestamp, str_with_time_zone_to_timestamptz,
};

use crate::parser::{
    BoxSourceWithStateStream, ByteStreamSourceParser, SourceColumnDesc, SourceStreamChunkBuilder,
    SourceStreamChunkRowWriter, StreamChunkWithState, WriteGuard,
};
use crate::source::{BoxSourceStream, SourceContextRef, SplitId};

macro_rules! to_rust_type {
    ($v:ident, $t:ty) => {
        $v.parse::<$t>()
            .map_err(|_| anyhow!("failed parse {} from {}", stringify!($t), $v))?
    };
}

#[derive(Debug, Clone)]
pub struct CsvParserConfig {
    pub delimiter: u8,
    pub has_header: bool,
}

#[derive(Debug)]
pub struct CsvParser {
    rw_columns: Vec<SourceColumnDesc>,
    next_row_is_header: bool,
    csv_reader: csv_core::Reader,
    // buffers for parse
    output: Vec<u8>,
    output_cursor: usize,
    ends: Vec<usize>,
    ends_cursor: usize,
    source_ctx: SourceContextRef,
}

impl CsvParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        parser_config: CsvParserConfig,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        let CsvParserConfig {
            delimiter,
            has_header,
        } = parser_config;

        Ok(Self {
            rw_columns,
            next_row_is_header: has_header,
            csv_reader: csv_core::ReaderBuilder::new().delimiter(delimiter).build(),
            output: vec![0],
            output_cursor: 0,
            ends: vec![0],
            ends_cursor: 1,
            source_ctx,
        })
    }

    fn reset_cursor(&mut self) {
        self.output_cursor = 0;
        self.ends_cursor = 1;
    }

    pub fn parse_columns_to_strings(&mut self, chunk: &mut &[u8]) -> Result<Option<Vec<String>>> {
        loop {
            let (result, n_input, n_output, n_ends) = self.csv_reader.read_record(
                chunk,
                &mut self.output[self.output_cursor..],
                &mut self.ends[self.ends_cursor..],
            );
            self.output_cursor += n_output;
            *chunk = &(*chunk)[n_input..];
            self.ends_cursor += n_ends;
            match result {
                // input empty, here means the `chunk` passed to this method
                // doesn't contain a whole record, need more bytes
                csv_core::ReadRecordResult::InputEmpty => break Ok(None),
                // the output buffer is not enough
                csv_core::ReadRecordResult::OutputFull => {
                    let length = self.output.len();
                    self.output.resize(length * 2, 0);
                }
                // the ends buffer is not enough
                csv_core::ReadRecordResult::OutputEndsFull => {
                    let length = self.ends.len();
                    self.ends.resize(length * 2, 0);
                }
                // Success cases
                csv_core::ReadRecordResult::Record | csv_core::ReadRecordResult::End => {
                    // skip the header
                    if self.next_row_is_header {
                        self.next_row_is_header = false;
                        self.reset_cursor();
                        continue;
                    }
                    let ends_cursor = self.ends_cursor;
                    // caller provides an empty chunk, and there is no data
                    // in inner buffer
                    if ends_cursor <= 1 {
                        break Ok(None);
                    }
                    self.reset_cursor();

                    let string_columns = (1..ends_cursor)
                        .map(|culomn| {
                            String::from_utf8(
                                self.output[self.ends[culomn - 1]..self.ends[culomn]].to_owned(),
                            )
                            .map_err(|e| {
                                RwError::from(InternalError(format!(
                                    "Parse csv column {} error: invalid UTF-8 ({})",
                                    culomn, e,
                                )))
                            })
                        })
                        .collect::<Result<Vec<String>>>()?;
                    break Ok(Some(string_columns));
                }
            }
        }
    }

    fn try_parse_single_record(
        &mut self,
        payload: &mut &[u8],
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<Option<WriteGuard>> {
        let columns_string = match self.parse_columns_to_strings(payload)? {
            None => return Ok(None),
            Some(strings) => strings,
        };
        writer
            .insert(move |desc| {
                // column_id is 1-based
                let column_id = desc.column_id.get_id() - 1;
                let column_type = &desc.data_type;
                let v = match columns_string.get(column_id as usize) {
                    Some(v) => v.to_owned(),
                    None => return Ok(None),
                };
                parse_string(column_type, v)
            })
            .map(Some)
    }

    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    async fn into_stream(mut self, data_stream: BoxSourceStream) {
        // the remain length of the last seen message
        let mut remain_len = 0;
        // current offset
        let mut offset = 0;
        // split id of current data stream
        let mut split_id = None;
        #[for_await]
        for batch in data_stream {
            let batch = batch?;

            let mut builder =
                SourceStreamChunkBuilder::with_capacity(self.rw_columns.clone(), batch.len() * 2);
            let mut split_offset_mapping: HashMap<SplitId, String> = HashMap::new();

            for msg in batch {
                if let Some(content) = msg.payload {
                    if split_id.is_none() {
                        split_id = Some(msg.split_id.clone());
                    }

                    offset = msg.offset.parse().unwrap();
                    let mut buff = content.as_ref();

                    remain_len = buff.len();
                    loop {
                        match self.try_parse_single_record(&mut buff, builder.row_writer()) {
                            Err(e) => {
                                tracing::warn!(
                                    "message parsing failed {}, skipping",
                                    e.to_string()
                                );
                                continue;
                            }
                            Ok(None) => {
                                break;
                            }
                            Ok(Some(_)) => {
                                let consumed = remain_len - buff.len();
                                offset += consumed;
                                remain_len = buff.len();
                            }
                        }
                    }
                    split_offset_mapping.insert(msg.split_id, offset.to_string());
                }
            }
            yield StreamChunkWithState {
                chunk: builder.finish(),
                split_offset_mapping: Some(split_offset_mapping),
            };
        }
        // The file may be missing the last terminator,
        // so we need to pass an empty payload to inform the parser.
        if remain_len > 0 {
            let mut builder = SourceStreamChunkBuilder::with_capacity(self.rw_columns.clone(), 1);
            let mut split_offset_mapping: HashMap<SplitId, String> = HashMap::new();
            let empty = vec![];
            match self.try_parse_single_record(&mut empty.as_ref(), builder.row_writer()) {
                Err(e) => {
                    tracing::warn!("message parsing failed {}, skipping", e.to_string());
                }
                Ok(Some(_)) => {
                    split_offset_mapping
                        .insert(split_id.unwrap(), (offset + remain_len).to_string());
                    yield StreamChunkWithState {
                        chunk: builder.finish(),
                        split_offset_mapping: Some(split_offset_mapping),
                    };
                }
                _ => {}
            }
        }
    }
}

impl ByteStreamSourceParser for CsvParser {
    fn into_stream(self, msg_stream: BoxSourceStream) -> BoxSourceWithStateStream {
        self.into_stream(msg_stream)
    }
}

#[inline]
fn parse_string(dtype: &DataType, v: String) -> Result<Datum> {
    let v = match dtype {
        // mysql use tinyint to represent boolean
        DataType::Boolean => ScalarImpl::Bool(to_rust_type!(v, i16) != 0),
        DataType::Int16 => ScalarImpl::Int16(to_rust_type!(v, i16)),
        DataType::Int32 => ScalarImpl::Int32(to_rust_type!(v, i32)),
        DataType::Int64 => ScalarImpl::Int64(to_rust_type!(v, i64)),
        DataType::Float32 => ScalarImpl::Float32(to_rust_type!(v, f32).into()),
        DataType::Float64 => ScalarImpl::Float64(to_rust_type!(v, f64).into()),
        // FIXME: decimal should have more precision than f64
        DataType::Decimal => Decimal::from_str(v.as_str())
            .map_err(|_| anyhow!("parse decimal from string err {}", v))?
            .into(),
        DataType::Varchar => v.into(),
        DataType::Date => str_to_date(v.as_str())?.into(),
        DataType::Time => str_to_date(v.as_str())?.into(),
        DataType::Timestamp => str_to_timestamp(v.as_str())?.into(),
        DataType::Timestamptz => str_with_time_zone_to_timestamptz(v.as_str())?.into(),
        _ => {
            return Err(RwError::from(InternalError(format!(
                "CSV data source not support type {}",
                dtype
            ))))
        }
    };
    Ok(Some(v))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_async_stream::for_await;

    use crate::source::{SourceMessage, SourceMeta};

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn prepare_data(data: Vec<u8>) {
        let mid = data.len() / 2;
        let part1 = data[..mid].to_vec();
        let part2 = data[mid..].to_vec();
        let id = "split1".into();
        let part1_len = part1.len();
        let msg1 = SourceMessage {
            payload: Some(part1.into()),
            offset: 0.to_string(),
            split_id: Arc::clone(&id),
            meta: SourceMeta::Empty,
        };
        let msg2 = SourceMessage {
            payload: Some(part2.into()),
            offset: part1_len.to_string(),
            split_id: Arc::clone(&id),
            meta: SourceMeta::Empty,
        };

        yield vec![msg1, msg2];
    }

    use super::*;
    #[ignore]
    #[tokio::test]
    async fn test_csv_parser_without_last_line_break() {
        let descs = vec![
            SourceColumnDesc::simple("name", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("age", DataType::Int32, 2.into()),
        ];

        let config = CsvParserConfig {
            delimiter: b',',
            has_header: true,
        };
        let parser = CsvParser::new(descs, config, Default::default()).unwrap();
        let data = b"
name,age
pite,20
alex,10";
        let data_stream = prepare_data(data.to_vec());
        let msg_stream = parser.into_stream(data_stream);
        #[for_await]
        for msg in msg_stream {
            println!("{:?}", msg);
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_csv_parser_with_last_line_break() {
        let descs = vec![
            SourceColumnDesc::simple("name", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("age", DataType::Int32, 2.into()),
        ];

        let config = CsvParserConfig {
            delimiter: b',',
            has_header: true,
        };
        let parser = CsvParser::new(descs, config, Default::default()).unwrap();
        let data = b"
name,age
pite,20
alex,10
";
        println!("data len: {}", data.len());
        let data_stream = prepare_data(data.to_vec());
        let msg_stream = parser.into_stream(data_stream);
        #[for_await]
        for msg in msg_stream {
            println!("{:?}", msg);
        }
    }
}
