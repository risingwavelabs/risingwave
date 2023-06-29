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

use risingwave_common::error::Result;

use super::unified::bytes::{BytesAccess, BytesChangeEvent};
use super::unified::ChangeEvent;
use super::{ByteStreamSourceParser, SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

/// Parser for BYTES format
#[derive(Debug)]
pub struct BytesParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl BytesParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, source_ctx: SourceContextRef) -> Result<Self> {
        debug_assert_eq!(
            1,
            rw_columns
                .iter()
                .fold(0, |cnt, col| cnt + if col.is_visible() { 1 } else { 0 })
        );
        Ok(Self {
            rw_columns,
            source_ctx,
        })
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let accessor = BytesChangeEvent::with_value(BytesAccess::new(payload));
        writer.insert(|column| {
            let res = accessor.access_field(&column.name, &column.data_type)?;
            tracing::trace!(
                "inserted {:?} {:?} is_pk:{:?} {:?} ",
                &column.name,
                &column.data_type,
                &column.is_pk,
                res
            );
            Ok(res)
        })
    }
}

impl ByteStreamSourceParser for BytesParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    async fn parse_one<'a>(
        &'a mut self,
        payload: Vec<u8>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        self.parse_inner(payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Op;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};

    use crate::parser::{BytesParser, SourceColumnDesc, SourceStreamChunkBuilder};

    fn get_payload() -> Vec<Vec<u8>> {
        vec![br#"t"#.to_vec(), br#"random"#.to_vec()]
    }

    async fn test_bytes_parser(get_payload: fn() -> Vec<Vec<u8>>) {
        let descs = vec![SourceColumnDesc::simple("id", DataType::Bytea, 0.into())];
        let parser = BytesParser::new(descs.clone(), Default::default()).unwrap();

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 2);

        for payload in get_payload() {
            let writer = builder.row_writer();
            parser.parse_inner(payload, writer).await.unwrap();
        }

        let chunk = builder.finish();
        let mut rows = chunk.rows();
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                Some(ScalarImpl::Bytea("t".as_bytes().into()))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                Some(ScalarImpl::Bytea("random".as_bytes().into()))
            );
        }
    }

    #[tokio::test]
    async fn test_bytes_parse_object_top_level() {
        test_bytes_parser(get_payload).await;
    }
}
