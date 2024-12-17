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

use risingwave_common::try_match_expand;

use super::unified::bytes::BytesAccess;
use super::unified::AccessImpl;
use super::{AccessBuilder, EncodingProperties};
use crate::error::ConnectorResult;

#[derive(Debug)]
pub struct BytesAccessBuilder {
    column_name: Option<String>,
}

impl AccessBuilder for BytesAccessBuilder {
    #[allow(clippy::unused_async)]
    async fn generate_accessor(&mut self, payload: Vec<u8>) -> ConnectorResult<AccessImpl<'_>> {
        Ok(AccessImpl::Bytes(BytesAccess::new(
            &self.column_name,
            payload,
        )))
    }
}

impl BytesAccessBuilder {
    pub fn new(encoding_properties: EncodingProperties) -> ConnectorResult<Self> {
        let config = try_match_expand!(encoding_properties, EncodingProperties::Bytes)?;
        Ok(Self {
            column_name: config.column_name,
        })
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::Op;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};

    use crate::parser::plain_parser::PlainParser;
    use crate::parser::{
        BytesProperties, EncodingProperties, ProtocolProperties, SourceColumnDesc,
        SourceStreamChunkBuilder, SpecificParserConfig,
    };
    use crate::source::{SourceContext, SourceCtrlOpts};

    fn get_payload() -> Vec<Vec<u8>> {
        vec![br#"t"#.to_vec(), br#"random"#.to_vec()]
    }

    async fn test_bytes_parser(get_payload: fn() -> Vec<Vec<u8>>) {
        let descs = vec![SourceColumnDesc::simple("id", DataType::Bytea, 0.into())];
        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Bytes(BytesProperties { column_name: None }),
            protocol_config: ProtocolProperties::Plain,
        };
        let mut parser = PlainParser::new(props, descs.clone(), SourceContext::dummy().into())
            .await
            .unwrap();

        let mut builder = SourceStreamChunkBuilder::new(descs, SourceCtrlOpts::for_test());

        for payload in get_payload() {
            let writer = builder.row_writer();
            parser
                .parse_inner(None, Some(payload), writer)
                .await
                .unwrap();
        }

        builder.finish_current_chunk();
        let chunk = builder.consume_ready_chunks().next().unwrap();
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
