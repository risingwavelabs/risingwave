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

use risingwave_common::catalog::DEFAULT_KEY_COLUMN_NAME;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{ErrorCode, Result, RwError};

use super::bytes_parser::BytesAccessBuilder;
use super::unified::util::apply_key_val_accessor_on_stream_chunk_writer;
use super::{
    AccessBuilderImpl, ByteStreamSourceParser, BytesProperties, EncodingProperties, EncodingType,
    SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::parser::ParserFormat;
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct PlainParser {
    pub key_builder: AccessBuilderImpl,
    pub payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    pub source_ctx: SourceContextRef,
}

impl PlainParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        let key_builder = AccessBuilderImpl::Bytes(BytesAccessBuilder::new(
            EncodingProperties::Bytes(BytesProperties {
                column_name: Some(DEFAULT_KEY_COLUMN_NAME.into()),
            }),
        )?);
        let payload_builder = match props.encoding_config {
            EncodingProperties::Protobuf(_)
            | EncodingProperties::Avro(_)
            | EncodingProperties::Bytes(_) => {
                AccessBuilderImpl::new_default(props.encoding_config, EncodingType::Value).await?
            }
            _ => {
                return Err(RwError::from(ProtocolError(
                    "unsupported encoding for Plain".to_string(),
                )));
            }
        };
        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
        })
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<()> {
        // if key is empty, set it as vec![]
        let key_data = key.unwrap_or_default();
        // if payload is empty, report error
        let payload_data = payload.ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(
                "Empty payload with nonempty key".into(),
            ))
        })?;

        let key_accessor = self.key_builder.generate_accessor(key_data).await?;
        let payload_accessor = self.payload_builder.generate_accessor(payload_data).await?;
        apply_key_val_accessor_on_stream_chunk_writer(
            DEFAULT_KEY_COLUMN_NAME,
            key_accessor,
            payload_accessor,
            &mut writer,
        )
        .map_err(Into::into)
    }
}

impl ByteStreamSourceParser for PlainParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Plain
    }

    async fn parse_one<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<()> {
        self.parse_inner(key, payload, writer).await
    }
}
