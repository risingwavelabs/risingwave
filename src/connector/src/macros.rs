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

#[macro_export]
macro_rules! impl_split_enumerator {
    ($({ $variant_name:ident, $split_enumerator_name:ident} ),*) => {
        impl SplitEnumeratorImpl {

             pub async fn create(properties: ConnectorProperties) -> Result<Self> {
                match properties {
                    $( ConnectorProperties::$variant_name(props) => $split_enumerator_name::new(*props).await.map(Self::$variant_name), )*
                    other => Err(anyhow!(
                        "split enumerator type for config {:?} is not supported",
                        other
                    )),
                }
             }

             pub async fn list_splits(&mut self) -> Result<Vec<SplitImpl>> {
                match self {
                    $( Self::$variant_name(inner) => inner
                        .list_splits()
                        .await
                        .map(|ss| {
                            ss.into_iter()
                                .map(SplitImpl::$variant_name)
                                .collect_vec()
                        })
                        .map_err(|e| ErrorCode::ConnectorError(e.into()).into()),
                    )*
                }
             }
        }
    }
}

#[macro_export]
macro_rules! impl_split {
    ($({ $variant_name:ident, $connector_name:ident, $split:ty} ),*) => {
        impl From<&SplitImpl> for ConnectorSplit {
            fn from(split: &SplitImpl) -> Self {
                match split {
                    $( SplitImpl::$variant_name(inner) => ConnectorSplit { split_type: String::from($connector_name), encoded_split: inner.encode_to_bytes().to_vec() }, )*
                }
            }
        }

        impl TryFrom<&ConnectorSplit> for SplitImpl {
            type Error = anyhow::Error;

            fn try_from(split: &ConnectorSplit) -> std::result::Result<Self, Self::Error> {
                match split.split_type.to_lowercase().as_str() {
                    $( $connector_name => <$split>::restore_from_bytes(split.encoded_split.as_ref()).map(SplitImpl::$variant_name), )*
                        other => {
                    Err(anyhow!("connector '{}' is not supported", other))
                    }
                }
            }
        }

        impl SplitMetaData for SplitImpl {
            fn id(&self) -> SplitId {
                match self {
                    $( Self::$variant_name(inner) => inner.id(), )*
                }
            }

            fn encode_to_bytes(&self) -> Bytes {
                Bytes::from(ConnectorSplit::from(self).encode_to_vec())
            }

            fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
                SplitImpl::try_from(&ConnectorSplit::decode(bytes)?)
            }
        }

        impl SplitImpl {
             pub fn get_type(&self) -> String {
                match self {
                    $( Self::$variant_name(_) => $connector_name, )*
                }
                    .to_string()
            }

            pub fn update(&self, start_offset: String) -> Self {
                match self {
                    $( Self::$variant_name(inner) => Self::$variant_name(inner.copy_with_offset(start_offset)), )*
                }
            }
        }
    }
}

#[macro_export]
macro_rules! impl_split_reader {
    ($({ $variant_name:ident, $split_reader_name:ident} ),*) => {
        impl SplitReaderImpl {
            pub fn into_stream(self) -> BoxSourceWithStateStream {
                match self {
                    $( Self::$variant_name(inner) => inner.into_stream(), )*                 }
            }

            pub async fn create(
                config: ConnectorProperties,
                state: ConnectorState,
                parser_config: ParserConfig,
                source_ctx: SourceContextRef,
                columns: Option<Vec<Column>>,
            ) -> Result<Self> {
                if state.is_none() {
                    return Ok(Self::Dummy(Box::new(DummySplitReader {})));
                }
                let splits = state.unwrap();
                let connector = match config {
                     $( ConnectorProperties::$variant_name(props) => Self::$variant_name(Box::new($split_reader_name::new(*props, splits, parser_config, source_ctx, columns).await?)), )*
                };

                Ok(connector)
            }
        }
    }
}

#[macro_export]
macro_rules! impl_connector_properties {
    ($({ $variant_name:ident, $connector_name:ident } ),*) => {
        impl ConnectorProperties {
            pub fn extract(mut props: HashMap<String, String>) -> Result<Self> {
                const UPSTREAM_SOURCE_KEY: &str = "connector";
                let connector = props.remove(UPSTREAM_SOURCE_KEY).ok_or_else(|| anyhow!("Must specify 'connector' in WITH clause"))?;
                if connector.ends_with("cdc") {
                    ConnectorProperties::new_cdc_properties(&connector, props)
                } else {
                    let json_value = serde_json::to_value(props).map_err(|e| anyhow!(e))?;
                    match connector.to_lowercase().as_str() {
                        $(
                            $connector_name => {
                                serde_json::from_value(json_value).map_err(|e| anyhow!(e.to_string())).map(Self::$variant_name)
                            },
                        )*
                        _ => {
                            Err(anyhow!("connector '{}' is not supported", connector,))
                        }
                    }
                }
            }
        }
    }
}

#[macro_export]
macro_rules! impl_common_parser_logic {
    ($parser_name:ty) => {
        impl $parser_name {
            #[allow(unused_mut)]
            #[try_stream(boxed, ok = $crate::source::StreamChunkWithState, error = RwError)]
            async fn into_chunk_stream(mut self, data_stream: $crate::source::BoxSourceStream) {
                #[for_await]
                for batch in data_stream {
                    let batch = batch?;
                    let mut builder =
                    $crate::parser::SourceStreamChunkBuilder::with_capacity(self.rw_columns.clone(), batch.len());
                    let mut split_offset_mapping: std::collections::HashMap<$crate::source::SplitId, String> = std::collections::HashMap::new();

                    for msg in batch {
                        if let Some(content) = msg.payload {
                            split_offset_mapping.insert(msg.split_id, msg.offset);

                            let old_op_num = builder.op_num();

                            if let Err(e) = self.parse_inner(content.as_ref(), builder.row_writer())
                                .await
                            {
                                self.source_ctx.report_stream_source_error(&e);
                                tracing::warn!("message parsing failed {}, skipping", e.to_string());
                                continue;
                            }

                            let new_op_num = builder.op_num();

                            // new_op_num - old_op_num is the number of rows added to the builder
                            for _ in old_op_num..new_op_num {
                                // TODO: support more kinds of SourceMeta
                                if let $crate::source::SourceMeta::Kafka(kafka_meta) = msg.meta.clone() {
                                    let f = |desc: &SourceColumnDesc| -> Option<risingwave_common::types::Datum> {
                                        if !desc.is_meta {
                                            return None;
                                        }
                                        match desc.name.as_str() {
                                            "_rw_kafka_timestamp" => Some(
                                                kafka_meta
                                                    .timestamp
                                                    .map(|ts| risingwave_expr::vector_op::cast::i64_to_timestamptz(ts).unwrap().into()),
                                            ),
                                            _ => unreachable!(
                                                "kafka will not have this meta column: {}",
                                                desc.name
                                            ),
                                        }
                                    };
                                    builder.row_writer().fulfill_meta_column(f)?;
                                }
                            }
                        }
                    }
                    yield $crate::source::StreamChunkWithState {
                        chunk: builder.finish(),
                        split_offset_mapping: Some(split_offset_mapping),
                    };
                }
            }
        }

        impl $crate::parser::ByteStreamSourceParser for $parser_name {
            fn into_stream(self, data_stream: $crate::source::BoxSourceStream) -> $crate::source::BoxSourceWithStateStream {
                self.into_chunk_stream(data_stream)
            }
        }

    }
}

#[macro_export]
macro_rules! impl_common_split_reader_logic {
    ($reader:ty, $props:ty) => {
        impl $reader {
            #[try_stream(boxed, ok = $crate::source::StreamChunkWithState, error = risingwave_common::error::RwError)]
            pub(crate) async fn into_chunk_stream(self) {
                use $crate::parser::ByteStreamSourceParser;
                let parser_config = self.parser_config.clone();
                let actor_id = self.source_ctx.source_info.actor_id.to_string();
                let source_id = self.source_ctx.source_info.source_id.to_string();
                let split_id = self.split_id.clone();
                let metrics = self.source_ctx.metrics.clone();
                let source_ctx = self.source_ctx.clone();

                let data_stream = self.into_data_stream();

                let data_stream = data_stream
                    .map_ok(move |data_batch| {
                        metrics
                            .partition_input_count
                            .with_label_values(&[&actor_id, &source_id, &split_id])
                            .inc_by(data_batch.len() as u64);
                        let sum_bytes = data_batch
                            .iter()
                            .map(|msg| match &msg.payload {
                                None => 0,
                                Some(payload) => payload.len() as u64,
                            })
                            .sum();
                        metrics
                            .partition_input_bytes
                            .with_label_values(&[&actor_id, &source_id, &split_id])
                            .inc_by(sum_bytes);
                        data_batch
                    })
                    .boxed();
                let parser =
                    $crate::parser::ByteStreamSourceParserImpl::create(parser_config, source_ctx)?;
                #[for_await]
                for msg_batch in parser.into_stream(data_stream) {
                    yield msg_batch?;
                }
            }
        }
    };
}
