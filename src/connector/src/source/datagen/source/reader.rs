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

use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use risingwave_common::field_generator::{FieldGeneratorImpl, VarcharProperty};
use risingwave_common::types::DataType;
use risingwave_common_estimate_size::EstimateSize;
use thiserror_ext::AsReport;

use super::generator::DatagenEventGenerator;
use crate::error::{ConnectorResult, ConnectorResult as Result};
use crate::parser::{EncodingProperties, ParserConfig, ProtocolProperties};
use crate::source::data_gen_util::spawn_data_generation_stream;
use crate::source::datagen::source::SEQUENCE_FIELD_KIND;
use crate::source::datagen::{DatagenProperties, DatagenSplit, FieldDesc};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, into_chunk_stream,
};

pub struct DatagenSplitReader {
    generator: DatagenEventGenerator,
    #[expect(dead_code)]
    assigned_split: DatagenSplit,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for DatagenSplitReader {
    type Properties = DatagenProperties;
    type Split = DatagenSplit;

    #[allow(clippy::unused_async)]
    async fn new(
        properties: DatagenProperties,
        splits: Vec<DatagenSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let mut events_so_far = u64::default();
        tracing::debug!("Splits for datagen found! {:?}", splits);

        debug_assert!(splits.len() == 1);
        let split = splits.into_iter().next().unwrap();
        // TODO: currently, assume there's only on split in one reader
        let split_id = split.id();
        let assigned_split = split;
        if let Some(s) = assigned_split.start_offset {
            // start_offset in `SplitImpl` indicates the latest successfully generated
            // index, so here we use start_offset+1
            events_so_far = s + 1;
        };

        let split_index = assigned_split.split_index as u64;
        let split_num = assigned_split.split_num as u64;

        let rows_per_second = properties.rows_per_second;
        let fields_option_map = properties.fields;

        // check columns
        assert!(columns.is_some());
        let columns = columns.unwrap();
        let mut fields_vec = Vec::with_capacity(columns.len());
        let mut data_types = Vec::with_capacity(columns.len());
        let mut field_names = Vec::with_capacity(columns.len());

        // parse field connector option to build FieldGeneratorImpl
        // for example:
        // create table t1  (
        //     f_sequence INT,
        //     f_random INT,
        //    ) with (
        //     'connector' = 'datagen',
        // 'fields.f_sequence.kind'='sequence',
        // 'fields.f_sequence.start'='1',
        // 'fields.f_sequence.end'='1000',

        // 'fields.f_random.min'='1',
        // 'fields.f_random.max'='1000',
        // 'fields.f_random.seed'='12345',

        // 'fields.f_random_str.length'='10'
        // )

        for column in columns {
            // let name = column.name.clone();
            let data_type = column.data_type.clone();

            let r#gen = if column.is_visible {
                FieldDesc::Visible(generator_from_data_type(
                    column.data_type,
                    &fields_option_map,
                    &column.name,
                    split_index,
                    split_num,
                    events_so_far,
                )?)
            } else {
                FieldDesc::Invisible
            };
            fields_vec.push(r#gen);
            data_types.push(data_type);
            field_names.push(column.name);
        }

        let generator = DatagenEventGenerator::new(
            fields_vec,
            field_names,
            parser_config.specific.clone(),
            data_types,
            rows_per_second,
            events_so_far,
            split_id.clone(),
            split_num,
            split_index,
        )?;

        Ok(DatagenSplitReader {
            generator,
            assigned_split,
            split_id,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        // Will buffer at most 4 event chunks.
        const BUFFER_SIZE: usize = 4;
        // spawn_data_generation_stream(self.generator.into_native_stream(), BUFFER_SIZE).boxed()
        match (
            &self.parser_config.specific.protocol_config,
            &self.parser_config.specific.encoding_config,
        ) {
            (ProtocolProperties::Native, EncodingProperties::Native) => {
                let actor_id = self.source_ctx.actor_id.to_string();
                let fragment_id = self.source_ctx.fragment_id.to_string();
                let source_id = self.source_ctx.source_id.to_string();
                let source_name = self.source_ctx.source_name.clone();
                let split_id = self.split_id.to_string();
                let metrics = self.source_ctx.metrics.clone();
                let partition_input_count_metric =
                    metrics.partition_input_count.with_guarded_label_values(&[
                        &actor_id,
                        &source_id,
                        &split_id,
                        &source_name,
                        &fragment_id,
                    ]);
                let partition_input_bytes_metric =
                    metrics.partition_input_bytes.with_guarded_label_values(&[
                        &actor_id,
                        &source_id,
                        &split_id,
                        &source_name,
                        &fragment_id,
                    ]);

                spawn_data_generation_stream(
                    self.generator
                        .into_native_stream()
                        .inspect_ok(move |stream_chunk| {
                            partition_input_count_metric.inc_by(stream_chunk.cardinality() as u64);
                            partition_input_bytes_metric
                                .inc_by(stream_chunk.estimated_size() as u64);
                        }),
                    BUFFER_SIZE,
                )
                .boxed()
            }
            _ => {
                let parser_config = self.parser_config.clone();
                let source_context = self.source_ctx.clone();
                into_chunk_stream(self.into_data_stream(), parser_config, source_context)
            }
        }
    }
}

impl DatagenSplitReader {
    fn into_data_stream(self) -> impl Stream<Item = ConnectorResult<Vec<SourceMessage>>> {
        // Will buffer at most 4 event chunks.
        const BUFFER_SIZE: usize = 4;
        spawn_data_generation_stream(self.generator.into_msg_stream(), BUFFER_SIZE)
    }
}

fn generator_from_data_type(
    data_type: DataType,
    fields_option_map: &HashMap<String, String>,
    name: &String,
    split_index: u64,
    split_num: u64,
    offset: u64,
) -> Result<FieldGeneratorImpl> {
    let random_seed_key = format!("fields.{}.seed", name);
    let random_seed: u64 = match fields_option_map
        .get(&random_seed_key)
        .map(|s| s.to_string())
    {
        Some(seed) => {
            match seed.parse::<u64>() {
                // we use given seed xor split_index to make sure every split has different
                // seed
                Ok(seed) => seed ^ split_index,
                Err(e) => {
                    tracing::warn!(
                        error = %e.as_report(),
                        "cannot parse {:?} to u64, will use {:?} as random seed",
                        seed,
                        split_index
                    );
                    split_index
                }
            }
        }
        None => split_index,
    };
    match data_type {
        ty @ (DataType::Timestamp | DataType::Timestamptz) => {
            let max_past_key = format!("fields.{}.max_past", name);
            let max_past_value = fields_option_map.get(&max_past_key).cloned();
            let max_past_mode_key = format!("fields.{}.max_past_mode", name);
            let max_past_mode_value = fields_option_map
                .get(&max_past_mode_key)
                .map(|s| s.to_lowercase());
            let basetime = match fields_option_map.get(format!("fields.{}.basetime", name).as_str())
            {
                Some(base) => Some(
                    chrono::DateTime::parse_from_rfc3339(base)
                        .with_context(|| format!("cannot parse `{base}` to rfc3339"))?,
                ),
                None => None,
            };

            if ty == DataType::Timestamptz {
                FieldGeneratorImpl::with_timestamptz(
                    basetime,
                    max_past_value,
                    max_past_mode_value,
                    random_seed,
                )
            } else {
                FieldGeneratorImpl::with_timestamp(
                    basetime,
                    max_past_value,
                    max_past_mode_value,
                    random_seed,
                )
            }
            .map_err(Into::into)
        }
        DataType::Varchar => {
            let length_key = format!("fields.{}.length", name);
            let length_value = fields_option_map
                .get(&length_key)
                .map(|s| s.parse::<usize>())
                .transpose()
                .context("failed to parse the length of varchar field")?;
            Ok(FieldGeneratorImpl::with_varchar(
                &VarcharProperty::RandomFixedLength(length_value),
                random_seed,
            ))
        }
        DataType::Struct(struct_type) => {
            let struct_fields = struct_type
                .iter()
                .map(|(field_name, data_type)| {
                    let r#gen = generator_from_data_type(
                        data_type.clone(),
                        fields_option_map,
                        &format!("{}.{}", name, field_name),
                        split_index,
                        split_num,
                        offset,
                    )?;
                    Ok((field_name.to_owned(), r#gen))
                })
                .collect::<Result<_>>()?;
            FieldGeneratorImpl::with_struct_fields(struct_fields).map_err(Into::into)
        }
        DataType::List(datatype) => {
            let length_key = format!("fields.{}.length", name);
            let length_value = fields_option_map.get(&length_key).map(|s| s.to_string());
            let generator = generator_from_data_type(
                *datatype,
                fields_option_map,
                &format!("{}._", name),
                split_index,
                split_num,
                offset,
            )?;
            FieldGeneratorImpl::with_list(generator, length_value).map_err(Into::into)
        }
        _ => {
            let kind_key = format!("fields.{}.kind", name);
            if let Some(kind) = fields_option_map.get(&kind_key)
                && kind.as_str() == SEQUENCE_FIELD_KIND
            {
                let start_key = format!("fields.{}.start", name);
                let end_key = format!("fields.{}.end", name);
                let start_value = fields_option_map.get(&start_key).map(|s| s.to_string());
                let end_value = fields_option_map.get(&end_key).map(|s| s.to_string());
                FieldGeneratorImpl::with_number_sequence(
                    data_type,
                    start_value,
                    end_value,
                    split_index,
                    split_num,
                    offset,
                )
                .map_err(Into::into)
            } else {
                let min_key = format!("fields.{}.min", name);
                let max_key = format!("fields.{}.max", name);
                let min_value = fields_option_map.get(&min_key).map(|s| s.to_string());
                let max_value = fields_option_map.get(&max_key).map(|s| s.to_string());
                FieldGeneratorImpl::with_number_random(data_type, min_value, max_value, random_seed)
                    .map_err(Into::into)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::{convert_args, hashmap};
    use risingwave_common::array::{Op, StructValue};
    use risingwave_common::row::Row;
    use risingwave_common::types::{ScalarImpl, StructType, ToDatumRef};

    use super::*;
    use crate::parser::SpecificParserConfig;
    use crate::source::SourceContext;

    #[tokio::test]
    async fn test_generator() -> Result<()> {
        let mock_datum = vec![
            Column {
                name: "random_int".to_owned(),
                data_type: DataType::Int32,
                is_visible: true,
            },
            Column {
                name: "random_float".to_owned(),
                data_type: DataType::Float32,
                is_visible: true,
            },
            Column {
                name: "sequence_int".to_owned(),
                data_type: DataType::Int32,
                is_visible: true,
            },
            Column {
                name: "struct".to_owned(),
                data_type: DataType::Struct(StructType::new(vec![(
                    "random_int".to_owned(),
                    DataType::Int32,
                )])),
                is_visible: true,
            },
        ];
        let state = vec![DatagenSplit {
            split_index: 0,
            split_num: 1,
            start_offset: None,
        }];
        let properties = DatagenProperties {
            split_num: None,
            rows_per_second: 10,
            fields: convert_args!(hashmap!(
                "fields.random_int.min" => "1",
                "fields.random_int.max" => "1000",
                "fields.random_int.seed" => "12345",

                "fields.random_float.min" => "1",
                "fields.random_float.max" => "1000",
                "fields.random_float.seed" => "12345",

                "fields.sequence_int.kind" => "sequence",
                "fields.sequence_int.start" => "1",
                "fields.sequence_int.end" => "1000",

                "fields.struct.random_int.min" => "1001",
                "fields.struct.random_int.max" => "2000",
                "fields.struct.random_int.seed" => "12345",
            )),
        };

        let mut reader = DatagenSplitReader::new(
            properties,
            state,
            ParserConfig {
                specific: SpecificParserConfig {
                    encoding_config: EncodingProperties::Native,
                    protocol_config: ProtocolProperties::Native,
                },
                ..Default::default()
            },
            SourceContext::dummy().into(),
            Some(mock_datum),
        )
        .await?
        .into_stream();

        let stream_chunk = reader.next().await.unwrap().unwrap();
        let (op, row) = stream_chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        assert_eq!(row.datum_at(0), Some(ScalarImpl::Int32(533)).to_datum_ref(),);
        assert_eq!(
            row.datum_at(1),
            Some(ScalarImpl::Float32(533.148_86.into())).to_datum_ref(),
        );
        assert_eq!(row.datum_at(2), Some(ScalarImpl::Int32(1)).to_datum_ref());
        assert_eq!(
            row.datum_at(3),
            Some(ScalarImpl::Struct(StructValue::new(vec![Some(
                ScalarImpl::Int32(1533)
            )])))
            .to_datum_ref()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_random_deterministic() -> Result<()> {
        let mock_datum = vec![
            Column {
                name: "_".to_owned(),
                data_type: DataType::Int64,
                is_visible: true,
            },
            Column {
                name: "random_int".to_owned(),
                data_type: DataType::Int32,
                is_visible: true,
            },
        ];
        let state = vec![DatagenSplit {
            split_index: 0,
            split_num: 1,
            start_offset: None,
        }];
        let properties = DatagenProperties {
            split_num: None,
            rows_per_second: 10,
            fields: HashMap::new(),
        };
        let parser_config = ParserConfig {
            specific: SpecificParserConfig {
                encoding_config: EncodingProperties::Native,
                protocol_config: ProtocolProperties::Native,
            },
            ..Default::default()
        };
        let stream = DatagenSplitReader::new(
            properties.clone(),
            state,
            parser_config.clone(),
            SourceContext::dummy().into(),
            Some(mock_datum.clone()),
        )
        .await?
        .into_stream();

        let v1 = stream.skip(1).next().await.unwrap()?;

        let state = vec![DatagenSplit {
            split_index: 0,
            split_num: 1,
            start_offset: Some(9),
        }];
        let mut stream = DatagenSplitReader::new(
            properties,
            state,
            parser_config,
            SourceContext::dummy().into(),
            Some(mock_datum),
        )
        .await?
        .into_stream();
        let v2 = stream.next().await.unwrap()?;

        assert_eq!(v1, v2);

        Ok(())
    }
}
