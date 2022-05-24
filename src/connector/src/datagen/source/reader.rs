// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

use super::field_generator::{FieldGeneratorImpl, FieldKind};
use super::generator::DatagenEventGenerator;
use crate::datagen::source::SEQUENCE_FIELD_KIND;
use crate::datagen::DatagenProperties;
use crate::{Column, ConnectorStateV2, DataType, SourceMessage, SplitReader};

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

pub struct DatagenSplitReader {
    generator: DatagenEventGenerator,
}

#[async_trait]
impl SplitReader for DatagenSplitReader {
    type Properties = DatagenProperties;

    async fn new(
        properties: DatagenProperties,
        state: ConnectorStateV2,
        columns: Option<Vec<Column>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let _ = state;
        let batch_chunk_size = properties.max_chunk_size.parse::<u64>()?;
        let rows_per_second = properties.rows_per_second.parse::<u64>()?;
        let fields_option_map = properties.fields;
        let mut fields_map = HashMap::<String, FieldGeneratorImpl>::new();

        assert!(columns.as_ref().is_some());
        let columns = columns.unwrap();
        assert!(columns.len() > 1);
        let columns = &columns[1..];

        for column in columns {
            let name = column.name.clone();
            let kind_key = format!("field.{}.kind", name);
            match column.data_type{
                DataType::Timestamp => {
                let max_past_key = format!("field.{}.max_past", name);
                let max_past_key_option =
                fields_option_map.get(&max_past_key).map(|s| s.to_string());
                fields_map.insert(
                    name,
                    FieldGeneratorImpl::new(
                        column.data_type.clone(),
                            FieldKind::Random,
                            max_past_key_option,
                        None,
                    )?,
                );},
                DataType::Varchar => {
                let length_key = format!("field.{}.length", name);
                let length_key_option =
                fields_option_map.get(&length_key).map(|s| s.to_string());
                fields_map.insert(
                    name,
                    FieldGeneratorImpl::new(
                        column.data_type.clone(),
                            FieldKind::Random,
                        length_key_option,
                        None,
                    )?,
                );},
                _ => {
                    if let Some(kind) = fields_option_map.get(&kind_key) && kind.as_str() == SEQUENCE_FIELD_KIND{
                        let start_key = format!("field.{}.start", name);
                        let end_key = format!("field.{}.end", name);
                        let start_key_option =
                            fields_option_map.get(&start_key).map(|s| s.to_string());
                        let end_key_option = fields_option_map.get(&end_key).map(|s| s.to_string());
                        fields_map.insert(
                            name,
                            FieldGeneratorImpl::new(
                                column.data_type.clone(),
                                FieldKind::Sequence,
                                start_key_option,
                                end_key_option,
                            )?,
                        );
                    } else{
                        let min_key = format!("field.{}.min", name);
                        let max_key = format!("field.{}.max", name);
                        let min_value_option = fields_option_map.get(&min_key).map(|s| s.to_string());
                        let max_value_option = fields_option_map.get(&max_key).map(|s| s.to_string());
                        fields_map.insert(
                            name,
                            FieldGeneratorImpl::new(
                                column.data_type.clone(),
                                FieldKind::Random,
                                min_value_option,
                                max_value_option,
                            )?,
                        );
                    }
                }
            }
        }

        Ok(DatagenSplitReader {
            generator: DatagenEventGenerator::new(
                fields_map,
                0,
                batch_chunk_size,
                rows_per_second,
            )?,
        })
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        self.generator.next().await
    }
}
