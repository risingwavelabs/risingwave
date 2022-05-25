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
use crate::datagen::{DatagenProperties, DatagenSplit};
use crate::{Column, ConnectorStateV2, DataType, SourceMessage, SplitImpl, SplitReader};

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

pub struct DatagenSplitReader {
    pub generator: DatagenEventGenerator,
    pub assigned_split: DatagenSplit,
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
        let mut assigned_split = DatagenSplit::default();
        let mut split_id = String::new();
        let mut events_so_far = u64::default();
        match state {
            ConnectorStateV2::Splits(splits) => {
                log::debug!("Splits for datagen found! {:?}", splits);
                for split in splits {
                    // TODO: currently, assume there's only on split in one reader
                    split_id = split.id();
                    if let SplitImpl::Datagen(n) = split {
                        if let Some(s) = n.start_offset {
                            events_so_far = s;
                        };
                        assigned_split = n;
                        break;
                    }
                }
            }
            ConnectorStateV2::State(cs) => {
                log::debug!("Splits for datagen found! {:?}", cs);
                todo!()
            }
            ConnectorStateV2::None => {}
        }

        let split_index = assigned_split.split_index as u64;
        let split_num = assigned_split.split_num as u64;

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
                        split_index,
                        split_num
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
                        split_index,
                        split_num
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
                                split_index,
                                split_num
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
                                split_index,
                                split_num
                            )?,
                        );
                    }
                }
            }
        }

        let generator = DatagenEventGenerator::new(
            fields_map,
            rows_per_second,
            events_so_far,
            split_id,
            split_num,
            split_index,
        )?;

        Ok(DatagenSplitReader {
            generator,
            assigned_split,
        })
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        self.generator.next().await
    }
}
