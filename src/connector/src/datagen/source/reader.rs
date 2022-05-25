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

        // parse field connector option to build FieldGeneratorImpl
        // for example:
        // create materialized source s1  (
        //     f_sequence INT,
        //     f_random INT,
        //    ) with (
        //     'connector' = 'datagen',
        // 'fields.f_sequence.kind'='sequence',
        // 'fields.f_sequence.start'='1',
        // 'fields.f_sequence.end'='1000',
       
        // 'fields.f_random.min'='1',
        // 'fields.f_random.max'='1000',
       
        // 'fields.f_random_str.length'='10'
        // )
        
        for column in columns {
            let name = column.name.clone();
            let kind_key = format!("fields.{}.kind", name);
            let data_type = column.data_type.clone();
            match column.data_type{
                DataType::Timestamp => {
                let max_past_key = format!("fields.{}.max_past", name);
                let max_past_value =
                fields_option_map.get(&max_past_key).map(|s| s.to_string());
                fields_map.insert(
                    name,
                    FieldGeneratorImpl::new(
                        data_type,
                            FieldKind::Random,
                            max_past_value,
                        None,
                        split_index,
                        split_num
                    )?,
                );},
                DataType::Varchar => {
                let length_key = format!("fields.{}.length", name);
                let length_value =
                fields_option_map.get(&length_key).map(|s| s.to_string());
                fields_map.insert(
                    name,
                    FieldGeneratorImpl::new(
                        data_type,
                            FieldKind::Random,
                        length_value,
                        None,
                        split_index,
                        split_num
                    )?,
                );},
                _ => {
                    if let Some(kind) = fields_option_map.get(&kind_key) && kind.as_str() == SEQUENCE_FIELD_KIND{
                        let start_key = format!("fields.{}.start", name);
                        let end_key = format!("fields.{}.end", name);
                        let start_value =
                            fields_option_map.get(&start_key).map(|s| s.to_string());
                        let end_value = fields_option_map.get(&end_key).map(|s| s.to_string());
                        fields_map.insert(
                            name,
                            FieldGeneratorImpl::new(
                                data_type,
                                FieldKind::Sequence,
                                start_value,
                                end_value,
                                split_index,
                                split_num
                            )?,
                        );
                    } else{
                        let min_key = format!("fields.{}.min", name);
                        let max_key = format!("fields.{}.max", name);
                        let min_value = fields_option_map.get(&min_key).map(|s| s.to_string());
                        let max_value = fields_option_map.get(&max_key).map(|s| s.to_string());
                        fields_map.insert(
                            name,
                            FieldGeneratorImpl::new(
                                data_type,
                                FieldKind::Random,
                                min_value,
                                max_value,
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
