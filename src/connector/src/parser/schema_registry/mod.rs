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

mod client;
mod util;
pub use client::*;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::RwError;
use risingwave_pb::catalog::SchemaRegistryNameStrategy as PbSchemaRegistryNameStrategy;
pub(crate) use util::*;

pub fn name_strategy_from_str(value: &str) -> Option<PbSchemaRegistryNameStrategy> {
    match value {
        "topic_name_strategy" => Some(PbSchemaRegistryNameStrategy::TopicNameStrategyUnspecified),
        "record_name_strategy" => Some(PbSchemaRegistryNameStrategy::RecordNameStrategy),
        "topic_record_name_strategy" => Some(PbSchemaRegistryNameStrategy::TopicRecordNameStrategy),
        _ => None,
    }
}

pub fn get_subject_by_strategy(
    name_strategy: &PbSchemaRegistryNameStrategy,
    topic: &str,
    key_record_name: Option<&str>,
    record: Option<&str>,
    require_key: bool,
) -> Result<(String, String), RwError> {
    let build_error_lack_field =
        |ns: &PbSchemaRegistryNameStrategy, expect: &[&str], got: &[Option<&str>]| -> RwError {
            RwError::from(ProtocolError(format!(
                "{:?} expect num-empty field {:?} but got {:?}",
                ns.as_str_name(),
                expect,
                got
            )))
        };
    let build_error_redundant_field =
        |ns: &PbSchemaRegistryNameStrategy, expect: &[&str], got: &[Option<&str>]| -> RwError {
            RwError::from(ProtocolError(format!(
                "{:?} expect empty field {:?} but got {:?}",
                ns.as_str_name(),
                expect,
                got
            )))
        };
    match (name_strategy, require_key) {
        (PbSchemaRegistryNameStrategy::TopicNameStrategyUnspecified, _) => {
            // default behavior
            Ok((format!("{}-key", topic), format!("{}-value", topic)))
        }
        (ns @ PbSchemaRegistryNameStrategy::RecordNameStrategy, true) => {
            if let Some(record_name) = record && let Some(key_rec_name) = key_record_name {
                Ok((key_rec_name.to_string(), record_name.to_string()))
            } else {
                Err(build_error_lack_field(ns, &["key.message","message"], &[key_record_name, record]))
            }
        }
        (ns @ PbSchemaRegistryNameStrategy::RecordNameStrategy, false) => {
            if key_record_name.is_some() {
                return Err(build_error_redundant_field(ns, &["key.message"], &[key_record_name]));
            }
            if let Some(record_name) = record {
                Ok(("".to_string(), record_name.to_string()))
            } else {
                Err(build_error_lack_field(ns, &["message"], &[record]))
            }
        }
        (ns @ PbSchemaRegistryNameStrategy::TopicRecordNameStrategy, true) => {
            if let Some(record_name) = record && let Some(key_rec_name) = key_record_name {
                Ok((
                    format!("{}-{}", topic, key_rec_name),
                    format!("{}-{}", topic, record_name),
                ))
            } else {
                Err(build_error_lack_field(
                    ns,
                    &["topic", "key.message","message"],
                    &[Some(topic), key_record_name, record],
                ))
            }
        }
        (ns @ PbSchemaRegistryNameStrategy::TopicRecordNameStrategy, false) => {
            if key_record_name.is_some() {
                return Err(build_error_redundant_field(ns, &["key.message"], &[key_record_name]));
            }
            if let Some(record_name) = record {
                Ok(("".to_string(), format!("{}-{}", topic, record_name)))
            } else {
                Err(build_error_lack_field(
                    ns,
                    &["topic","message"],
                    &[Some(topic), record],
                ))
            }
        }
    }
}
