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

pub fn get_subject_by_strategy(
    name_strategy: &PbSchemaRegistryNameStrategy,
    topic: &str,
    record: Option<&str>,
) -> Result<(String, String), RwError> {
    let build_error =
        |ns: &PbSchemaRegistryNameStrategy, expect: &[&str], got: &[Option<&str>]| -> RwError {
            RwError::from(ProtocolError(format!(
                "{:?} expect num-empty field {:?} but got {:?}",
                ns.as_str_name(),
                expect,
                got
            )))
        };
    match name_strategy {
        PbSchemaRegistryNameStrategy::TopicNameStrategy => {
            Ok((format!("{}-key", topic), format!("{}-value", topic)))
        }
        ns @ PbSchemaRegistryNameStrategy::RecordNameStrategy => {
            if let Some(record_name) = record {
                Ok((
                    format!("{}-key", record_name),
                    format!("{}-value", record_name),
                ))
            } else {
                Err(build_error(ns, &["record"], &[record]))
            }
        }
        ns @ PbSchemaRegistryNameStrategy::TopicRecordNameStrategy => {
            if let Some(record_name) = record {
                Ok((
                    format!("{}-{}-key", topic, record_name),
                    format!("{}-{}-value", topic, record_name),
                ))
            } else {
                Err(build_error(
                    ns,
                    &["topic", "record"],
                    &[Some(topic), record],
                ))
            }
        }
    }
}
