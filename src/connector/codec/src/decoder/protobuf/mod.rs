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

pub mod parser;
use std::collections::HashSet;
use std::sync::LazyLock;

use prost_reflect::{DynamicMessage, ReflectMessage};
use risingwave_common::log::LogSuppressor;
use risingwave_common::types::{DataType, DatumCow};
use thiserror_ext::AsReport;

use super::{Access, AccessResult, uncategorized};

pub struct ProtobufAccess<'a> {
    message: DynamicMessage,
    messages_as_jsonb: &'a HashSet<String>,
}

impl<'a> ProtobufAccess<'a> {
    pub fn new(message: DynamicMessage, messages_as_jsonb: &'a HashSet<String>) -> Self {
        Self {
            message,
            messages_as_jsonb,
        }
    }

    #[cfg(test)]
    pub fn descriptor(&self) -> prost_reflect::MessageDescriptor {
        self.message.descriptor()
    }
}

impl Access for ProtobufAccess<'_> {
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>> {
        debug_assert_eq!(1, path.len());
        let field_desc = self
            .message
            .descriptor()
            .get_field_by_name(path[0])
            .ok_or_else(|| uncategorized!("protobuf schema don't have field {}", path[0]))
            .inspect_err(|e| {
                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                    LazyLock::new(LogSuppressor::default);
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::error!(suppressed_count, "{}", e.as_report());
                }
            })?;

        parser::from_protobuf_message_field(
            &field_desc,
            &self.message,
            type_expected,
            self.messages_as_jsonb,
        )
    }
}
