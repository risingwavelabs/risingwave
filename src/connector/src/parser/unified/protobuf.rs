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

use std::borrow::Cow;
use std::sync::LazyLock;

use prost_reflect::{DynamicMessage, ReflectMessage};
use risingwave_common::log::LogSuppresser;
use risingwave_common::types::{DataType, DatumCow, ToOwnedDatum};
use thiserror_ext::AsReport;

use super::{Access, AccessResult};
use crate::parser::from_protobuf_value;
use crate::parser::unified::uncategorized;

pub struct ProtobufAccess {
    message: DynamicMessage,
}

impl ProtobufAccess {
    pub fn new(message: DynamicMessage) -> Self {
        Self { message }
    }
}

impl Access for ProtobufAccess {
    fn access<'a>(
        &'a self,
        path: &[&str],
        _type_expected: &DataType,
    ) -> AccessResult<DatumCow<'a>> {
        debug_assert_eq!(1, path.len());
        let field_desc = self
            .message
            .descriptor()
            .get_field_by_name(path[0])
            .ok_or_else(|| uncategorized!("protobuf schema don't have field {}", path[0]))
            .inspect_err(|e| {
                static LOG_SUPPERSSER: LazyLock<LogSuppresser> =
                    LazyLock::new(LogSuppresser::default);
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(suppressed_count, "{}", e.as_report());
                }
            })?;

        match self.message.get_field(&field_desc) {
            Cow::Borrowed(value) => from_protobuf_value(&field_desc, value),

            // `Owned` variant occurs only if there's no such field and the default value is returned.
            Cow::Owned(value) => from_protobuf_value(&field_desc, &value)
                // enforce `Owned` variant to avoid returning a reference to a temporary value
                .map(|d| d.to_owned_datum().into()),
        }
    }
}
