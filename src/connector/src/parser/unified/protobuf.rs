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

use std::sync::Arc;

use anyhow::anyhow;
use prost_reflect::{DescriptorPool, DynamicMessage, ReflectMessage};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::RwError;
use risingwave_common::types::DataType;

use super::{Access, AccessResult};
use crate::parser::from_protobuf_value;
use crate::parser::unified::AccessError;

pub struct ProtobufAccess {
    message: DynamicMessage,
    descriptor_pool: Arc<DescriptorPool>,
}

impl ProtobufAccess {
    pub fn new(message: DynamicMessage, descriptor_pool: Arc<DescriptorPool>) -> Self {
        Self {
            message,
            descriptor_pool,
        }
    }
}

impl Access for ProtobufAccess {
    fn access(&self, path: &[&str], _type_expected: Option<&DataType>) -> AccessResult {
        debug_assert_eq!(1, path.len());
        let field_desc = self
            .message
            .descriptor()
            .get_field_by_name(path[0])
            .ok_or_else(|| {
                let err_msg = format!("protobuf schema don't have field {}", path[0]);
                tracing::error!(err_msg);
                RwError::from(ProtocolError(err_msg))
            })
            .map_err(|e| AccessError::Other(anyhow!(e)))?;
        let value = self.message.get_field(&field_desc);
        from_protobuf_value(&field_desc, &value, &self.descriptor_pool)
            .map_err(|e| AccessError::Other(anyhow!(e)))
    }
}
