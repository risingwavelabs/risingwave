// Copyright 2026 RisingWave Labs
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

use anyhow::Context;
use prost_reflect::{DynamicMessage, FileDescriptor, MessageDescriptor};
use risingwave_common::bail;

use super::message::parse_confluent_protobuf_message;
use super::schema_cache::ProtobufSchemaCache;
use crate::error::ConnectorResult;
use crate::schema::ConfluentSchemaLoader;

#[derive(Debug)]
pub(super) enum ProtobufDecoder {
    Static(MessageDescriptor),
    Confluent(ConfluentProtobufDecoder),
}

impl ProtobufDecoder {
    pub async fn confluent(
        message_name: String,
        loader: ConfluentSchemaLoader,
        initial_schema: (i32, FileDescriptor),
    ) -> Self {
        Self::Confluent(ConfluentProtobufDecoder {
            message_name,
            schemas: ProtobufSchemaCache::new(loader, initial_schema).await,
        })
    }

    pub async fn decode(&self, payload: &[u8]) -> ConnectorResult<DynamicMessage> {
        let (descriptor, payload) = match self {
            Self::Static(descriptor) => (descriptor.clone(), payload),
            Self::Confluent(decoder) => return decoder.decode(payload).await,
        };
        DynamicMessage::decode(descriptor, payload)
            .context("failed to parse protobuf message")
            .map_err(Into::into)
    }
}

#[derive(Debug)]
pub(super) struct ConfluentProtobufDecoder {
    message_name: String,
    schemas: ProtobufSchemaCache,
}

impl ConfluentProtobufDecoder {
    async fn decode(&self, payload: &[u8]) -> ConnectorResult<DynamicMessage> {
        let message = parse_confluent_protobuf_message(payload)?;
        let root_file = self.schemas.get(message.schema_id).await?;
        let descriptor = resolve_message_descriptor(&root_file, &message.message_indexes)?;

        if descriptor.full_name() != self.message_name {
            bail!(
                "protobuf writer message `{}` does not match configured message `{}`",
                descriptor.full_name(),
                self.message_name
            );
        }

        DynamicMessage::decode(descriptor, message.payload)
            .with_context(|| {
                format!(
                    "failed to parse protobuf message with writer schema ID {}",
                    message.schema_id
                )
            })
            .map_err(Into::into)
    }
}

fn resolve_message_descriptor(
    root_file: &FileDescriptor,
    message_indexes: &[u32],
) -> ConnectorResult<MessageDescriptor> {
    let Some((&top_level_index, nested_indexes)) = message_indexes.split_first() else {
        bail!("protobuf message index array must not be empty");
    };
    let mut descriptor = root_file
        .messages()
        .nth(top_level_index as usize)
        .with_context(|| {
            format!(
                "protobuf top-level message index {top_level_index} is out of bounds in `{}`",
                root_file.name()
            )
        })?;

    for &index in nested_indexes {
        let parent_name = descriptor.full_name().to_owned();
        let child = descriptor.child_messages().nth(index as usize);
        descriptor = child.with_context(|| {
            format!("protobuf nested message index {index} is out of bounds in `{parent_name}`")
        })?;
    }
    Ok(descriptor)
}

#[cfg(test)]
mod tests {
    use prost_reflect::DescriptorPool;
    use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};

    use super::*;

    fn test_file_descriptor() -> FileDescriptor {
        let nested = DescriptorProto {
            name: Some("Nested".into()),
            ..Default::default()
        };
        let parent = DescriptorProto {
            name: Some("Parent".into()),
            nested_type: vec![nested],
            ..Default::default()
        };
        let event = DescriptorProto {
            name: Some("Event".into()),
            ..Default::default()
        };
        let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".into()),
                package: Some("test".into()),
                message_type: vec![event, parent],
                syntax: Some("proto3".into()),
                ..Default::default()
            }],
        })
        .unwrap();
        pool.get_file_by_name("test.proto").unwrap()
    }

    #[test]
    fn test_resolve_message_descriptor() {
        let file = test_file_descriptor();
        assert_eq!(
            resolve_message_descriptor(&file, &[0]).unwrap().full_name(),
            "test.Event"
        );
        assert_eq!(
            resolve_message_descriptor(&file, &[1, 0])
                .unwrap()
                .full_name(),
            "test.Parent.Nested"
        );
        assert!(resolve_message_descriptor(&file, &[]).is_err());
        assert!(resolve_message_descriptor(&file, &[2]).is_err());
        assert!(resolve_message_descriptor(&file, &[1, 1]).is_err());
    }
}
