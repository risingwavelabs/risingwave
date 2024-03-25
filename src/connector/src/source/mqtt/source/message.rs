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

use std::sync::Arc;

use risingwave_common::types::{Datum, ScalarImpl};
use rumqttc::v5::mqttbytes::v5::Publish;

use crate::impl_source_meta_extract_func;
use crate::source::base::SourceMessage;
use crate::source::{SourceMeta, SplitId};

#[derive(Clone, Debug)]
pub struct MqttMeta {
    offset: i32,
    topic: Arc<str>,
}

pub fn to_source_message(message: Publish) -> SourceMessage {
    SourceMessage {
        key: None,
        payload: Some(message.payload.to_vec()),
        meta: SourceMeta::Mqtt(MqttMeta {
            offset: message.pkid as i32,
            topic: Arc::from(String::from_utf8_lossy(&message.topic).to_string()),
        }),
    }
}

impl_source_meta_extract_func!(MqttMeta, Int32, offset, topic);
