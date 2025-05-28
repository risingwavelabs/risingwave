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

use async_trait::async_trait;

use super::MqttProperties;
use super::source::MqttSplit;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub struct MqttSplitEnumerator {
    topic: String,
}

#[async_trait]
impl SplitEnumerator for MqttSplitEnumerator {
    type Properties = MqttProperties;
    type Split = MqttSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<MqttSplitEnumerator> {
        Ok(Self {
            topic: properties.topic,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<MqttSplit>> {
        Ok(vec![MqttSplit::new(self.topic.clone())])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::super::MqttProperties;
    use super::MqttSplitEnumerator;
    use crate::connector_common::MqttCommon;
    use crate::source::mqtt::source::MqttSplit;
    use crate::source::{SourceEnumeratorContext, SplitEnumerator};

    #[tokio::test]
    async fn test_mqtt_enumerator() {
        let common = MqttCommon {
            url: "mqtt://broker".to_owned(),
            qos: None,
            user: None,
            password: None,
            client_prefix: None,
            clean_start: true,
            inflight_messages: None,
            max_packet_size: None,
            ca: None,
            client_cert: None,
            client_key: None,
        };
        let props = MqttProperties {
            common,
            topic: "test".to_owned(),
            qos: None,
            unknown_fields: HashMap::new(),
        };
        let context = Arc::new(SourceEnumeratorContext::dummy());
        let mut enumerator = MqttSplitEnumerator::new(props, context).await.unwrap();

        assert_eq!(
            enumerator.list_splits().await.unwrap(),
            vec![MqttSplit::new("test".to_owned())]
        );
    }
}
