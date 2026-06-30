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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::SplitImpl;
use crate::source::google_pubsub::PubsubSplit;
use crate::source::nats::split::NatsSplit;

pub fn fill_adaptive_split(
    split_template: &SplitImpl,
    actor_count: usize,
) -> ConnectorResult<BTreeMap<Arc<str>, SplitImpl>> {
    match split_template {
        SplitImpl::Nats(split) => {
            let mut new_splits = BTreeMap::new();
            for idx in 0..actor_count {
                let split_id: Arc<str> = idx.to_string().into();
                new_splits.insert(
                    split_id.clone(),
                    SplitImpl::Nats(NatsSplit::new(
                        split.subject.clone(),
                        split_id,
                        split.start_sequence.clone(),
                    )),
                );
            }
            tracing::debug!(
                "Filled adaptive splits for Nats source, {} splits in total",
                new_splits.len()
            );
            Ok(new_splits)
        }
        SplitImpl::GooglePubsub(split) => {
            let mut new_splits = BTreeMap::new();
            for idx in 0..actor_count {
                let split_id: Arc<str> = format!("{}-{}", split.subscription, idx).into();
                new_splits.insert(
                    split_id,
                    SplitImpl::GooglePubsub(PubsubSplit {
                        index: idx as u32,
                        subscription: split.subscription.clone(),
                        __deprecated_start_offset: None,
                        __deprecated_stop_offset: None,
                    }),
                );
            }
            tracing::debug!(
                "Filled adaptive splits for GooglePubsub source, {} splits in total",
                new_splits.len()
            );
            Ok(new_splits)
        }
        _ => Err(ConnectorError::from(anyhow::anyhow!(
            "Unsupported split type for adaptive splits: {:?}",
            split_template
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::SplitMetaData;
    use crate::source::nats::split::NatsOffset;

    #[test]
    fn test_fill_adaptive_split_pubsub() {
        let template = SplitImpl::GooglePubsub(PubsubSplit {
            index: 0,
            subscription: "projects/p/subscriptions/s".to_owned(),
            __deprecated_start_offset: None,
            __deprecated_stop_offset: None,
        });

        let splits = fill_adaptive_split(&template, 3).unwrap();
        assert_eq!(splits.len(), 3);

        for (idx, (split_id, split)) in splits.iter().enumerate() {
            let expected_id = format!("projects/p/subscriptions/s-{}", idx);
            assert_eq!(split_id.as_ref(), expected_id.as_str());
            if let SplitImpl::GooglePubsub(ps) = split {
                assert_eq!(ps.index, idx as u32);
                assert_eq!(ps.subscription, "projects/p/subscriptions/s");
                let expected_split_id: Arc<str> = expected_id.as_str().into();
                assert_eq!(ps.id(), expected_split_id);
            } else {
                panic!("expected GooglePubsub split");
            }
        }
    }

    #[test]
    fn test_fill_adaptive_split_pubsub_single_actor() {
        let template = SplitImpl::GooglePubsub(PubsubSplit {
            index: 0,
            subscription: "sub".to_owned(),
            __deprecated_start_offset: None,
            __deprecated_stop_offset: None,
        });

        let splits = fill_adaptive_split(&template, 1).unwrap();
        assert_eq!(splits.len(), 1);
        assert!(splits.contains_key("sub-0"));
    }

    #[test]
    fn test_fill_adaptive_split_nats() {
        let template = SplitImpl::Nats(NatsSplit::new(
            "test-subject".to_owned(),
            "0".into(),
            NatsOffset::None,
        ));

        let splits = fill_adaptive_split(&template, 4).unwrap();
        assert_eq!(splits.len(), 4);

        for idx in 0..4 {
            assert!(splits.contains_key(idx.to_string().as_str()));
        }
    }

    #[test]
    fn test_fill_adaptive_split_unsupported() {
        // Use a Kafka split (not adaptive) to test the error path
        let template = SplitImpl::Kafka(crate::source::kafka::split::KafkaSplit::new(
            0,
            None,
            None,
            "topic".into(),
        ));
        let result = fill_adaptive_split(&template, 2);
        assert!(result.is_err());
    }
}

/// A no-op source, which can be used to deprecate a source.
/// When removing the code for a source, we can remove the source reader and executor,
/// but we need to let the meta source manager running normally without panic.
/// Otherwise the user can DROP the source.
pub mod dummy {
    use std::collections::HashMap;
    use std::marker::PhantomData;

    use risingwave_common::types::JsonbVal;
    use serde::Deserialize;
    use with_options::WithOptions;

    use crate::enforce_secret::EnforceSecret;
    use crate::error::ConnectorResult;
    use crate::parser::ParserConfig;
    use crate::source::{
        BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef,
        SplitEnumerator, SplitId, SplitMetaData, SplitReader, UnknownFields,
    };

    /// See [`crate::source::util::dummy`].
    #[derive(Deserialize, Debug, Clone, WithOptions, Default)]
    pub struct DummyProperties<T> {
        _marker: PhantomData<T>,
    }

    impl<T> EnforceSecret for DummyProperties<T> {}

    /// See [`crate::source::util::dummy`].
    pub struct DummySplitEnumerator<T> {
        _marker: PhantomData<T>,
    }
    /// See [`crate::source::util::dummy`].
    #[derive(Deserialize, Debug, PartialEq, Clone)]
    pub struct DummySplit<T> {
        _marker: PhantomData<T>,
    }

    impl<T> UnknownFields for DummyProperties<T> {
        fn unknown_fields(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    impl<T> SplitMetaData for DummySplit<T> {
        fn id(&self) -> SplitId {
            unreachable!()
        }

        fn restore_from_json(_value: JsonbVal) -> ConnectorResult<Self> {
            unreachable!()
        }

        fn encode_to_json(&self) -> JsonbVal {
            unreachable!()
        }

        fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
            unreachable!()
        }
    }

    #[async_trait::async_trait]
    impl<T: Send> SplitEnumerator for DummySplitEnumerator<T> {
        type Properties = DummyProperties<T>;
        type Split = DummySplit<T>;

        async fn new(
            _properties: Self::Properties,
            _context: SourceEnumeratorContextRef,
        ) -> crate::error::ConnectorResult<Self> {
            Ok(Self {
                _marker: PhantomData,
            })
        }

        async fn list_splits(&mut self) -> crate::error::ConnectorResult<Vec<Self::Split>> {
            // no op
            Ok(vec![])
        }
    }

    pub struct DummySourceReader<T> {
        _marker: PhantomData<T>,
    }

    #[async_trait::async_trait]
    impl<T: Send> SplitReader for DummySourceReader<T> {
        type Properties = DummyProperties<T>;
        type Split = DummySplit<T>;

        async fn new(
            _props: Self::Properties,
            _splits: Vec<Self::Split>,
            _parser_config: ParserConfig,
            _source_ctx: SourceContextRef,
            _columns: Option<Vec<Column>>,
        ) -> crate::error::ConnectorResult<Self> {
            Ok(Self {
                _marker: PhantomData,
            })
        }

        fn into_stream(self) -> BoxSourceChunkStream {
            unreachable!()
        }
    }
}
