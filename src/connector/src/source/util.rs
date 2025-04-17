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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::SplitImpl;
use crate::source::nats::split::NatsSplit;

pub fn fill_adaptive_split(
    split_template: &SplitImpl,
    actor_in_use: &HashSet<u32>,
) -> ConnectorResult<BTreeMap<Arc<str>, SplitImpl>> {
    // Just Nats is adaptive for now
    if let SplitImpl::Nats(split) = split_template {
        let mut new_splits = BTreeMap::new();
        for actor_id in actor_in_use {
            let actor_id: Arc<str> = actor_id.to_string().into();
            new_splits.insert(
                actor_id.clone(),
                SplitImpl::Nats(NatsSplit::new(
                    split.subject.clone(),
                    actor_id,
                    split.start_sequence.clone(),
                )),
            );
        }
        tracing::debug!(
            "Filled adaptive splits for Nats source, {} splits in total",
            new_splits.len()
        );
        Ok(new_splits)
    } else {
        Err(ConnectorError::from(anyhow::anyhow!(
            "Unsupported split type, expect Nats SplitImpl but get {:?}",
            split_template
        )))
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
