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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use parking_lot::Mutex;
use risingwave_common::bail;
use risingwave_common::types::JsonbVal;
use serde_derive::{Deserialize, Serialize};
use with_options::WithOptions;

use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef, SourceProperties,
    SplitEnumerator, SplitId, SplitMetaData, SplitReader, TryFromBTreeMap,
};

pub type BoxListSplits = Box<
    dyn FnMut(
            TestSourceProperties,
            SourceEnumeratorContextRef,
        ) -> ConnectorResult<Vec<TestSourceSplit>>
        + Send
        + 'static,
>;

pub type BoxIntoSourceStream = Box<
    dyn FnMut(
            TestSourceProperties,
            Vec<TestSourceSplit>,
            ParserConfig,
            SourceContextRef,
            Option<Vec<Column>>,
        ) -> BoxSourceChunkStream
        + Send
        + 'static,
>;

pub struct BoxSource {
    list_split: BoxListSplits,
    into_source_stream: BoxIntoSourceStream,
}

impl BoxSource {
    pub fn new(
        list_splits: impl FnMut(
            TestSourceProperties,
            SourceEnumeratorContextRef,
        ) -> ConnectorResult<Vec<TestSourceSplit>>
        + Send
        + 'static,
        into_source_stream: impl FnMut(
            TestSourceProperties,
            Vec<TestSourceSplit>,
            ParserConfig,
            SourceContextRef,
            Option<Vec<Column>>,
        ) -> BoxSourceChunkStream
        + Send
        + 'static,
    ) -> BoxSource {
        BoxSource {
            list_split: Box::new(list_splits),
            into_source_stream: Box::new(into_source_stream),
        }
    }
}

struct TestSourceRegistry {
    box_source: Arc<Mutex<Option<BoxSource>>>,
}

impl TestSourceRegistry {
    fn new() -> Self {
        TestSourceRegistry {
            box_source: Arc::new(Mutex::new(None)),
        }
    }
}

fn get_registry() -> &'static TestSourceRegistry {
    static GLOBAL_REGISTRY: OnceLock<TestSourceRegistry> = OnceLock::new();
    GLOBAL_REGISTRY.get_or_init(TestSourceRegistry::new)
}

pub struct TestSourceRegistryGuard;

impl Drop for TestSourceRegistryGuard {
    fn drop(&mut self) {
        assert!(get_registry().box_source.lock().take().is_some());
    }
}

pub fn register_test_source(box_source: BoxSource) -> TestSourceRegistryGuard {
    assert!(
        get_registry()
            .box_source
            .lock()
            .replace(box_source)
            .is_none()
    );
    TestSourceRegistryGuard
}

pub const TEST_CONNECTOR: &str = "test";

#[derive(Clone, Debug, Default, WithOptions)]
pub struct TestSourceProperties {
    properties: BTreeMap<String, String>,
}

impl TryFromBTreeMap for TestSourceProperties {
    fn try_from_btreemap(
        props: BTreeMap<String, String>,
        _deny_unknown_fields: bool,
    ) -> ConnectorResult<Self> {
        if cfg!(any(madsim, test)) {
            Ok(TestSourceProperties { properties: props })
        } else {
            bail!("test source only available at test")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TestSourceSplit {
    pub id: SplitId,
    pub properties: HashMap<String, String>,
    pub offset: String,
}

impl SplitMetaData for TestSourceSplit {
    fn id(&self) -> SplitId {
        self.id.clone()
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.offset = last_seen_offset;
        Ok(())
    }
}

pub struct TestSourceSplitEnumerator {
    properties: TestSourceProperties,
    context: SourceEnumeratorContextRef,
}

#[async_trait]
impl SplitEnumerator for TestSourceSplitEnumerator {
    type Properties = TestSourceProperties;
    type Split = TestSourceSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            properties,
            context,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
        (get_registry()
            .box_source
            .lock()
            .as_mut()
            .expect("should have init")
            .list_split)(self.properties.clone(), self.context.clone())
    }
}

pub struct TestSourceSplitReader {
    properties: TestSourceProperties,
    state: Vec<TestSourceSplit>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    columns: Option<Vec<Column>>,
}

#[async_trait]
impl SplitReader for TestSourceSplitReader {
    type Properties = TestSourceProperties;
    type Split = TestSourceSplit;

    async fn new(
        properties: Self::Properties,
        state: Vec<Self::Split>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            properties,
            state,
            parser_config,
            source_ctx,
            columns,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        (get_registry()
            .box_source
            .lock()
            .as_mut()
            .expect("should have init")
            .into_source_stream)(
            self.properties,
            self.state,
            self.parser_config,
            self.source_ctx,
            self.columns,
        )
    }
}

impl SourceProperties for TestSourceProperties {
    type Split = TestSourceSplit;
    type SplitEnumerator = TestSourceSplitEnumerator;
    type SplitReader = TestSourceSplitReader;

    const SOURCE_NAME: &'static str = TEST_CONNECTOR;
}

impl crate::source::UnknownFields for TestSourceProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}
