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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures_async_stream::try_stream;

use super::opendal_enumerator::OpenDALConnector;
use super::{GCSProperties, OpenDALProperties};
use crate::parser::ParserConfig;
use crate::source::filesystem::GcsSplit;
use crate::source::{
    BoxSourceWithStateStream, Column, CommonSplitReader, SourceContextRef, SourceMessage,
    SplitReader,
};
#[async_trait]
impl SplitReader for OpenDALConnector {
    type Properties = GCSProperties;
    type Split = GcsSplit;

    async fn new(
        properties: GCSProperties,
        splits: Vec<GcsSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        // match properties {
        //     OpenDALProperties::GCSProperties(gcs_properties) => {
        //         OpenDALConnector::new_gcs_source(gcs_properties)
        //     }
        //     OpenDALProperties::S3Properties(s3_properties) => {
        //         OpenDALConnector::new_s3_source(s3_properties)
        //     }
        // }
        OpenDALConnector::new_gcs_source(properties)
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        todo!()
    }
}
