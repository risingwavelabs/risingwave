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

use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Azblob;
use opendal::Operator;

use super::{EngineType, OpendalObjectStore};
use crate::object::ObjectResult;

const AZBLOB_ENDPOINT: &str = "AZBLOB_ENDPOINT";
impl OpendalObjectStore {
    /// create opendal azblob engine.
    pub fn new_azblob_engine(container_name: String, root: String) -> ObjectResult<Self> {
        // Create azblob backend builder.
        let mut builder = Azblob::default();
        builder.root(&root);
        builder.container(&container_name);

        let endpoint = std::env::var(AZBLOB_ENDPOINT)
            .unwrap_or_else(|_| panic!("AZBLOB_ENDPOINT not found from environment variables"));

        builder.endpoint(&endpoint);

        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::Azblob,
        })
    }
}
