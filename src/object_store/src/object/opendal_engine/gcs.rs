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

use opendal::services::Gcs;
use opendal::Operator;

use super::{EngineType, OpendalObjectStore};
use crate::object::ObjectResult;
impl OpendalObjectStore {
    /// create opendal gcs engine.
    pub fn new_gcs_engine(bucket: String, root: String) -> ObjectResult<Self> {
        // Create gcs backend builder.
        let mut builder = Gcs::default();

        builder.bucket(&bucket);

        builder.root(&root);

        // if credential env is set, use it. Otherwise, ADC will be used.
        let cred = std::env::var("GOOGLE_APPLICATION_CREDENTIALS");
        if let Ok(cred) = cred {
            builder.credential(&cred);
        }

        let op: Operator = Operator::create(builder)?.finish();
        Ok(Self {
            op,
            engine_type: EngineType::Gcs,
        })
    }
}
