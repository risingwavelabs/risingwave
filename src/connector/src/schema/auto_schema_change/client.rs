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

use risingwave_rpc_client::MetaClient;

use crate::parser::schema_change::SchemaChangeEnvelope;

/// client for auto schema change
/// we may collect some metrics here
pub struct AutoSchemaChangeClient {
    meta_client: MetaClient,
}

impl AutoSchemaChangeClient {
    pub fn submit_schema_change(&self, schema_change: SchemaChangeEnvelope) -> anyhow::Result<()> {
        // TODO:

        Ok(())
    }
}
