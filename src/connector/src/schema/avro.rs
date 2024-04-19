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

use apache_avro::Schema as AvroSchema;

use super::loader::{LoadedSchema, SchemaLoader};
use super::schema_registry::Subject;
use super::SchemaFetchError;

pub struct SchemaWithId {
    pub schema: Arc<AvroSchema>,
    pub id: i32,
}

/// Schema registry only
pub async fn fetch_schema(
    format_options: &BTreeMap<String, String>,
    topic: &str,
) -> Result<(SchemaWithId, SchemaWithId), SchemaFetchError> {
    let loader = SchemaLoader::from_format_options(topic, format_options)?;

    let (key_id, key_avro) = loader.load_key_schema().await?;
    let (val_id, val_avro) = loader.load_val_schema().await?;

    Ok((
        SchemaWithId {
            id: key_id,
            schema: Arc::new(key_avro),
        },
        SchemaWithId {
            id: val_id,
            schema: Arc::new(val_avro),
        },
    ))
}

impl LoadedSchema for AvroSchema {
    fn compile(primary: Subject, _: Vec<Subject>) -> Result<Self, SchemaFetchError> {
        AvroSchema::parse_str(&primary.schema.content)
            .map_err(|e| SchemaFetchError::SchemaCompile(e.into()))
    }
}
