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

use serde_derive::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use crate::connector_common::MongodbCommon;

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MongodbConfig {
    #[serde(flatten)]
    pub common: MongodbCommon,

    pub r#type: String, // accept "append-only" or "upsert"

    /// The dynamic collection name where data should be sunk to. If specified, the field value will be used
    /// as the collection name. The collection name format is same as `collection.name`. If the field value is
    /// null or an empty string, then the `collection.name` will be used as a fallback destination, if both
    /// `collection.name.field` and `collection.name` are empty, then an error is printed in the log and the
    /// current sinking record is dropped.
    #[serde(rename = "collection.name.field")]
    pub collection_name_field: Option<String>,
}
