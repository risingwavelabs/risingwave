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

use std::collections::HashSet;
use std::sync::LazyLock;

pub static REDUCT_PROPS: LazyLock<HashSet<String>> = LazyLock::new(|| {
    HashSet::from([
        "properties.sasl.password".into(),
        "properties.ssl.key.password".into(),
        "auth.token".into(),
        "schema.registry.password".into(),
        "aws.credentials.secret_access_key".into(),
        "aws.credentials.session_token".into(),
        "password".into(),
        "jwt".into(),
        "nkey".into(),
        "pubsub.credentials".into(),
        "s3.secret.key".into(),
        "s3.credentials.secret".into(),
        "azblob.credentials.account_key".into(),
        "gcs.credentials".into(),
        "aws.credentials.secret_access_key".into(),
        "sqlserver.password".into(),
        "clickhouse.password".into(),
        "doris.password".into(),
        "starrocks.password".into(),
        "s3.credentials.secret".into(),
    ])
});
