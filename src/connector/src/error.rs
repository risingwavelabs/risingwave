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

use risingwave_common::error::v2::def_anyhow_newtype;

def_anyhow_newtype! {
    pub ConnectorError,

    // TODO(error-handling): Remove implicit contexts below and specify ad-hoc context for each conversion.
    mysql_async::Error => "MySQL error",
    tokio_postgres::Error => "Postgres error",
}

pub type ConnectorResult<T> = Result<T, ConnectorError>;
