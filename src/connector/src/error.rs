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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("MySQL error: {0}")]
    MySql(
        #[from]
        #[backtrace]
        mysql_async::Error,
    ),

    #[error("Postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error(transparent)]
    Uncategorized(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
}

pub type ConnectorResult<T> = Result<T, ConnectorError>;
