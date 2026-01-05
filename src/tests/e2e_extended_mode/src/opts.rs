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

use clap::{Parser, ValueHint};

#[derive(Parser, Debug, Clone)]
pub struct Opts {
    /// Database name used to connect to pg.
    #[clap(name = "DB", long = "database", default_value = "dev")]
    pub pg_db_name: String,
    /// Username used to connect to postgresql.
    #[clap(name = "PG_USERNAME", short = 'u', long = "user", default_value="postgres", value_hint=ValueHint::Username)]
    pub pg_user_name: String,
    /// Postgresql server address to test against.
    #[clap(name = "PG_SERVER_ADDRESS", long = "host", default_value = "localhost")]
    pub pg_server_host: String,
    /// Postgresql server port to test against.
    #[clap(name = "PG_SERVER_PORT", short = 'p', long = "port")]
    pub pg_server_port: u16,
    #[clap(name = "PG_PASSWARD", long = "password", default_value = "")]
    pub pg_password: String,
}
