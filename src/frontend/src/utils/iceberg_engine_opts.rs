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

use std::ffi::OsString;
use std::iter;

use clap::Parser;

#[derive(Parser, Clone, Debug)]
pub struct IcebergEngineOpts {
    #[clap(long, env = "AWS_ENDPOINT_URL")]
    pub s3_endpoint: Option<String>,

    #[clap(long, env = "AWS_REGION")]
    pub s3_region: Option<String>,

    #[clap(long, env = "AWS_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    #[clap(long, env = "RW_DATA_DIRECTORY")]
    pub data_directory: Option<String>,

    #[clap(long, env = "AWS_ACCESS_KEY_ID")]
    pub s3_ak: Option<String>,

    #[clap(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub s3_sk: Option<String>,

    #[clap(long, env = "RW_SQL_ENDPOINT")]
    pub meta_store_endpoint: Option<String>,

    #[clap(long, env = "RW_SQL_DATABASE")]
    pub meta_store_database: Option<String>,

    #[clap(long, env = "RW_SQL_USERNAME")]
    pub meta_store_user: Option<String>,

    #[clap(long, env = "RW_SQL_PASSWORD")]
    pub meta_store_password: Option<String>,

    #[clap(long, env = "RW_BACKEND")]
    pub meta_store_backend: Option<String>,
}

impl Default for IcebergEngineOpts {
    fn default() -> Self {
        IcebergEngineOpts::parse_from(iter::empty::<OsString>())
    }
}
