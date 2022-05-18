// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod config;
pub mod enumerator;
pub mod source;
pub mod split;

pub use config::build_client;

const KINESIS_STREAM_NAME: &str = "kinesis.stream.name";
const KINESIS_STREAM_REGION: &str = "kinesis.stream.region";
const KINESIS_ENDPOINT: &str = "kinesis.endpoint";
const KINESIS_CREDENTIALS_ACCESS_KEY: &str = "kinesis.credentials.access";
const KINESIS_CREDENTIALS_SECRET_ACCESS_KEY: &str = "kinesis.credentials.secret";
const KINESIS_CREDENTIALS_SESSION_TOKEN: &str = "kinesis.credentials.session_token";
const KINESIS_ASSUMEROLE_ARN: &str = "kinesis.assumerole.arn";
const KINESIS_ASSUMEROLE_EXTERNAL_ID: &str = "kinesis.assumerole.external_id";
