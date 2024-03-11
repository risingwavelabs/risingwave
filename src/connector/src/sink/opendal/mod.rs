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

pub mod s3;
// pub mod gcs;

pub const GCS_SINK: &str = "gcs";

// pub trait OpendalSink: Send + Sync + 'static + Clone + PartialEq {
//     type Properties: SourceProperties + Send + Sync;

//     fn new_sink_engine(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>>;
// }

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub struct OpendalS3;

// impl OpendalSink for OpendalS3 {
//     type Properties = OpendalS3Properties;

//     fn new_sink_engine(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>> {
//        todo!()
//     }
// }

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub struct OpendalGcs;

// impl OpendalSink for OpendalGcs {
//     type Properties = GcsProperties;

//     fn new_sink_engine(properties: Self::Properties) -> ConnectorResult<OpendalEnumerator<Self>> {
//         OpendalEnumerator::new_gcs_source(properties)
//     }
// }
