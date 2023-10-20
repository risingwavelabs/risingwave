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

use futures::future::BoxFuture;
use risingwave_connector::sink::SinkError;
use risingwave_pb::catalog::PbSink;

extern "Rust" {
    fn __exported_validate_sink(
        prost_sink_catalog: &PbSink,
    ) -> BoxFuture<'_, std::result::Result<(), SinkError>>;
}

pub async fn validate_sink(prost_sink_catalog: &PbSink) -> std::result::Result<(), SinkError> {
    unsafe { __exported_validate_sink(prost_sink_catalog).await }
}
