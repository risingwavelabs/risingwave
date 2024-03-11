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

use std::env;
use std::time::Duration;

use opendal::layers::{LoggingLayer, RetryLayer, TimeoutLayer};
use opendal::services::Gcs;
use opendal::Operator;

use super::{EngineType, OpendalObjectStore};
use crate::object::ObjectResult;

impl OpendalObjectStore {
    /// create opendal gcs engine.
    pub fn new_gcs_engine(bucket: String, root: String) -> ObjectResult<Self> {
        // Create gcs backend builder.
        let mut builder = Gcs::default();

        builder.bucket(&bucket);

        builder.root(&root);

        // if credential env is set, use it. Otherwise, ADC will be used.
        let cred = std::env::var("GOOGLE_APPLICATION_CREDENTIALS");
        if let Ok(cred) = cred {
            builder.credential(&cred);
        }
        let op_timeout = env::var("RW_OPENDAL_TIMEOUT")
            .unwrap_or("60".into())
            .parse()
            .unwrap();
        let op_speed = env::var("RW_OPENDAL_SPEED")
            .unwrap_or("1048576".into())
            .parse()
            .unwrap();
        let op_retry_min_delay = env::var("RW_OPENDAL_RETRY_MIN_DELAY")
            .unwrap_or("1".into())
            .parse()
            .unwrap();
        let op_retry_max_delay = env::var("RW_OPENDAL_RETRY_MAX_DELAY")
            .unwrap_or("5".into())
            .parse()
            .unwrap();
        let op_retry_max_times = env::var("RW_OPENDAL_RETRY_MAX_TIMES")
            .unwrap_or("10".into())
            .parse()
            .unwrap();
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(
                RetryLayer::default()
                    .with_min_delay(Duration::from_secs(op_retry_min_delay))
                    .with_max_delay(Duration::from_secs(op_retry_max_delay))
                    .with_max_times(op_retry_max_times),
            )
            .layer(
                TimeoutLayer::default()
                    .with_timeout(Duration::from_secs(op_timeout))
                    .with_speed(op_speed),
            )
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::Gcs,
        })
    }
}
