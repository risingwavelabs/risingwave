// Copyright 2022 RisingWave Labs
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

use aws_sdk_kinesis::types::Record;
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use risingwave_common::types::{DatumRef, ScalarRefImpl};

use crate::source::{SourceMessage, SourceMeta, SplitId};

#[derive(Clone, Debug)]
pub struct KinesisMeta {
    // from `approximate_arrival_timestamp` of type `Option<aws_smithy_types::DateTime>`
    timestamp: Option<DateTime>,
}

impl KinesisMeta {
    pub fn extract_timestamp(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Timestamptz(
            self.timestamp?.to_chrono_utc().ok()?.into(),
        ))
    }
}

pub fn from_kinesis_record(value: &Record, split_id: SplitId) -> SourceMessage {
    SourceMessage {
        key: Some(value.partition_key.clone().into_bytes()),
        payload: Some(value.data.clone().into_inner()),
        offset: value.sequence_number.clone(),
        split_id,
        meta: SourceMeta::Kinesis(KinesisMeta {
            timestamp: value.approximate_arrival_timestamp,
        }),
    }
}
