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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::nats::split::NatsSplit;
use crate::source::SplitImpl;

pub fn fill_adaptive_split(
    split_template: &SplitImpl,
    actor_in_use: &HashSet<u32>,
) -> ConnectorResult<BTreeMap<Arc<str>, SplitImpl>> {
    // Just Nats is adaptive for now
    if let SplitImpl::Nats(split) = split_template {
        let mut new_splits = BTreeMap::new();
        for actor_id in actor_in_use {
            let actor_id: Arc<str> = actor_id.to_string().into();
            new_splits.insert(
                actor_id.clone(),
                SplitImpl::Nats(NatsSplit::new(
                    split.subject.clone(),
                    actor_id,
                    split.start_sequence.clone(),
                )),
            );
        }
        tracing::debug!(
            "Filled adaptive splits for Nats source, {} splits in total",
            new_splits.len()
        );
        Ok(new_splits)
    } else {
        Err(ConnectorError::from(anyhow::anyhow!(
            "Unsupported split type, expect Nats SplitImpl but get {:?}",
            split_template
        )))
    }
}
