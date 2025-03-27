// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;

use risingwave_common::catalog::ObjectId;
use risingwave_pb::common::Uint32Vector;
use risingwave_pb::stream_plan::backfill_order_strategy::Strategy as PbStrategy;
use risingwave_pb::stream_plan::{
    BackfillOrderFixed, BackfillOrderStrategy as PbBackfillOrderStrategy, BackfillOrderUnspecified,
};
use risingwave_sqlparser::ast::BackfillOrderStrategy;

use crate::Binder;
use crate::error::Result;

impl Binder {
    // TODO(kwannoel): Detect cycles
    pub fn bind_backfill_order_strategy(
        &mut self,
        backfill_order_strategy: Option<BackfillOrderStrategy>,
    ) -> Result<PbBackfillOrderStrategy> {
        let strategy = backfill_order_strategy.unwrap_or(BackfillOrderStrategy::None);
        let pb_strategy = match strategy {
            BackfillOrderStrategy::Auto
            | BackfillOrderStrategy::Default
            | BackfillOrderStrategy::None => PbStrategy::Unspecified(BackfillOrderUnspecified {}),
            BackfillOrderStrategy::Fixed(orders) => {
                let mut order: HashMap<ObjectId, Uint32Vector> = HashMap::new();
                for (start_name, end_name) in orders {
                    let start_relation_id = self.bind_backfill_relation_id_by_name(start_name)?;
                    let end_relation_id = self.bind_backfill_relation_id_by_name(end_name)?;
                    order
                        .entry(start_relation_id)
                        .or_default()
                        .data
                        .push(end_relation_id);
                }
                PbStrategy::Fixed(BackfillOrderFixed { order })
            }
        };
        Ok(PbBackfillOrderStrategy {
            strategy: Some(pb_strategy),
        })
    }
}
