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

use risingwave_common::types::Datum;
use risingwave_expr::expr::BoxedExpression;


pub struct BoundedOutOfOrdernessGenerator {
    max_out_of_orderness_expr: BoxedExpression,
    current_max_timestamp: Datum,
}

impl BoundedOutOfOrdernessGenerator {
    fn on_event(&mut self, event_timestamp: Datum) {
        self.current_max_timestamp = std::cmp::max(self.current_max_timestamp, event_timestamp);
    }
}

