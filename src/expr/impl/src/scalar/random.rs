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

use rand::random_range;
use risingwave_common::types::F64;
use risingwave_expr::function;

/// Generates a random float between 0 and 1 inclusive.
#[function("random() -> float8", volatile)]
fn random() -> F64 {
    let val: f64 = random_range(0.0..=1.0);
    F64::from(val)
}
