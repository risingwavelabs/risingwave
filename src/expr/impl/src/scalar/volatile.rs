// Copyright 2026 RisingWave Labs
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

use chrono::Utc;
use risingwave_common::types::Timestamptz;
use risingwave_expr::function;
use uuid::Uuid;

/// Returns the wall-clock timestamp at evaluation time.
///
/// `volatile` tells the function registry this result can change across evaluations with the same
/// inputs, so stream planning treats it as an impure expression.
#[function("clock_timestamp() -> timestamptz", volatile)]
fn clock_timestamp() -> Timestamptz {
    Utc::now().into()
}

/// Generates a random UUID v4 in PostgreSQL-compatible text form.
#[function("gen_random_uuid() -> varchar", volatile)]
fn gen_random_uuid() -> Box<str> {
    Uuid::new_v4().to_string().into_boxed_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_timestamp() {
        assert!(clock_timestamp().timestamp_micros() > 0);
    }

    #[test]
    fn test_gen_random_uuid() {
        let uuid = gen_random_uuid();
        assert!(Uuid::parse_str(&uuid).is_ok());
    }
}
