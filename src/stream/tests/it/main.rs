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

// tests
mod eowc;
mod project_set;

// utils
mod prelude {
    pub use expect_test::{expect, expect_file};
    pub use risingwave_stream::executor::test_utils::prelude::*;

    pub use crate::snapshot::*;
}
mod snapshot;
