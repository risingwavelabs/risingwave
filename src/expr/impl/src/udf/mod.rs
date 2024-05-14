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

// common imports for submodules
use anyhow::{Context as _, Result};
use arrow_array::RecordBatch;
use futures_util::stream::BoxStream;
use risingwave_expr::sig::{UdfRuntime, UdfRuntimeDescriptor, UDF_RUNTIMES};

// #[cfg(feature = "embedded-deno-udf")]
mod deno;
mod external;
// #[cfg(feature = "embedded-python-udf")]
mod python;
mod quickjs;
mod wasm;
