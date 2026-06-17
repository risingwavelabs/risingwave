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

//! Iceberg V3 Sink with Primary Key Index
//!
//! This module implements three core executors for the Iceberg V3 sink that uses
//! Deletion Vectors (DVs) instead of Equality Delete files:
//!
//! 1. **Writer Executor** (Stateful): Maintains a PK index mapping primary keys to
//!    (`file_path`, `position`). Writes data files for inserts and emits
//!    (`file_path`, `position`) messages for deletes.
//!
//! 2. **DV Merger Executor** (Stateless): Consumes the Writer's (`file_path`, `position`) messages,
//!    merges delete positions with historical DVs, and reports the resulting DV files to meta.

mod dv_handler_impl;
mod dv_merger;
mod output_scan;
mod resolve_executor;
mod writer;
mod writer_impl;

pub use dv_handler_impl::DvHandlerImpl;
pub use dv_merger::DvMergerExecutor;
pub use resolve_executor::ResolveExecutor;
pub use writer::WriterExecutor;
pub use writer_impl::IcebergWriterImpl;
