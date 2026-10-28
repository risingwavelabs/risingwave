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

use crate::source::cdc::{CdcSourceRuntimeFlags, Postgres};

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Postgres {
    const CAN_BACKFILL: bool = true;
    const CAN_BACKFILL_IN_PARALLEL: bool = true;
    const CAN_USE_TRANSACTION_METADATA: bool = true;
    const NEEDS_OFFSET_COMMIT_ON_CHECKPOINT: bool = true;
}
