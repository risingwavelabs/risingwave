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

use std::collections::HashMap;
use std::marker::PhantomData;

use risingwave_common::array::column::Column;
use risingwave_common::array::Op;
use risingwave_common::buffer::Bitmap;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;

pub struct DistinctAggDeduplicater<S: StateStore> {
    _phantom: PhantomData<S>,
}

impl<S: StateStore> DistinctAggDeduplicater<S> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    pub fn dedup_chunk(
        &self,
        ops: &[Op],
        columns: &[Column],
        visibilities: Vec<Option<Bitmap>>,
        dedup_tables: &mut HashMap<usize, StateTable<S>>,
    ) -> Vec<Option<Bitmap>> {
        todo!()
    }
}
