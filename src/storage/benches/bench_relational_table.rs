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

use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
#[cfg(target_os = "macos")]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use futures::future::BoxFuture;
use futures::FutureExt;
#[cfg(target_os = "macos")]
use libc::F_NOCACHE;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::state_table::{RowBasedStateTable, StateTable};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::spawn_blocking;

#[derive(Default)]
struct BenchStats {
    total_time_millis: u128,
    total_count: usize,
}

impl Display for BenchStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "op {} times, avg time: {} ms",
            self.total_count,
            self.total_time_millis as f64 / self.total_count as f64
        )
    }
}

struct Timer<'a> {
    stats: &'a mut BenchStats,
    start_time: SystemTime,
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        let end_time = SystemTime::now();
        let duration = end_time
            .duration_since(self.start_time)
            .unwrap()
            .as_millis();
        self.stats.total_time_millis += duration;
        self.stats.total_count += 1;
    }
}

impl BenchStats {
    fn start_timer(&mut self) -> Timer {
        Timer {
            stats: self,
            start_time: SystemTime::now(),
        }
    }
}

const ROW_COUNT: usize = 1000;
const COLUMN_COUNR: usize = 1_000;
fn gen_cell_based_table(row_count: usize, column_count: usize) -> StateTable<MemoryStateStore> {
    let state_store = MemoryStateStore::new();
    let mut column_descs = vec![];
    for col_id in 1..=column_count {
        column_descs.push(ColumnDesc::unnamed(
            ColumnId::from(col_id as i32),
            DataType::Int32,
        ));
    }

    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let cell_based_state_table = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs,
        order_types,
        pk_index,
    );
    cell_based_state_table
}

fn gen_dataset(row_count: usize, column_count: usize) -> Vec<Row> {
    let mut rows = vec![];
    for row_id in 1..=row_count {
        let mut row = vec![];
        for col_id in 1..=column_count {
            row.push(Some((col_id as i32).into()));
        }
        rows.push(Row::new(row));
    }
    rows
}

fn gen_data(dataset: &[Row]) -> Row {
    let mut data = vec![];
    for entry in dataset.iter() {
        
    }
    risingwave_common::array::Row(data)
}


fn insert_into_table() {
    let mut cell_based_table = gen_cell_based_table(ROW_COUNT, COLUMN_COUNR);
    let rows = gen_dataset(ROW_COUNT, COLUMN_COUNR);
    for row in rows {
        cell_based_table.insert(row);
    }
    cell_based_table.commit(0);
}

fn bench_relational_table(c: &mut Criterion){

    for column_count in [8, 16, 32, 64]{
        let dataset = gen_dataset(100, column_count);
        c.bench_with_input(
            BenchmarkId::new(format!("buffer - vsize: {}B", column_count), ""),
            &dataset,
            |b, dataset| b.iter(|| gen_data(dataset)),
        );
    }
   

}

criterion_group!(benches, bench_relational_table);
criterion_main!(benches);