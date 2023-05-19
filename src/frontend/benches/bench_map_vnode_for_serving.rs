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

use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use risingwave_frontend::scheduler::worker_node_manager::map_vnode_for_serving;
use risingwave_pb::common::ParallelUnit;

fn bench_map_vnode_for_serving(_c: &mut Criterion) {
    let mut stats: Vec<(u32, u32)> = vec![];
    let mut rng = rand::thread_rng();
    let pu_id_scale: u32 = (rng.gen::<f32>() * 10000000.0) as u32;
    let fragment_id_scale_offset: u32 = (rng.gen::<f32>() * 10000000.0) as u32;
    let total_pu_count = 256;
    let mut pus = vec![];
    for id_base in 0..total_pu_count {
        pus.push(ParallelUnit {
            id: id_base + pu_id_scale,
            ..Default::default()
        });
    }
    for fragment_id in fragment_id_scale_offset..fragment_id_scale_offset + 1000 {
        let result = map_vnode_for_serving(&pus, fragment_id).unwrap();
        let used_pu_count = result.iter_unique().count() as u32;
        stats.push((fragment_id, used_pu_count));
    }
    stats.sort_by_key(|(_, score)| *score);
    let get_percentile = |p: usize| {
        let pos = std::cmp::min(stats.len() * p / 100, stats.len() - 1);
        (
            stats[pos].1,
            total_pu_count,
            stats[pos].1 as f64 * 100.0 / total_pu_count as f64,
        )
    };
    println!("{} runs", stats.len());
    let p = get_percentile(0);
    println!("min {}/{} {}%", p.0, p.1, p.2);
    let p = get_percentile(10);
    println!("p10 {}/{} {}%", p.0, p.1, p.2);
    let p = get_percentile(50);
    println!("p50 {}/{} {}%", p.0, p.1, p.2);
    let p = get_percentile(90);
    println!("p90 {}/{} {}%", p.0, p.1, p.2);
    let p = get_percentile(100);
    println!("max {}/{} {}%", p.0, p.1, p.2);
}

criterion_group!(benches, bench_map_vnode_for_serving);
criterion_main!(benches);
