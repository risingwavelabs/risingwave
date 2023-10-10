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

use std::ffi::CString;
use std::fs;
use std::path::Path;

use parking_lot::Once;
use tikv_jemalloc_ctl::{
    epoch as jemalloc_epoch, opt as jemalloc_opt, prof as jemalloc_prof, stats as jemalloc_stats,
};
use tokio::time::{self, Duration};

use super::AUTO_DUMP_SUFFIX;
use crate::config::HeapProfilingConfig;

pub struct HeapProfiler {
    config: HeapProfilingConfig,
    threshold_auto_dump_heap_profile: usize,
    jemalloc_dump_mib: jemalloc_prof::dump_mib,
    jemalloc_allocated_mib: jemalloc_stats::allocated_mib,
    jemalloc_epoch_mib: tikv_jemalloc_ctl::epoch_mib,
    /// If jemalloc profiling is enabled
    opt_prof: bool,
}

impl HeapProfiler {
    pub fn new(total_memory: usize, config: HeapProfilingConfig) -> Self {
        let threshold_auto_dump_heap_profile =
            (total_memory as f64 * config.threshold_auto as f64) as usize;
        let jemalloc_dump_mib = jemalloc_prof::dump::mib().unwrap();
        let jemalloc_allocated_mib = jemalloc_stats::allocated::mib().unwrap();
        let jemalloc_epoch_mib = jemalloc_epoch::mib().unwrap();
        let opt_prof = jemalloc_opt::prof::read().unwrap();

        Self {
            config,
            threshold_auto_dump_heap_profile,
            jemalloc_dump_mib,
            jemalloc_allocated_mib,
            jemalloc_epoch_mib,
            opt_prof,
        }
    }

    fn dump_heap_prof(&self, cur_used_memory_bytes: usize, prev_used_memory_bytes: usize) {
        if !self.config.enable_auto {
            return;
        }

        if cur_used_memory_bytes > self.threshold_auto_dump_heap_profile
            && prev_used_memory_bytes <= self.threshold_auto_dump_heap_profile
        {
            if !self.opt_prof {
                tracing::info!("Cannot dump heap profile because Jemalloc prof is not enabled");
                return;
            }

            let time_prefix = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S");
            let file_name = format!("{}.{}", time_prefix, AUTO_DUMP_SUFFIX);

            let file_path = Path::new(&self.config.dir)
                .join(&file_name)
                .to_str()
                .expect("file path is not valid utf8")
                .to_owned();
            let file_path_c = CString::new(file_path).expect("0 byte in file path");

            // FIXME(yuhao): `unsafe` here because `jemalloc_dump_mib.write` requires static lifetime
            if let Err(e) = self
                .jemalloc_dump_mib
                .write(unsafe { &*(file_path_c.as_c_str() as *const _) })
            {
                tracing::warn!("Auto Jemalloc dump heap file failed! {:?}", e);
            } else {
                tracing::info!("Successfully dumped heap profile to {}", file_name);
            }
        }
    }

    fn advance_jemalloc_epoch(&self, prev_jemalloc_allocated_bytes: usize) -> usize {
        if let Err(e) = self.jemalloc_epoch_mib.advance() {
            tracing::warn!("Jemalloc epoch advance failed! {:?}", e);
        }

        self.jemalloc_allocated_mib.read().unwrap_or_else(|e| {
            tracing::warn!("Jemalloc read allocated failed! {:?}", e);
            prev_jemalloc_allocated_bytes
        })
    }

    pub fn start(self) {
        static START: Once = Once::new();
        START.call_once(|| {
            fs::create_dir_all(&self.config.dir).unwrap();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(500));
                let mut prev_jemalloc_allocated_bytes = 0;
                loop {
                    interval.tick().await;
                    let jemalloc_allocated_bytes =
                        self.advance_jemalloc_epoch(prev_jemalloc_allocated_bytes);
                    self.dump_heap_prof(jemalloc_allocated_bytes, prev_jemalloc_allocated_bytes);
                    prev_jemalloc_allocated_bytes = jemalloc_allocated_bytes;
                }
            });
        })
    }
}
