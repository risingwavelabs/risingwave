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

use std::path::Path;

use criterion::profiler::Profiler;

pub struct CpuProfiler<'a> {
    guard: Option<pprof::ProfilerGuard<'a>>,
}

impl CpuProfiler<'_> {
    pub fn new() -> Self {
        Self { guard: None }
    }
}

impl<'a> Profiler for CpuProfiler<'a> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        let guard = pprof::ProfilerGuardBuilder::default()
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();

        self.guard = Some(guard);
    }

    fn stop_profiling(&mut self, benchmark_id: &str, benchmark_dir: &Path) {
        let guard = self.guard.as_ref().unwrap();
        match guard.report().build() {
            Ok(report) => {
                let profile_svg = benchmark_dir.join(format!("{}.svg", benchmark_id));
                let file = std::fs::File::create(&profile_svg).unwrap();
                report.flamegraph(file).unwrap();
                tracing::info!("produced {:?}", profile_svg);
            }
            Err(err) => {
                panic!("failed to generate flamegraph: {}", err);
            }
        }
    }
}
