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

use prometheus::core::{Collector, Desc};
use prometheus::{IntCounter, IntGauge, Opts, Registry, proto};

#[cfg(target_os = "linux")]
use super::{CLOCK_TICK, PAGESIZE};

/// Monitors current process.
pub fn monitor_process(registry: &Registry) {
    let pc = ProcessCollector::new();
    registry.register(Box::new(pc)).unwrap()
}

/// A collector to collect process metrics.
struct ProcessCollector {
    descs: Vec<Desc>,
    cpu_total: IntCounter,
    vsize: IntGauge,
    rss: IntGauge,
    cpu_core_num: IntGauge,
}

impl Default for ProcessCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessCollector {
    fn new() -> Self {
        let mut descs = Vec::new();

        let cpu_total = IntCounter::with_opts(Opts::new(
            "process_cpu_seconds_total",
            "Total user and system CPU time spent in \
                 seconds.",
        ))
        .unwrap();
        descs.extend(cpu_total.desc().into_iter().cloned());

        let vsize = IntGauge::with_opts(Opts::new(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes.",
        ))
        .unwrap();
        descs.extend(vsize.desc().into_iter().cloned());

        let rss = IntGauge::with_opts(Opts::new(
            "process_resident_memory_bytes",
            "Resident memory size in bytes.",
        ))
        .unwrap();
        descs.extend(rss.desc().into_iter().cloned());

        let cpu_core_num =
            IntGauge::with_opts(Opts::new("process_cpu_core_num", "Cpu core num.")).unwrap();
        descs.extend(cpu_core_num.desc().into_iter().cloned());

        Self {
            descs,
            cpu_total,
            vsize,
            rss,
            cpu_core_num,
        }
    }
}

#[cfg(target_os = "linux")]
impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let p = match procfs::process::Process::myself() {
            Ok(p) => p,
            Err(..) => {
                // we can't construct a Process object, so there's no stats to gather
                return Vec::new();
            }
        };
        let stat = match p.stat() {
            Ok(stat) => stat,
            Err(..) => {
                // we can't get the stat, so there's no stats to gather
                return Vec::new();
            }
        };

        // memory
        self.vsize.set(stat.vsize as i64);
        self.rss.set(stat.rss as i64 * *PAGESIZE);

        // cpu
        let cpu_total_mfs = {
            let total = (stat.utime + stat.stime) / *CLOCK_TICK;
            let past = self.cpu_total.get();
            self.cpu_total.inc_by(total - past);
            self.cpu_total.collect()
        };

        self.cpu_core_num
            .set(rw_resource_util::cpu::total_cpu_available() as i64);

        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);
        mfs.extend(cpu_total_mfs);
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs.extend(self.cpu_core_num.collect());
        mfs
    }
}

#[cfg(target_os = "macos")]
impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let pid = unsafe { libc::getpid() };
        let clock_tick = unsafe {
            let mut info = mach2::mach_time::mach_timebase_info::default();
            let errno = mach2::mach_time::mach_timebase_info(&mut info as *mut _);
            if errno != 0 {
                1_f64
            } else {
                (info.numer / info.denom) as f64
            }
        };
        let proc_info = match darwin_libproc::task_info(pid) {
            Ok(info) => info,
            Err(_) => {
                return Vec::new();
            }
        };

        // memory
        self.vsize.set(proc_info.pti_virtual_size as i64);
        self.rss.set(proc_info.pti_resident_size as i64);

        // cpu
        let cpu_total_mfs = {
            // both pti_total_user and pti_total_system are returned in nano seconds
            let total =
                (proc_info.pti_total_user + proc_info.pti_total_system) as f64 * clock_tick / 1e9;
            let past = self.cpu_total.get();
            self.cpu_total.inc_by((total - past as f64) as u64);
            self.cpu_total.collect()
        };

        self.cpu_core_num
            .set(rw_resource_util::cpu::total_cpu_available() as i64);

        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);
        mfs.extend(cpu_total_mfs);
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs.extend(self.cpu_core_num.collect());
        mfs
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // fake number
        self.vsize.set(100 * 1000);
        self.rss.set(100 * 1000);

        // cpu
        let cpu_total_mfs = {
            self.cpu_total.inc_by(10);
            self.cpu_total.collect()
        };

        self.cpu_core_num
            .set(rw_resource_util::cpu::total_cpu_available() as i64);

        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);
        mfs.extend(cpu_total_mfs);
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs.extend(self.cpu_core_num.collect());
        mfs
    }
}
