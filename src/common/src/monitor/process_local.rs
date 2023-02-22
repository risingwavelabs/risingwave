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

use std::io::{Error, ErrorKind, Result};
use std::time::Instant;

#[cfg(target_os = "linux")]
use super::CLOCK_TICK;

pub struct LocalProcessCollector {
    sched_time: f64,
    num_cpus: usize,
    last_cpu_collect_time: Instant,
}

#[cfg(target_os = "linux")]
impl LocalProcessCollector {
    fn sched_time() -> Result<f64> {
        let p = procfs::process::Process::myself()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        Ok(((p.stat.utime + p.stat.stime) as f64) / (*CLOCK_TICK as f64))
    }
}

#[cfg(target_os = "macos")]
impl LocalProcessCollector {
    fn sched_time() -> Result<f64> {
        let pid = unsafe { libc::getpid() };
        let clock_tick = unsafe {
            let mut info = mach::mach_time::mach_timebase_info::default();
            let errno = mach::mach_time::mach_timebase_info(&mut info as *mut _);
            if errno != 0 {
                1_f64
            } else {
                (info.numer / info.denom) as f64
            }
        };
        let proc_info = darwin_libproc::task_info(pid)
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

        let total =
            (proc_info.pti_total_user + proc_info.pti_total_system) as f64 * clock_tick / 1e9;

        Ok(total)
    }
}

impl LocalProcessCollector {
    pub fn new() -> Result<Self> {
        Ok(Self {
            sched_time: Self::sched_time()?,
            num_cpus: num_cpus::get(),
            last_cpu_collect_time: Instant::now(),
        })
    }

    pub fn cpu_avg(&mut self) -> Result<f64> {
        self.cpu_total().map(|v| v / self.num_cpus as f64)
    }

    pub fn cpu_total(&mut self) -> Result<f64> {
        // The number of seconds this process has been scheduled since last measurement.
        let sched_time_total = Self::sched_time()?;
        let sched_time_delta = sched_time_total - self.sched_time;
        assert!(sched_time_delta >= 0.0, "time went backwards");

        let now = Instant::now();
        let elapsed = now - self.last_cpu_collect_time;

        self.sched_time = sched_time_total;
        self.last_cpu_collect_time = now;

        Ok(sched_time_delta / elapsed.as_secs_f64())
    }
}
