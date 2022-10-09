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

use std::time::{Duration, Instant};

use bcc::perf_event::PerfMapBuilder;
use bcc::{BccError, Kprobe, Kretprobe, BPF};
use itertools::Itertools;
use tokio::fs::read_to_string;
use tokio::sync::oneshot;

use crate::Args;

const BPF_BUFFER_TRACE_MAGIC: u64 = 0xdeadbeefdeadbeef;

const FNAME_MAX_WIDTH: usize = 30;
const DURATION_MAX_WIDTH: usize = 10;
const OCCUPANCY_MAX_WIDTH: usize = 50;

enum Probe<'a> {
    Kprobe { function: &'a str, handler: &'a str },
    Kretprobe { function: &'a str, handler: &'a str },
}

impl<'a> Probe<'a> {
    fn attach(self, bpf: &mut BPF) -> Result<(), BccError> {
        match self {
            Self::Kprobe { function, handler } => Kprobe::new()
                .function(function)
                .handler(handler)
                .attach(bpf),
            Self::Kretprobe { function, handler } => Kretprobe::new()
                .function(function)
                .handler(handler)
                .attach(bpf),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
struct Event {
    magic: u64,
    vfs_read_enter_ts: u64,
    vfs_read_leave_ts: u64,
    ext4_file_read_iter_enter_ts: u64,
    ext4_file_read_iter_leave_ts: u64,
    iomap_dio_rw_enter_ts: u64,
    iomap_dio_rw_leave_ts: u64,
    filemap_write_and_wait_range_enter_ts: u64,
    filemap_write_and_wait_range_leave_ts: u64,
}

pub async fn bpf(args: Args, mut stop: oneshot::Receiver<()>) {
    let text = r#"
#include <uapi/linux/ptrace.h>
#include <linux/fs.h>
#include <linux/blk-mq.h>

#define PMAX 128

#define BPF_BUFFER_TRACE_MAGIC BBTM

typedef unsigned __int128 u128;

struct data_t {
    u64 magic;
    u64 vfs_read_enter_ts;
    u64 vfs_read_leave_ts;
    u64 ext4_file_read_iter_enter_ts;
    u64 ext4_file_read_iter_leave_ts;
    u64 iomap_dio_rw_enter_ts;
    u64 iomap_dio_rw_leave_ts;
    u64 filemap_write_and_wait_range_enter_ts;
    u64 filemap_write_and_wait_range_leave_ts;
};

BPF_HASH(tss, u64, struct data_t);
BPF_PERF_OUTPUT(events);

static bool scmp(unsigned char *s1, unsigned char *s2) {
    char *c1 = s1, *c2 = s2;
    while (*c1 != 0 && *c2 != 0 && *c1 == *c2) {
        c1++; c2++;
    }
    if (*c1 == 0 && *c2 == 0) return true;
    return false;
}

int vfs_read_enter(struct pt_regs *ctx, struct file *file, char *buf, size_t count, long long *pos) {
    u64 id = bpf_get_current_pid_tgid();

    unsigned char target[] = "cache\x0";

    u64 ts = bpf_ktime_get_ns();

    if ((u64)file->f_op != (u64)EXT4_FILE_OPERATIONS) return 0;
    
    if (!scmp(&file->f_path.dentry->d_iname[0], &target[0])) return 0;

    u64 magic = *((u64 *)buf);
    
    struct data_t data = {0};
    data.vfs_read_enter_ts = ts;
    data.magic = magic;

    tss.update(&id, &data);

    return 0;
}

int vfs_read_leave(struct pt_regs *ctx, struct file *file, char *buf, size_t count, long long *pos) {
    u64 id = bpf_get_current_pid_tgid();

    u64 ts = bpf_ktime_get_ns();
    
    struct data_t *data = tss.lookup(&id);
    if (data == 0) return 0;
    data->vfs_read_leave_ts = ts;

    events.perf_submit(ctx, data, sizeof(*data));

    tss.delete(&id);
    
    return 0;
}

int ext4_file_read_iter_enter(struct pt_regs *ctx, struct kiocb *iocb, struct iov_iter *to) {
    u64 id = bpf_get_current_pid_tgid();
    
    u64 ts = bpf_ktime_get_ns();

    struct data_t *data = tss.lookup(&id);
    if (data == 0) return 0;
    data->ext4_file_read_iter_enter_ts = ts;

    return 0;
}

int ext4_file_read_iter_leave(struct pt_regs *ctx, struct kiocb *iocb, struct iov_iter *to) {
    u64 id = bpf_get_current_pid_tgid();
    
    u64 ts = bpf_ktime_get_ns();
    
    struct data_t *data = tss.lookup(&id);
    if (data == 0) return 0;
    data->ext4_file_read_iter_leave_ts = ts;
    
    return 0;
}

int iomap_dio_rw_enter(struct pt_regs *ctx, struct kiocb *iocb, struct iov_iter *iter) {
    u64 id = bpf_get_current_pid_tgid();
    
    u64 ts = bpf_ktime_get_ns();

    struct data_t *data = tss.lookup(&id);
    if (data == 0) return 0;
    data->iomap_dio_rw_enter_ts = ts;

    return 0;
}

int iomap_dio_rw_leave(struct pt_regs *ctx, struct kiocb *iocb, struct iov_iter *iter) {
    u64 id = bpf_get_current_pid_tgid();
    
    u64 ts = bpf_ktime_get_ns();

    struct data_t *data = tss.lookup(&id);
    if (data == 0) return 0;
    data->iomap_dio_rw_leave_ts = ts;

    return 0;
}

int filemap_write_and_wait_range_enter(struct pt_regs *ctx, struct address_space *mapping, long long lstart, long long lend) {
    u64 id = bpf_get_current_pid_tgid();
    
    u64 ts = bpf_ktime_get_ns();

    struct data_t *data = tss.lookup(&id);
    if (data == 0) return 0;
    data->filemap_write_and_wait_range_enter_ts = ts;

    return 0;
}

int filemap_write_and_wait_range_leave(struct pt_regs *ctx, struct address_space *mapping, long long lstart, long long lend) {
    u64 id = bpf_get_current_pid_tgid();
    
    u64 ts = bpf_ktime_get_ns();

    struct data_t *data = tss.lookup(&id);
    if (data == 0) return 0;
    data->filemap_write_and_wait_range_leave_ts = ts;
    
    return 0;
}
"#;

    // HINT: Kernel should be built with CONFIG_KALLSYMS_ALL.
    // HINT: Should run with sudo.
    let syms = read_to_string("/proc/kallsyms").await.unwrap();
    let ext4sym = syms
        .split('\n')
        .map(|line| line.split_whitespace().collect_vec())
        .filter(|items| match items.get(2) {
            None => false,
            Some(&name) => name == "ext4_file_operations",
        })
        .at_most_one()
        .unwrap()
        .expect("no ext4_file_operations in /proc/kallsyms");
    let text = text.replace("EXT4_FILE_OPERATIONS", &format!("0x{}", ext4sym[0]));
    let text = text.replace("BBTM", &format!("{:#0x}", BPF_BUFFER_TRACE_MAGIC));

    let mut bpf = BPF::new(&text).unwrap();

    let probes = [
        Probe::Kprobe {
            function: "vfs_read",
            handler: "vfs_read_enter",
        },
        Probe::Kretprobe {
            function: "vfs_read",
            handler: "vfs_read_leave",
        },
        Probe::Kprobe {
            function: "ext4_file_read_iter",
            handler: "ext4_file_read_iter_enter",
        },
        Probe::Kretprobe {
            function: "ext4_file_read_iter",
            handler: "ext4_file_read_iter_leave",
        },
        Probe::Kprobe {
            function: "iomap_dio_rw",
            handler: "iomap_dio_rw_enter",
        },
        Probe::Kretprobe {
            function: "iomap_dio_rw",
            handler: "iomap_dio_rw_leave",
        },
        Probe::Kprobe {
            function: "filemap_write_and_wait_range",
            handler: "filemap_write_and_wait_range_enter",
        },
        Probe::Kretprobe {
            function: "filemap_write_and_wait_range",
            handler: "filemap_write_and_wait_range_leave",
        },
    ];

    for probe in probes {
        probe.attach(&mut bpf).unwrap();
    }

    let table = bpf.table("events").unwrap();

    let mut perf_map = PerfMapBuilder::new(table, move || {
        let slow = args.slow; // ms
        Box::new(move |raw| {
            let data = unsafe { std::ptr::read_unaligned(raw.as_ptr() as *const Event) };
            if data.magic != BPF_BUFFER_TRACE_MAGIC {
                return;
            }
            let duration = data.vfs_read_leave_ts - data.vfs_read_enter_ts; // ns
            if duration < slow * 1_000_000 {
                return;
            }
            print_slow_read(&data);
        })
    })
    .build()
    .unwrap();

    let start = Instant::now();

    loop {
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= args.time {
            return;
        }

        perf_map.poll(10);
    }
}

fn print_slow_read(data: &Event) {
    println!("{data:#?}");

    let start = data.vfs_read_enter_ts;
    let end = data.vfs_read_leave_ts;

    print_occupancy(
        "vfs_read",
        data.vfs_read_enter_ts,
        data.vfs_read_leave_ts,
        start,
        end,
    );
    print_occupancy(
        "ext4_file_read_iter",
        data.ext4_file_read_iter_enter_ts,
        data.ext4_file_read_iter_leave_ts,
        start,
        end,
    );
    print_occupancy(
        "iomap_dio_rw",
        data.iomap_dio_rw_enter_ts,
        data.iomap_dio_rw_leave_ts,
        start,
        end,
    );
    print_occupancy(
        "filemap_write_and_wait_range",
        data.filemap_write_and_wait_range_enter_ts,
        data.filemap_write_and_wait_range_leave_ts,
        start,
        end,
    );
}

fn print_occupancy(fname: &str, enter: u64, leave: u64, start: u64, end: u64) {
    let total = end - start;
    let offset = (((enter - start) as f64 / total as f64) * OCCUPANCY_MAX_WIDTH as f64) as usize;
    let len = std::cmp::max(
        1,
        (((leave - enter) as f64 / total as f64) * OCCUPANCY_MAX_WIDTH as f64) as usize,
    );
    println!(
        "{:FNAME_MAX_WIDTH$} | {: >DURATION_MAX_WIDTH$.3?} | {: <offset$}{:=<len$}",
        fname,
        Duration::from_nanos(leave - enter),
        "",
        ""
    );
}
