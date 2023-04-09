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

use std::path::{Path, PathBuf};

use itertools::Itertools;
use nix::fcntl::readlink;
use nix::sys::stat::stat;

/// Given a normal file path, returns the containing block device static file path (of the
/// partition).
pub fn file_stat_path(path: impl AsRef<Path>) -> PathBuf {
    let st_dev = stat(path.as_ref()).unwrap().st_dev;

    let major = unsafe { libc::major(st_dev) };
    let minor = unsafe { libc::minor(st_dev) };

    let dev = PathBuf::from("/dev/block").join(format!("{}:{}", major, minor));

    let linkname = readlink(&dev).unwrap();
    let devname = Path::new(linkname.as_os_str()).file_name().unwrap();
    dev_stat_path(devname.to_str().unwrap())
}

pub fn dev_stat_path(devname: &str) -> PathBuf {
    let classpath = Path::new("/sys/class/block").join(devname);
    let devclass = readlink(&classpath).unwrap();

    let devpath = Path::new(&devclass);
    Path::new("/sys")
        .join(devpath.strip_prefix("../..").unwrap())
        .join("stat")
}

#[derive(Debug, Clone, Copy)]
pub struct IoStat {
    /// read I/Os       requests      number of read I/Os processed
    pub read_ios: usize,
    /// read merges     requests      number of read I/Os merged with in-queue I/O
    pub read_merges: usize,
    /// read sectors    sectors       number of sectors read
    pub read_sectors: usize,
    /// read ticks      milliseconds  total wait time for read requests
    pub read_ticks: usize,
    /// write I/Os      requests      number of write I/Os processed
    pub write_ios: usize,
    /// write merges    requests      number of write I/Os merged with in-queue I/O
    pub write_merges: usize,
    /// write sectors   sectors       number of sectors written
    pub write_sectors: usize,
    /// write ticks     milliseconds  total wait time for write requests
    pub write_ticks: usize,
    /// in_flight       requests      number of I/Os currently in flight
    pub in_flight: usize,
    /// io_ticks        milliseconds  total time this block device has been active
    pub io_ticks: usize,
    /// time_in_queue   milliseconds  total wait time for all requests
    pub time_in_queue: usize,
    /// discard I/Os    requests      number of discard I/Os processed
    pub discard_ios: usize,
    /// discard merges  requests      number of discard I/Os merged with in-queue I/O
    pub discard_merges: usize,
    /// discard sectors sectors       number of sectors discarded
    pub discard_sectors: usize,
    /// discard ticks   milliseconds  total wait time for discard requests
    pub discard_ticks: usize,
    /// flush I/Os      requests      number of flush I/Os processed
    pub flush_ios: usize,
    /// flush ticks     milliseconds  total wait time for flush requests
    pub flush_ticks: usize,
}

/// Given the device static file path and get the iostat.
pub fn iostat(path: impl AsRef<Path>) -> IoStat {
    let content = std::fs::read_to_string(path.as_ref()).unwrap();
    let nums = content.split_ascii_whitespace().collect_vec();

    let read_ios = nums[0].parse().unwrap();
    let read_merges = nums[1].parse().unwrap();
    let read_sectors = nums[2].parse().unwrap();
    let read_ticks = nums[3].parse().unwrap();
    let write_ios = nums[4].parse().unwrap();
    let write_merges = nums[5].parse().unwrap();
    let write_sectors = nums[6].parse().unwrap();
    let write_ticks = nums[7].parse().unwrap();
    let in_flight = nums[8].parse().unwrap();
    let io_ticks = nums[9].parse().unwrap();
    let time_in_queue = nums[10].parse().unwrap();
    let discard_ios = nums[11].parse().unwrap();
    let discard_merges = nums[12].parse().unwrap();
    let discard_sectors = nums[13].parse().unwrap();
    let discard_ticks = nums[14].parse().unwrap();
    let flush_ios = nums[15].parse().unwrap();
    let flush_ticks = nums[16].parse().unwrap();

    IoStat {
        read_ios,
        read_merges,
        read_sectors,
        read_ticks,
        write_ios,
        write_merges,
        write_sectors,
        write_ticks,
        in_flight,
        io_ticks,
        time_in_queue,
        discard_ios,
        discard_merges,
        discard_sectors,
        discard_ticks,
        flush_ios,
        flush_ticks,
    }
}
