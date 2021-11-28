// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{self},
    hash::Hasher,
    ptr,
};

use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_pb::hummock::Checksum;

use super::{HummockError, HummockResult};

unsafe fn u64(ptr: *const u8) -> u64 {
    ptr::read_unaligned(ptr as *const u64)
}

unsafe fn u32(ptr: *const u8) -> u32 {
    ptr::read_unaligned(ptr as *const u32)
}

#[inline]
pub fn bytes_diff<'a, 'b>(base: &'a [u8], target: &'b [u8]) -> &'b [u8] {
    let end = cmp::min(base.len(), target.len());
    let mut i = 0;
    unsafe {
        while i + 8 <= end {
            if u64(base.as_ptr().add(i)) != u64(target.as_ptr().add(i)) {
                break;
            }
            i += 8;
        }
        if i + 4 <= end && u32(base.as_ptr().add(i)) == u32(target.as_ptr().add(i)) {
            i += 4;
        }
        while i < end {
            if base.get_unchecked(i) != target.get_unchecked(i) {
                return target.get_unchecked(i..);
            }
            i += 1;
        }
        target.get_unchecked(end..)
    }
}

/// Calculate the CRC32 of the given data.
pub fn crc32_checksum(data: &[u8]) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() as u64
}

/// Calculate the ``XxHash`` of the given data.
pub fn xxhash64_checksum(data: &[u8]) -> u64 {
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(data);
    hasher.finish() as u64
}

/// Verify the checksum of the data equals the given checksum.
pub fn verify_checksum(chksum: &Checksum, data: &[u8]) -> HummockResult<()> {
    match chksum.algo() {
        ChecksumAlg::Crc32c => {
            if crc32_checksum(data) != chksum.get_sum() {
                return Err(HummockError::ChecksumMismatch);
            }
        }
        ChecksumAlg::XxHash64 => {
            if xxhash64_checksum(data) != chksum.get_sum() {
                return Err(HummockError::ChecksumMismatch);
            }
        }
    }
    Ok(())
}
