// Copyright 2026 RisingWave Labs
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

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the P1 codec is intentionally not wired into a V3 reader until a later phase"
    )
)]

use bytes::{Buf, BufMut};
use risingwave_hummock_sdk::KeyComparator;
use risingwave_hummock_sdk::key::{EPOCH_LEN, TABLE_PREFIX_LEN};

use crate::hummock::sstable::utils::{
    put_length_prefixed_slice, xxhash64_checksum, xxhash64_verify,
};
use crate::hummock::sstable::{BlockMeta, MAGIC};
use crate::hummock::{HummockError, HummockResult};

const PARTITIONED_META_FOOTER_LEN: usize = 16;

// Lower bound for one MetaShardDesc: fixed-width fields plus smallest-key length prefix,
// excluding the variable-length smallest-key bytes.
const MIN_META_SHARD_DESC_ENCODED_LEN: usize = 20;

// Lower bound for one BlockMeta: fixed-width fields plus smallest-key length prefix,
// excluding the variable-length smallest-key bytes.
const MIN_BLOCK_META_ENCODED_LEN: usize = 24;

/// Persisted metadata format version for partitioned SST metadata.
pub(crate) const PARTITIONED_META_VERSION: u32 = 3;

/// Index entry that points to one encoded metadata shard body.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MetaShardDesc {
    block_count: u32,
    smallest_key: Vec<u8>,
    len: u32,
    checksum: u64,
}

impl MetaShardDesc {
    fn encode(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.block_count);
        put_length_prefixed_slice(&mut buf, &self.smallest_key);
        buf.put_u32_le(self.len);
        buf.put_u64_le(self.checksum);
    }

    fn decode(buf: &mut &[u8]) -> HummockResult<Self> {
        let block_count = get_u32_le_checked(buf, "meta shard block_count")?;
        let smallest_key = get_length_prefixed_slice_checked(buf, "meta shard smallest_key")?;
        let len = get_u32_le_checked(buf, "meta shard len")?;
        let checksum = get_u64_le_checked(buf, "meta shard checksum")?;
        Ok(Self {
            block_count,
            smallest_key,
            len,
            checksum,
        })
    }

    fn encoded_size(&self) -> usize {
        4 // block_count
            + 4 // smallest_key len
            + self.smallest_key.len()
            + 4 // len
            + 8 // checksum
    }

    fn validate_body(&self, body: &[u8]) -> HummockResult<()> {
        if body.len() != self.len as usize {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard body length mismatch: desc {} actual {}",
                self.len,
                body.len()
            )));
        }
        xxhash64_verify(body, self.checksum)
    }

    /// Decode a shard body after validating it against this descriptor.
    pub(crate) fn decode_body(&self, body: &[u8]) -> HummockResult<MetaShard> {
        self.validate_body(body)?;
        validate_encoded_full_key(&self.smallest_key, "meta shard smallest_key")?;
        MetaShard::decode_body(self.block_count, &self.smallest_key, body)
    }
}

/// Persisted index for locating V3 partitioned metadata shards.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MetaPartitionIndex {
    shards: Vec<MetaShardDesc>,
}

impl MetaPartitionIndex {
    fn validate(&self, meta_offset: u64) -> HummockResult<()> {
        if self.shards.is_empty() {
            return Err(HummockError::decode_error(
                "partitioned meta index has no shard",
            ));
        }
        let mut last_smallest_key: Option<&[u8]> = None;
        for (shard_idx, shard) in self.shards.iter().enumerate() {
            if shard.block_count == 0 {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} has no block",
                    shard_idx
                )));
            }
            validate_encoded_full_key(
                &shard.smallest_key,
                &format!("partitioned meta shard {shard_idx} smallest_key"),
            )?;
            if let Some(last_smallest_key) = last_smallest_key
                && KeyComparator::compare_encoded_full_key(last_smallest_key, &shard.smallest_key)
                    != std::cmp::Ordering::Less
            {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} smallest key is not strictly increasing",
                    shard_idx
                )));
            }

            last_smallest_key = Some(&shard.smallest_key);
        }
        self.shards
            .iter()
            .try_fold(0u32, |total, shard| {
                total.checked_add(shard.block_count).ok_or_else(|| {
                    HummockError::decode_error("partitioned meta block count overflow")
                })
            })
            .map(|_| ())?;
        let shard_bytes = self.shards.iter().try_fold(0u64, |total, shard| {
            total.checked_add(shard.len as u64).ok_or_else(|| {
                HummockError::decode_error("partitioned meta shard lengths overflow")
            })
        })?;
        if shard_bytes > meta_offset {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard bytes {shard_bytes} exceed index offset {meta_offset}",
            )));
        }
        Ok(())
    }

    pub(crate) fn encode_to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        buf.put_u32_le(
            self.shards
                .len()
                .try_into()
                .expect("too many metadata shards"),
        );
        for shard in &self.shards {
            shard.encode(&mut buf);
        }
        let checksum = xxhash64_checksum(&buf);
        buf.put_u64_le(checksum);
        buf.put_u32_le(PARTITIONED_META_VERSION);
        buf.put_u32_le(MAGIC);
        buf
    }

    pub(crate) fn decode(buf: &[u8], meta_offset: u64) -> HummockResult<Self> {
        let version = decode_partitioned_meta_footer_version(buf)?;
        if version != PARTITIONED_META_VERSION {
            return Err(HummockError::invalid_format_version(version));
        }

        let cursor = buf.len() - PARTITIONED_META_FOOTER_LEN;
        let checksum = (&buf[cursor..cursor + 8]).get_u64_le();
        let buf = &mut &buf[..cursor];
        xxhash64_verify(buf, checksum)?;

        let shard_count = get_u32_le_checked(buf, "partitioned meta shard_count")?;
        ensure_count_fits_remaining(
            shard_count as usize,
            buf.len(),
            MIN_META_SHARD_DESC_ENCODED_LEN,
            "partitioned meta shard_count",
        )?;
        let mut shards = Vec::with_capacity(shard_count as usize);
        for _ in 0..shard_count {
            shards.push(MetaShardDesc::decode(buf)?);
        }
        if !buf.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta index has {} trailing bytes",
                buf.len()
            )));
        }

        let index = Self { shards };
        index.validate(meta_offset)?;
        Ok(index)
    }

    fn encoded_size(&self) -> usize {
        4 // shard_count
            + self
                .shards
                .iter()
                .map(MetaShardDesc::encoded_size)
                .sum::<usize>()
            + PARTITIONED_META_FOOTER_LEN
    }
}

/// Persisted body for one metadata shard.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MetaShard {
    block_metas: Vec<BlockMeta>,
    filter: Vec<u8>,
}

impl MetaShard {
    pub(crate) fn encode_body_to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_body_size());
        for block_meta in &self.block_metas {
            block_meta.encode(&mut buf);
        }
        put_length_prefixed_slice(&mut buf, &self.filter);
        buf
    }

    fn decode_body(
        expected_block_count: u32,
        expected_smallest_key: &[u8],
        mut buf: &[u8],
    ) -> HummockResult<Self> {
        let block_meta_count = expected_block_count as usize;
        ensure_count_fits_remaining(
            block_meta_count,
            buf.len(),
            MIN_BLOCK_META_ENCODED_LEN,
            "meta shard block_count",
        )?;
        let mut block_metas = Vec::with_capacity(block_meta_count);
        for _ in 0..block_meta_count {
            block_metas.push(decode_block_meta_checked(&mut buf)?);
        }
        let filter = get_length_prefixed_slice_checked(&mut buf, "meta shard filter")?;
        if !buf.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard body has {} trailing bytes",
                buf.len()
            )));
        }
        if block_metas.is_empty() {
            return Err(HummockError::decode_error(
                "partitioned meta shard body has no block meta",
            ));
        }
        for (idx, block_meta) in block_metas.iter().enumerate() {
            validate_encoded_full_key(
                &block_meta.smallest_key,
                &format!("partitioned meta shard block meta {idx} smallest_key"),
            )?;
        }
        if block_metas[0].smallest_key != expected_smallest_key {
            return Err(HummockError::decode_error(
                "partitioned meta shard body smallest key mismatch",
            ));
        }
        for (idx, block_pair) in block_metas.windows(2).enumerate() {
            let left = &block_pair[0];
            let right = &block_pair[1];
            if KeyComparator::compare_encoded_full_key(&left.smallest_key, &right.smallest_key)
                != std::cmp::Ordering::Less
            {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard body block meta {} smallest key is not strictly increasing",
                    idx + 1
                )));
            }
        }
        Ok(Self {
            block_metas,
            filter,
        })
    }

    fn encoded_body_size(&self) -> usize {
        self
                .block_metas
                .iter()
                .map(BlockMeta::encoded_size)
                .sum::<usize>()
            + 4 // filter len
            + self.filter.len()
    }
}

/// Decode only the partitioned metadata footer version after validating the magic.
fn decode_partitioned_meta_footer_version(buf: &[u8]) -> HummockResult<u32> {
    if buf.len() < PARTITIONED_META_FOOTER_LEN {
        return Err(HummockError::decode_error(format!(
            "partitioned meta footer too short: {}",
            buf.len()
        )));
    }

    let magic = (&buf[buf.len() - 4..]).get_u32_le();
    if magic != MAGIC {
        return Err(HummockError::magic_mismatch(MAGIC, magic));
    }

    Ok((&buf[buf.len() - 8..buf.len() - 4]).get_u32_le())
}

fn ensure_remaining(buf: &[u8], len: usize, field: &str) -> HummockResult<()> {
    if buf.len() < len {
        return Err(HummockError::decode_error(format!(
            "partitioned meta {} too short: need {} bytes, remaining {}",
            field,
            len,
            buf.len()
        )));
    }
    Ok(())
}

fn ensure_count_fits_remaining(
    count: usize,
    remaining_len: usize,
    min_entry_len: usize,
    field: &str,
) -> HummockResult<()> {
    let max_count = remaining_len / min_entry_len;
    if count > max_count {
        return Err(HummockError::decode_error(format!(
            "partitioned meta {} {} exceeds remaining {} bytes for minimum entry size {}",
            field, count, remaining_len, min_entry_len
        )));
    }
    Ok(())
}

fn get_u32_le_checked(buf: &mut &[u8], field: &str) -> HummockResult<u32> {
    ensure_remaining(buf, 4, field)?;
    Ok(buf.get_u32_le())
}

fn get_u64_le_checked(buf: &mut &[u8], field: &str) -> HummockResult<u64> {
    ensure_remaining(buf, 8, field)?;
    Ok(buf.get_u64_le())
}

fn get_length_prefixed_slice_checked(buf: &mut &[u8], field: &str) -> HummockResult<Vec<u8>> {
    let len = get_u32_le_checked(buf, field)? as usize;
    ensure_remaining(buf, len, field)?;
    let value = buf[..len].to_vec();
    buf.advance(len);
    Ok(value)
}

fn validate_encoded_full_key(key: &[u8], field: &str) -> HummockResult<()> {
    const MIN_ENCODED_FULL_KEY_LEN: usize = TABLE_PREFIX_LEN + EPOCH_LEN;

    if key.len() < MIN_ENCODED_FULL_KEY_LEN {
        return Err(HummockError::decode_error(format!(
            "{field} too short for encoded full key: need at least {MIN_ENCODED_FULL_KEY_LEN} bytes, got {}",
            key.len()
        )));
    }
    Ok(())
}

fn decode_block_meta_checked(buf: &mut &[u8]) -> HummockResult<BlockMeta> {
    ensure_remaining(buf, 20, "block meta fixed fields")?;
    let offset = buf.get_u32_le();
    let len = buf.get_u32_le();
    let uncompressed_size = buf.get_u32_le();
    let total_key_count = buf.get_u32_le();
    let stale_key_count = buf.get_u32_le();
    let smallest_key = get_length_prefixed_slice_checked(buf, "block meta smallest_key")?;
    Ok(BlockMeta {
        smallest_key,
        offset,
        len,
        uncompressed_size,
        total_key_count,
        stale_key_count,
    })
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::panic::rw_catch_unwind;
    use risingwave_hummock_sdk::key::FullKey;

    use super::*;

    fn test_key(idx: usize) -> Vec<u8> {
        FullKey::for_test(
            TableId::new(1),
            format!("key-{idx:03}").into_bytes(),
            test_epoch(1),
        )
        .encode()
    }

    fn block_meta(idx: usize) -> BlockMeta {
        BlockMeta {
            smallest_key: test_key(idx),
            offset: (idx * 100) as u32,
            len: 32,
            uncompressed_size: 64,
            total_key_count: 10,
            stale_key_count: 1,
        }
    }

    fn shard(first_block_idx: u32, block_count: u32) -> MetaShard {
        let block_metas = (0..block_count)
            .map(|idx| block_meta(first_block_idx as usize + idx as usize))
            .collect();
        MetaShard {
            block_metas,
            filter: vec![9, 8, 7],
        }
    }

    fn sample_index() -> MetaPartitionIndex {
        let shards = [shard(0, 2), shard(2, 2)];
        let shards = shards
            .iter()
            .map(|shard| {
                let body = shard.encode_body_to_bytes();
                MetaShardDesc {
                    block_count: shard.block_metas.len() as u32,
                    smallest_key: shard.block_metas[0].smallest_key.clone(),
                    len: body.len() as u32,
                    checksum: xxhash64_checksum(&body),
                }
            })
            .collect();

        MetaPartitionIndex { shards }
    }

    fn encode_index_body_with_footer(body: &[u8]) -> Vec<u8> {
        let mut encoded = body.to_vec();
        encoded.put_u64_le(xxhash64_checksum(body));
        encoded.put_u32_le(PARTITIONED_META_VERSION);
        encoded.put_u32_le(MAGIC);
        encoded
    }

    #[test]
    fn test_partitioned_meta_index_round_trip() {
        let index = sample_index();
        let encoded = index.encode_to_bytes();

        assert_eq!(
            decode_partitioned_meta_footer_version(&encoded).unwrap(),
            PARTITIONED_META_VERSION
        );
        assert_eq!(MetaPartitionIndex::decode(&encoded, 4096).unwrap(), index);
    }

    #[test]
    fn test_partitioned_meta_fixed_wire_fixture() {
        const INDEX: &[u8] = &[
            0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x6b, 0x65, 0x79, 0x2d, 0x30, 0x30, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x5d, 0x00, 0x00, 0x00, 0x3f, 0x8c, 0x3d, 0xf7, 0x93, 0x02, 0xfc,
            0x86, 0x02, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6b,
            0x65, 0x79, 0x2d, 0x30, 0x30, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x5d, 0x00, 0x00, 0x00, 0x4c, 0x16, 0x91, 0x83, 0xae, 0x65, 0x49, 0x25, 0x39, 0x46,
            0x61, 0x95, 0xf4, 0x09, 0x3a, 0x38, 0x03, 0x00, 0x00, 0x00, 0x73, 0xab, 0x85, 0x57,
        ];
        const SHARD: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x0a, 0x00,
            0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x6b, 0x65, 0x79, 0x2d, 0x30, 0x30, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x64, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x0a,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, 0x6b, 0x65, 0x79, 0x2d, 0x30, 0x30, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x09, 0x08, 0x07,
        ];

        let index = MetaPartitionIndex::decode(INDEX, 4096).unwrap();
        assert_eq!(index.encode_to_bytes(), INDEX);
        assert_eq!(
            4096 - index
                .shards
                .iter()
                .map(|shard| shard.len as u64)
                .sum::<u64>(),
            3910
        );
        assert_eq!(
            4096 - index.shards[1..]
                .iter()
                .map(|shard| shard.len as u64)
                .sum::<u64>(),
            4003
        );

        let desc = &index.shards[0];
        assert_eq!(desc.block_count, 2);
        assert_eq!(desc.smallest_key, test_key(0));
        assert_eq!(desc.len as usize, SHARD.len());
        let shard = desc.decode_body(SHARD).unwrap();
        assert_eq!(shard.encode_body_to_bytes(), SHARD);
    }

    #[test]
    fn test_partitioned_meta_rejects_shards_larger_than_meta_prefix() {
        let index = sample_index();
        let encoded = index.encode_to_bytes();
        let shard_bytes = index
            .shards
            .iter()
            .map(|shard| shard.len as u64)
            .sum::<u64>();

        assert!(MetaPartitionIndex::decode(&encoded, shard_bytes - 1).is_err());
    }

    #[test]
    fn test_meta_shard_round_trip_and_desc_validation() {
        let shard = shard(0, 2);
        let body = shard.encode_body_to_bytes();
        let desc = MetaShardDesc {
            block_count: 2,
            smallest_key: shard.block_metas[0].smallest_key.clone(),
            len: body.len() as u32,
            checksum: xxhash64_checksum(&body),
        };

        assert_eq!(desc.decode_body(&body).unwrap(), shard);
    }

    #[test]
    fn test_partitioned_meta_footer_validation() {
        let index = sample_index();
        let mut encoded = index.encode_to_bytes();

        let magic_idx = encoded.len() - 1;
        encoded[magic_idx] ^= 1;
        assert!(decode_partitioned_meta_footer_version(&encoded).is_err());

        let mut encoded = index.encode_to_bytes();
        let version_idx = encoded.len() - 8;
        encoded[version_idx] ^= 1;
        assert!(MetaPartitionIndex::decode(&encoded, 4096).is_err());

        let mut encoded = index.encode_to_bytes();
        encoded[0] ^= 1;
        assert!(MetaPartitionIndex::decode(&encoded, 4096).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_bad_shard_ordering() {
        let mut index = sample_index();
        index.shards[1].smallest_key = index.shards[0].smallest_key.clone();
        assert!(MetaPartitionIndex::decode(&index.encode_to_bytes(), 4096).is_err());
    }

    #[test]
    fn test_meta_shard_rejects_count_first_key_and_block_order_mismatches() {
        let shard = shard(0, 2);
        let body = shard.encode_body_to_bytes();

        let mut bad_count = MetaShardDesc {
            block_count: 3,
            smallest_key: shard.block_metas[0].smallest_key.clone(),
            len: body.len() as u32,
            checksum: xxhash64_checksum(&body),
        };
        assert!(bad_count.decode_body(&body).is_err());

        bad_count.block_count = 2;
        bad_count.smallest_key = test_key(99);
        assert!(bad_count.decode_body(&body).is_err());

        let mut bad_order = shard;
        bad_order.block_metas[1].smallest_key = bad_order.block_metas[0].smallest_key.clone();
        let bad_order_body = bad_order.encode_body_to_bytes();
        let bad_order_desc = MetaShardDesc {
            block_count: 2,
            smallest_key: bad_order.block_metas[0].smallest_key.clone(),
            len: bad_order_body.len() as u32,
            checksum: xxhash64_checksum(&bad_order_body),
        };
        assert!(bad_order_desc.decode_body(&bad_order_body).is_err());
    }

    #[test]
    fn test_meta_shard_desc_rejects_body_len_or_checksum_mismatch() {
        let shard = shard(0, 1);
        let body = shard.encode_body_to_bytes();
        let desc = MetaShardDesc {
            block_count: 1,
            smallest_key: shard.block_metas[0].smallest_key.clone(),
            len: body.len() as u32,
            checksum: xxhash64_checksum(&body),
        };

        let bad_len = MetaShardDesc {
            block_count: desc.block_count,
            smallest_key: desc.smallest_key.clone(),
            len: desc.len + 1,
            checksum: desc.checksum,
        };
        assert!(bad_len.decode_body(&body).is_err());

        let mut bad_checksum = desc;
        bad_checksum.checksum ^= 1;
        assert!(bad_checksum.decode_body(&body).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_checksum_valid_malformed_index_body() {
        let mut truncated_body = Vec::new();
        truncated_body.put_u32_le(1); // shard_count
        truncated_body.put_u32_le(1); // block_count
        truncated_body.put_u32_le(16); // smallest_key len without enough bytes

        assert!(
            MetaPartitionIndex::decode(&encode_index_body_with_footer(&truncated_body), 4096)
                .is_err()
        );
    }

    #[test]
    fn test_partitioned_meta_rejects_checksum_valid_huge_shard_count() {
        let mut body = Vec::new();
        body.put_u32_le(u32::MAX); // shard_count

        assert!(MetaPartitionIndex::decode(&encode_index_body_with_footer(&body), 4096).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_checksum_valid_total_block_count_overflow() {
        let index = MetaPartitionIndex {
            shards: vec![
                MetaShardDesc {
                    block_count: u32::MAX,
                    smallest_key: test_key(0),
                    len: 0,
                    checksum: 0,
                },
                MetaShardDesc {
                    block_count: 1,
                    smallest_key: test_key(1),
                    len: 0,
                    checksum: 0,
                },
            ],
        };

        assert!(MetaPartitionIndex::decode(&index.encode_to_bytes(), 0).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_short_full_keys_without_panicking() {
        let short_key = vec![0; TABLE_PREFIX_LEN + EPOCH_LEN - 1];

        let mut short_shard_key = sample_index();
        short_shard_key.shards[1].smallest_key = short_key.clone();
        assert_decode_returns_err_without_panicking(|| {
            MetaPartitionIndex::decode(&short_shard_key.encode_to_bytes(), 4096)
        });

        let mut shard = shard(0, 2);
        shard.block_metas[1].smallest_key = short_key;
        let body = shard.encode_body_to_bytes();
        let desc = MetaShardDesc {
            block_count: shard.block_metas.len() as u32,
            smallest_key: shard.block_metas[0].smallest_key.clone(),
            len: body.len() as u32,
            checksum: xxhash64_checksum(&body),
        };
        assert_decode_returns_err_without_panicking(|| desc.decode_body(&body));
    }

    #[test]
    fn test_meta_shard_rejects_checksum_valid_malformed_shard_body() {
        let mut truncated_body = Vec::new();
        truncated_body.put_u32_le(0); // partial block meta fixed fields
        let desc = MetaShardDesc {
            block_count: 1,
            smallest_key: test_key(0),
            len: truncated_body.len() as u32,
            checksum: xxhash64_checksum(&truncated_body),
        };

        assert!(desc.decode_body(&truncated_body).is_err());
    }

    #[test]
    fn test_meta_shard_rejects_checksum_valid_huge_expected_block_count() {
        let body = Vec::new();

        let desc = MetaShardDesc {
            block_count: u32::MAX,
            smallest_key: test_key(0),
            len: 0,
            checksum: xxhash64_checksum(&body),
        };

        assert!(desc.decode_body(&body).is_err());
    }

    fn assert_decode_returns_err_without_panicking<T>(f: impl FnOnce() -> HummockResult<T>) {
        assert!(
            rw_catch_unwind(std::panic::AssertUnwindSafe(f))
                .expect("malformed metadata must not panic")
                .is_err()
        );
    }
}
