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

use bytes::{Buf, BufMut};
use risingwave_hummock_sdk::KeyComparator;
use risingwave_hummock_sdk::key::{EPOCH_LEN, TABLE_PREFIX_LEN};
use serde::{Deserialize, Serialize};

use crate::hummock::sstable::utils::{
    put_length_prefixed_slice, xxhash64_checksum, xxhash64_verify,
};
use crate::hummock::sstable::{BlockMeta, MAGIC};
use crate::hummock::{HummockError, HummockResult};

const PARTITIONED_META_FOOTER_LEN: usize = 16;

// Lower bound for one MetaShardDesc: fixed-width fields plus smallest-key length prefix,
// excluding the variable-length smallest-key bytes.
const MIN_META_SHARD_DESC_ENCODED_LEN: usize = 28;

// Lower bound for one BlockMeta: fixed-width fields plus smallest-key length prefix,
// excluding the variable-length smallest-key bytes.
const MIN_BLOCK_META_ENCODED_LEN: usize = 24;

/// Persisted metadata format version for partitioned SST metadata.
pub const PARTITIONED_META_VERSION: u32 = 3;

/// Index entry that points to one encoded metadata shard body.
#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetaShardDesc {
    pub shard_idx: u32,
    pub first_block_idx: u32,
    pub block_count: u32,
    pub smallest_key: Vec<u8>,
    pub offset: u64,
    pub len: u32,
    pub checksum: u64,
}

impl MetaShardDesc {
    fn encode(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.block_count);
        put_length_prefixed_slice(&mut buf, &self.smallest_key);
        buf.put_u64_le(self.offset);
        buf.put_u32_le(self.len);
        buf.put_u64_le(self.checksum);
    }

    fn decode(shard_idx: u32, first_block_idx: u32, buf: &mut &[u8]) -> HummockResult<Self> {
        let block_count = get_u32_le_checked(buf, "meta shard block_count")?;
        let smallest_key = get_length_prefixed_slice_checked(buf, "meta shard smallest_key")?;
        let offset = get_u64_le_checked(buf, "meta shard offset")?;
        let len = get_u32_le_checked(buf, "meta shard len")?;
        let checksum = get_u64_le_checked(buf, "meta shard checksum")?;
        Ok(Self {
            shard_idx,
            first_block_idx,
            block_count,
            smallest_key,
            offset,
            len,
            checksum,
        })
    }

    fn encoded_size(&self) -> usize {
        4 // block_count
            + 4 // smallest_key len
            + self.smallest_key.len()
            + 8 // offset
            + 4 // len
        + 8 // checksum
    }

    fn validate_body(&self, body: &[u8]) -> HummockResult<()> {
        if body.len() != self.len as usize {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} body length mismatch: desc {} actual {}",
                self.shard_idx,
                self.len,
                body.len()
            )));
        }
        xxhash64_verify(body, self.checksum)
    }

    /// Decode a shard body after validating it against this descriptor.
    pub fn decode_body(&self, body: &[u8]) -> HummockResult<MetaShard> {
        self.validate_body(body)?;
        validate_encoded_full_key(&self.smallest_key, "meta shard smallest_key")?;
        MetaShard::decode_body(
            self.shard_idx,
            self.first_block_idx,
            self.block_count,
            &self.smallest_key,
            body,
        )
    }
}

/// Persisted index for locating V3 partitioned metadata shards.
#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetaPartitionIndex {
    pub estimated_size: u32,
    pub key_count: u32,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub block_count: u32,
    pub shard_count: u32,
    pub filter_type: u32,
    pub shards: Vec<MetaShardDesc>,
}

impl MetaPartitionIndex {
    fn validate(&self, encoded_len: usize) -> HummockResult<()> {
        if self.shard_count as usize != self.shards.len() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard_count mismatch: header {} actual {}",
                self.shard_count,
                self.shards.len()
            )));
        }
        if self.block_count == 0 {
            return Err(HummockError::decode_error(
                "partitioned meta index has no data block",
            ));
        }
        if self.shards.is_empty() {
            return Err(HummockError::decode_error(
                "partitioned meta index has no shard",
            ));
        }
        validate_encoded_full_key(&self.smallest_key, "partitioned meta index smallest_key")?;
        validate_encoded_full_key(&self.largest_key, "partitioned meta index largest_key")?;
        if KeyComparator::compare_encoded_full_key(&self.smallest_key, &self.largest_key)
            == std::cmp::Ordering::Greater
        {
            return Err(HummockError::decode_error(
                "partitioned meta index has inverted boundary keys",
            ));
        }
        if self.shards[0].smallest_key != self.smallest_key {
            return Err(HummockError::decode_error(
                "partitioned meta first shard smallest key mismatch",
            ));
        }
        if self.estimated_size as usize <= encoded_len {
            return Err(HummockError::decode_error(format!(
                "partitioned meta estimated size {} too small for index {}",
                self.estimated_size, encoded_len
            )));
        }

        let index_offset = self.estimated_size as usize - encoded_len;
        let mut expected_first_block_idx = 0u32;
        let mut expected_shard_offset = None;
        let mut last_smallest_key: Option<&[u8]> = None;
        for (shard_idx, shard) in self.shards.iter().enumerate() {
            if shard.shard_idx as usize != shard_idx {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard idx mismatch: expected {} actual {}",
                    shard_idx, shard.shard_idx
                )));
            }
            if shard.block_count == 0 {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} has no block",
                    shard_idx
                )));
            }
            if shard.first_block_idx != expected_first_block_idx {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} first block mismatch: expected {} actual {}",
                    shard_idx, expected_first_block_idx, shard.first_block_idx
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

            let shard_offset = shard.offset as usize;
            let shard_end = shard_offset
                .checked_add(shard.len as usize)
                .ok_or_else(|| {
                    HummockError::decode_error(format!(
                        "partitioned meta shard {} offset overflow",
                        shard_idx
                    ))
                })?;
            if shard_end > index_offset {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} range {}..{} crosses index offset {}",
                    shard_idx, shard_offset, shard_end, index_offset
                )));
            }
            if let Some(expected_shard_offset) = expected_shard_offset
                && shard_offset != expected_shard_offset
            {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} offset mismatch: expected {} actual {}",
                    shard_idx, expected_shard_offset, shard_offset
                )));
            }
            expected_shard_offset = Some(shard_end);
            last_smallest_key = Some(&shard.smallest_key);
            expected_first_block_idx = expected_first_block_idx
                .checked_add(shard.block_count)
                .ok_or_else(|| {
                    HummockError::decode_error("partitioned meta block count overflow")
                })?;
        }
        if expected_shard_offset != Some(index_offset) {
            return Err(HummockError::decode_error(format!(
                "partitioned meta final shard ends at {:?}, expected index offset {}",
                expected_shard_offset, index_offset
            )));
        }
        if expected_first_block_idx != self.block_count {
            return Err(HummockError::decode_error(format!(
                "partitioned meta block count mismatch: header {} actual {}",
                self.block_count, expected_first_block_idx
            )));
        }
        Ok(())
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let encoded_size = self.encoded_size();
        let mut buf = Vec::with_capacity(encoded_size);
        self.encode_to(&mut buf);
        buf
    }

    fn encode_to(&self, mut buf: impl BufMut + AsRef<[u8]>) {
        let start = buf.as_ref().len();
        self.encode_body(&mut buf);
        let end = buf.as_ref().len();

        let checksum = xxhash64_checksum(&buf.as_ref()[start..end]);
        buf.put_u64_le(checksum);
        buf.put_u32_le(PARTITIONED_META_VERSION);
        buf.put_u32_le(MAGIC);
    }

    fn encode_body(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.estimated_size);
        buf.put_u32_le(self.key_count);
        put_length_prefixed_slice(&mut buf, &self.smallest_key);
        put_length_prefixed_slice(&mut buf, &self.largest_key);
        buf.put_u32_le(self.block_count);
        buf.put_u32_le(self.shard_count);
        buf.put_u32_le(self.filter_type);
        assert_eq!(self.shard_count as usize, self.shards.len());
        for shard in &self.shards {
            shard.encode(&mut buf);
        }
    }

    pub fn decode(buf: &[u8]) -> HummockResult<Self> {
        let version = decode_partitioned_meta_footer_version(buf)?;
        if version != PARTITIONED_META_VERSION {
            return Err(HummockError::invalid_format_version(version));
        }

        let cursor = buf.len() - PARTITIONED_META_FOOTER_LEN;
        let checksum = (&buf[cursor..cursor + 8]).get_u64_le();
        let buf = &mut &buf[..cursor];
        xxhash64_verify(buf, checksum)?;

        let estimated_size = get_u32_le_checked(buf, "partitioned meta estimated_size")?;
        let key_count = get_u32_le_checked(buf, "partitioned meta key_count")?;
        let smallest_key = get_length_prefixed_slice_checked(buf, "partitioned meta smallest_key")?;
        let largest_key = get_length_prefixed_slice_checked(buf, "partitioned meta largest_key")?;
        let block_count = get_u32_le_checked(buf, "partitioned meta block_count")?;
        let shard_count = get_u32_le_checked(buf, "partitioned meta shard_count")?;
        let filter_type = get_u32_le_checked(buf, "partitioned meta filter_type")?;
        ensure_count_fits_remaining(
            shard_count as usize,
            buf.len(),
            MIN_META_SHARD_DESC_ENCODED_LEN,
            "partitioned meta shard_count",
        )?;
        let mut shards = Vec::with_capacity(shard_count as usize);
        let mut next_first_block_idx = 0u32;
        for shard_idx in 0..shard_count {
            let shard = MetaShardDesc::decode(shard_idx, next_first_block_idx, buf)?;
            next_first_block_idx = next_first_block_idx
                .checked_add(shard.block_count)
                .ok_or_else(|| {
                    HummockError::decode_error("partitioned meta block count overflow")
                })?;
            shards.push(shard);
        }
        if !buf.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta index has {} trailing bytes",
                buf.len()
            )));
        }

        let index = Self {
            estimated_size,
            key_count,
            smallest_key,
            largest_key,
            block_count,
            shard_count,
            filter_type,
            shards,
        };
        index.validate(cursor + PARTITIONED_META_FOOTER_LEN)?;
        Ok(index)
    }

    fn encoded_size(&self) -> usize {
        4 // estimated_size
            + 4 // key_count
            + 4 // smallest_key len
            + self.smallest_key.len()
            + 4 // largest_key len
            + self.largest_key.len()
            + 4 // block_count
            + 4 // shard_count
            + 4 // filter_type
            + self
                .shards
                .iter()
                .map(MetaShardDesc::encoded_size)
                .sum::<usize>()
            + PARTITIONED_META_FOOTER_LEN
    }
}

/// Persisted body for one metadata shard.
#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetaShard {
    pub shard_idx: u32,
    pub first_block_idx: u32,
    pub block_metas: Vec<BlockMeta>,
    pub filter: Vec<u8>,
}

impl MetaShard {
    fn encode_body(&self, mut buf: impl BufMut) {
        for block_meta in &self.block_metas {
            block_meta.encode(&mut buf);
        }
        put_length_prefixed_slice(&mut buf, &self.filter);
    }

    pub fn encode_body_to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_body_size());
        self.encode_body(&mut buf);
        buf
    }

    fn decode_body(
        shard_idx: u32,
        first_block_idx: u32,
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
                "partitioned meta shard {} has {} trailing bytes",
                shard_idx,
                buf.len()
            )));
        }
        if block_metas.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} has no block meta",
                shard_idx
            )));
        }
        for (idx, block_meta) in block_metas.iter().enumerate() {
            validate_encoded_full_key(
                &block_meta.smallest_key,
                &format!("partitioned meta shard {shard_idx} block meta {idx} smallest_key"),
            )?;
        }
        if block_metas[0].smallest_key != expected_smallest_key {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} smallest key mismatch",
                shard_idx
            )));
        }
        for (idx, block_pair) in block_metas.windows(2).enumerate() {
            let left = &block_pair[0];
            let right = &block_pair[1];
            if KeyComparator::compare_encoded_full_key(&left.smallest_key, &right.smallest_key)
                != std::cmp::Ordering::Less
            {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} block meta {} smallest key is not strictly increasing",
                    shard_idx,
                    idx + 1
                )));
            }
        }
        Ok(Self {
            shard_idx,
            first_block_idx,
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
pub fn decode_partitioned_meta_footer_version(buf: &[u8]) -> HummockResult<u32> {
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

    fn shard(shard_idx: u32, first_block_idx: u32, block_count: u32) -> MetaShard {
        let block_metas = (0..block_count)
            .map(|idx| block_meta(first_block_idx as usize + idx as usize))
            .collect();
        MetaShard {
            shard_idx,
            first_block_idx,
            block_metas,
            filter: vec![shard_idx as u8, 9, 8, 7],
        }
    }

    fn index_from_shards_at_offset(
        shards: &[MetaShard],
        initial_offset: u64,
    ) -> MetaPartitionIndex {
        let mut offset = initial_offset;
        let mut descs = Vec::with_capacity(shards.len());
        for shard in shards {
            let body = shard.encode_body_to_bytes();
            descs.push(MetaShardDesc {
                shard_idx: shard.shard_idx,
                first_block_idx: shard.first_block_idx,
                block_count: shard.block_metas.len() as u32,
                smallest_key: shard.block_metas[0].smallest_key.clone(),
                offset,
                len: body.len() as u32,
                checksum: xxhash64_checksum(&body),
            });
            offset += body.len() as u64;
        }

        let mut index = MetaPartitionIndex {
            estimated_size: 0,
            key_count: 100,
            smallest_key: descs[0].smallest_key.clone(),
            largest_key: shards
                .last()
                .unwrap()
                .block_metas
                .last()
                .unwrap()
                .smallest_key
                .clone(),
            block_count: shards
                .iter()
                .map(|shard| shard.block_metas.len() as u32)
                .sum(),
            shard_count: descs.len() as u32,
            filter_type: 1,
            shards: descs,
        };
        index.estimated_size = offset as u32 + index.encoded_size() as u32;
        index
    }

    fn index_from_shards(shards: &[MetaShard]) -> MetaPartitionIndex {
        index_from_shards_at_offset(shards, 0)
    }

    fn sample_index() -> MetaPartitionIndex {
        index_from_shards(&[shard(0, 0, 2), shard(1, 2, 2)])
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
        assert_eq!(MetaPartitionIndex::decode(&encoded).unwrap(), index);
    }

    #[test]
    fn test_partitioned_meta_index_round_trip_with_data_prefix() {
        let index = index_from_shards_at_offset(&[shard(0, 0, 2), shard(1, 2, 2)], 4096);

        assert_eq!(
            MetaPartitionIndex::decode(&index.encode_to_bytes()).unwrap(),
            index
        );
    }

    #[test]
    fn test_meta_shard_round_trip_and_desc_validation() {
        let shard = shard(0, 0, 2);
        let body = shard.encode_body_to_bytes();
        let desc = MetaShardDesc {
            shard_idx: 0,
            first_block_idx: 0,
            block_count: 2,
            smallest_key: shard.block_metas[0].smallest_key.clone(),
            offset: 0,
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
        assert!(MetaPartitionIndex::decode(&encoded).is_err());

        let mut encoded = index.encode_to_bytes();
        encoded[0] ^= 1;
        assert!(MetaPartitionIndex::decode(&encoded).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_bad_shard_ordering() {
        let mut index = sample_index();
        index.shards[1].smallest_key = index.shards[0].smallest_key.clone();
        index = reencode_index(index);

        assert!(MetaPartitionIndex::decode(&index.encode_to_bytes()).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_block_count_mismatch() {
        let mut too_many = sample_index();
        too_many.shards[1].block_count += 1;
        too_many = reencode_index(too_many);
        assert!(MetaPartitionIndex::decode(&too_many.encode_to_bytes()).is_err());

        let mut too_few = sample_index();
        too_few.shards[1].block_count -= 1;
        too_few = reencode_index(too_few);
        assert!(MetaPartitionIndex::decode(&too_few.encode_to_bytes()).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_shard_gaps_or_bad_meta_offset_boundary() {
        let mut gap = sample_index();
        gap.shards[1].offset += 1;
        gap = reencode_index(gap);
        assert!(MetaPartitionIndex::decode(&gap.encode_to_bytes()).is_err());

        let mut final_gap = sample_index();
        final_gap.shards[1].len -= 1;
        assert!(MetaPartitionIndex::decode(&final_gap.encode_to_bytes()).is_err());

        let mut crosses_index = sample_index();
        crosses_index.shards[1].len += 1;
        assert!(MetaPartitionIndex::decode(&crosses_index.encode_to_bytes()).is_err());
    }

    #[test]
    fn test_meta_shard_rejects_bad_body_layout() {
        let shard = shard(0, 0, 2);
        let body = shard.encode_body_to_bytes();

        let mut bad_count = MetaShardDesc {
            shard_idx: 0,
            first_block_idx: 0,
            block_count: 3,
            smallest_key: shard.block_metas[0].smallest_key.clone(),
            offset: 0,
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
            shard_idx: 0,
            first_block_idx: 0,
            block_count: 2,
            smallest_key: bad_order.block_metas[0].smallest_key.clone(),
            offset: 0,
            len: bad_order_body.len() as u32,
            checksum: xxhash64_checksum(&bad_order_body),
        };
        assert!(bad_order_desc.decode_body(&bad_order_body).is_err());
    }

    #[test]
    fn test_meta_shard_desc_rejects_body_len_or_checksum_mismatch() {
        let shard = shard(0, 0, 1);
        let body = shard.encode_body_to_bytes();
        let desc = MetaShardDesc {
            shard_idx: 0,
            first_block_idx: 0,
            block_count: 1,
            smallest_key: shard.block_metas[0].smallest_key.clone(),
            offset: 0,
            len: body.len() as u32,
            checksum: xxhash64_checksum(&body),
        };

        let mut bad_len = desc.clone();
        bad_len.len += 1;
        assert!(bad_len.decode_body(&body).is_err());

        let mut bad_checksum = desc;
        bad_checksum.checksum ^= 1;
        assert!(bad_checksum.decode_body(&body).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_checksum_valid_malformed_index_body() {
        let mut truncated_body = Vec::new();
        truncated_body.put_u32_le(128); // estimated_size
        truncated_body.put_u32_le(100); // key_count
        truncated_body.put_u32_le(16); // smallest_key len without enough bytes

        assert!(
            MetaPartitionIndex::decode(&encode_index_body_with_footer(&truncated_body)).is_err()
        );
    }

    #[test]
    fn test_partitioned_meta_rejects_checksum_valid_huge_shard_count() {
        let mut body = Vec::new();
        body.put_u32_le(128); // estimated_size
        body.put_u32_le(100); // key_count
        put_length_prefixed_slice(&mut body, &test_key(0));
        put_length_prefixed_slice(&mut body, &test_key(1));
        body.put_u32_le(1); // block_count
        body.put_u32_le(u32::MAX); // shard_count
        body.put_u32_le(1); // filter_type

        assert!(MetaPartitionIndex::decode(&encode_index_body_with_footer(&body)).is_err());
    }

    #[test]
    fn test_partitioned_meta_rejects_short_full_keys_without_panicking() {
        let short_key = vec![0; TABLE_PREFIX_LEN + EPOCH_LEN - 1];

        let mut short_index_boundary = sample_index();
        short_index_boundary.smallest_key = short_key.clone();
        short_index_boundary = reencode_index(short_index_boundary);
        assert_decode_returns_err_without_panicking(|| {
            MetaPartitionIndex::decode(&short_index_boundary.encode_to_bytes())
        });

        let mut short_index_largest_key = sample_index();
        short_index_largest_key.largest_key = short_key.clone();
        short_index_largest_key = reencode_index(short_index_largest_key);
        assert_decode_returns_err_without_panicking(|| {
            MetaPartitionIndex::decode(&short_index_largest_key.encode_to_bytes())
        });

        let mut short_shard_key = sample_index();
        short_shard_key.shards[1].smallest_key = short_key.clone();
        short_shard_key = reencode_index(short_shard_key);
        assert_decode_returns_err_without_panicking(|| {
            MetaPartitionIndex::decode(&short_shard_key.encode_to_bytes())
        });

        let mut shard = shard(0, 0, 2);
        shard.block_metas[1].smallest_key = short_key;
        let body = shard.encode_body_to_bytes();
        let desc = MetaShardDesc {
            shard_idx: shard.shard_idx,
            first_block_idx: shard.first_block_idx,
            block_count: shard.block_metas.len() as u32,
            smallest_key: shard.block_metas[0].smallest_key.clone(),
            offset: 0,
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
            shard_idx: 0,
            first_block_idx: 0,
            block_count: 1,
            smallest_key: test_key(0),
            offset: 0,
            len: truncated_body.len() as u32,
            checksum: xxhash64_checksum(&truncated_body),
        };

        assert!(desc.decode_body(&truncated_body).is_err());
    }

    #[test]
    fn test_meta_shard_rejects_checksum_valid_huge_expected_block_count() {
        let body = Vec::new();

        let desc = MetaShardDesc {
            shard_idx: 0,
            first_block_idx: 0,
            block_count: u32::MAX,
            smallest_key: test_key(0),
            offset: 0,
            len: 0,
            checksum: xxhash64_checksum(&body),
        };

        assert!(desc.decode_body(&body).is_err());
    }

    fn reencode_index(mut index: MetaPartitionIndex) -> MetaPartitionIndex {
        let shard_body_len: u32 = index.shards.iter().map(|shard| shard.len).sum();
        index.estimated_size = shard_body_len + index.encoded_size() as u32;
        index
    }

    fn assert_decode_returns_err_without_panicking<T>(f: impl FnOnce() -> HummockResult<T>) {
        assert!(
            rw_catch_unwind(std::panic::AssertUnwindSafe(f))
                .expect("malformed metadata must not panic")
                .is_err()
        );
    }
}
