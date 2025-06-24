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

use std::mem::take;
use std::slice;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::future::BoxFuture;
use risingwave_hummock_sdk::HummockVectorFileId;
use risingwave_hummock_sdk::vector_index::VectorFileInfo;
use risingwave_object_store::object::ObjectStreamingUploader;

use crate::hummock::{HummockError, HummockResult, xxhash64_checksum, xxhash64_verify};
use crate::vector::VectorRef;

const VECTOR_FILE_VERSION: u32 = 1;
const VECTOR_FILE_MAGIC_NUM: u32 = 0x3866cd92;

#[cfg_attr(test, derive(PartialEq, Debug))]
pub struct VectorBlockInner {
    dimension: usize,
    vector_payload: Vec<f32>,
    info_payload: Vec<u8>,
    info_offset: Vec<u32>,
}

impl VectorBlockInner {
    pub fn count(&self) -> usize {
        self.info_offset.len()
    }

    pub fn vec_ref(&self, idx: usize) -> VectorRef<'_> {
        let start = idx * self.dimension;
        let end = start + self.dimension;
        VectorRef::from_slice(&self.vector_payload[start..end])
    }

    pub fn info(&self, idx: usize) -> &[u8] {
        let start = if idx == 0 {
            0
        } else {
            self.info_offset[idx - 1] as usize
        };
        let end = self.info_offset[idx] as usize;
        &self.info_payload[start..end]
    }

    pub fn encoded_len(&self) -> usize {
        size_of::<u32>() // dimension
            + size_of::<u32>() // vector count
            + self.info_offset.len() * size_of::<u32>() // info offsets
            + self.info_payload.len() // info payload
            + self.vector_payload.len() * size_of::<f32>() // vector payload
    }
}

pub struct VectorBlockBuilder {
    inner: VectorBlockInner,
    encoded_len: usize,
}

impl VectorBlockBuilder {
    pub fn new(dimension: usize) -> Self {
        Self {
            inner: VectorBlockInner {
                dimension,
                vector_payload: vec![],
                info_payload: vec![],
                info_offset: vec![],
            },
            encoded_len: size_of::<u32>() // dimension
             + size_of::<u32>(), // vector count
        }
    }

    pub fn add(&mut self, vec: VectorRef<'_>, info: &[u8]) {
        let slice = vec.as_slice();
        assert_eq!(self.inner.dimension, slice.len());
        self.inner.vector_payload.extend_from_slice(slice);
        self.inner.info_payload.extend_from_slice(info);
        let offset = self.inner.info_payload.len();
        self.inner.info_offset.push(
            offset
                .try_into()
                .unwrap_or_else(|_| panic!("offset {} overflow", offset)),
        );
        self.encoded_len += size_of::<u32>() + size_of_val(slice) + info.len()
    }

    pub fn encoded_len(&self) -> usize {
        debug_assert_eq!(self.encoded_len, self.inner.encoded_len());
        self.encoded_len
    }

    pub fn finish(self) -> Option<VectorBlock> {
        if !self.inner.info_offset.is_empty() {
            Some(VectorBlock(Arc::new(self.inner)))
        } else {
            None
        }
    }
}

#[cfg_attr(test, derive(PartialEq, Debug))]
#[derive(Clone)]
pub struct VectorBlock(Arc<VectorBlockInner>);

impl VectorBlock {
    pub fn count(&self) -> usize {
        self.0.count()
    }

    pub fn vec_ref(&self, idx: usize) -> VectorRef<'_> {
        self.0.vec_ref(idx)
    }

    pub fn info(&self, idx: usize) -> &[u8] {
        self.0.info(idx)
    }

    /// # Format:
    ///
    /// ```plain
    /// | dimension (u32) | vector_count (u32) |
    /// | info_offset (u32) * vector_count |
    /// | info_payload (u8) * (last offset in info_offset) |
    /// | vector_payload (f32) * (dimension * vector_count)  |
    /// ```
    fn encode_payload(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.0.dimension.try_into().unwrap());
        let vector_count = self.0.info_offset.len();
        assert!(vector_count > 0);
        buf.put_u32_le(
            vector_count
                .try_into()
                .unwrap_or_else(|_| panic!("vector count {} overflow", vector_count)),
        );
        for offset in &self.0.info_offset {
            buf.put_u32_le(*offset);
        }
        let last_offset = *self.0.info_offset.last().unwrap();
        assert_eq!(last_offset as usize, self.0.info_payload.len());
        buf.put_slice(&self.0.info_payload);
        assert_eq!(self.0.vector_payload.len(), self.0.dimension * vector_count);
        let vector_payload_ptr = self.0.vector_payload.as_slice().as_ptr() as *const u8;
        // safety: correctly set the size of vector_payload
        let vector_payload_slice = unsafe {
            slice::from_raw_parts(
                vector_payload_ptr,
                self.0.vector_payload.len() * size_of::<f32>(),
            )
        };
        buf.put_slice(vector_payload_slice);
    }

    fn decode_payload(mut buf: impl Buf) -> Self {
        let dimension: usize = buf.get_u32_le().try_into().unwrap();
        let vector_count = buf.get_u32_le() as usize;
        let mut info_offset = Vec::with_capacity(vector_count);
        for _ in 0..vector_count {
            let offset = buf.get_u32_le();
            info_offset.push(offset);
        }
        let info_payload_len = *info_offset.last().expect("non-empty") as usize;
        let mut info_payload = vec![0; info_payload_len];
        buf.copy_to_slice(&mut info_payload);
        let vector_item_count = dimension * vector_count;
        let mut vector_payload = Vec::with_capacity(vector_item_count);

        let vector_payload_ptr = vector_payload.spare_capacity_mut().as_mut_ptr() as *mut u8;
        // safety: no data append to vector_payload, and correctly set the size of vector_payload
        let vector_payload_slice = unsafe {
            slice::from_raw_parts_mut(vector_payload_ptr, vector_item_count * size_of::<f32>())
        };
        buf.copy_to_slice(vector_payload_slice);
        // safety: have written correct amount of data
        unsafe {
            vector_payload.set_len(vector_item_count);
        }
        Self(Arc::new(VectorBlockInner {
            dimension,
            vector_payload,
            info_payload,
            info_offset,
        }))
    }

    fn encode(&self, encoded_block_len: usize) -> Bytes {
        let encoded_len = encoded_block_len + size_of::<u64>(); // add space for checksum
        let mut encoded_block = BytesMut::with_capacity(encoded_len);
        self.encode_payload(&mut encoded_block);
        let checksum = xxhash64_checksum(encoded_block.as_ref());
        encoded_block.put_u64_le(checksum);
        let encoded_block = encoded_block.freeze();
        debug_assert_eq!(encoded_block.len(), encoded_len);
        encoded_block
    }

    pub fn decode(buf: &[u8]) -> HummockResult<Self> {
        if buf.len() < size_of::<u64>() {
            return Err(HummockError::decode_error("block too short"));
        }
        let back_cursor_end = buf.len();
        let back_cursor_start = back_cursor_end - size_of::<u64>();
        let payload = &buf[..back_cursor_start];
        {
            let checksum =
                u64::from_le_bytes(buf[back_cursor_start..back_cursor_end].try_into().unwrap());
            xxhash64_verify(payload, checksum)?;
        }
        Ok(Self::decode_payload(payload))
    }
}

impl<'a> IntoIterator for &'a VectorBlock {
    type Item = (VectorRef<'a>, &'a [u8]);

    type IntoIter = impl Iterator<Item = Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        (0..self.0.info_offset.len()).map(|i| (self.vec_ref(i), self.info(i)))
    }
}

pub struct VectorBlockMeta {
    pub offset: usize,
    pub block_size: usize,
    pub vector_count: usize,
    pub start_vector_id: usize,
}

impl VectorBlockMeta {
    fn encode(&self, mut buf: impl BufMut) {
        buf.put_u64_le(self.offset.try_into().unwrap());
        buf.put_u64_le(self.block_size.try_into().unwrap());
        buf.put_u64_le(self.vector_count.try_into().unwrap());
        buf.put_u64_le(self.start_vector_id.try_into().unwrap());
    }

    fn encoded_len() -> usize {
        size_of::<u64>() * 4 // offset, block_size, vector_count, start_vector_id
    }

    fn decode(mut buf: impl Buf) -> Self {
        let offset = buf.get_u64_le().try_into().unwrap();
        let block_size = buf.get_u64_le().try_into().unwrap();
        let vector_count = buf.get_u64_le().try_into().unwrap();
        let start_vector_id = buf.get_u64_le().try_into().unwrap();
        Self {
            offset,
            block_size,
            vector_count,
            start_vector_id,
        }
    }
}

pub struct VectorFileMeta {
    pub block_metas: Vec<VectorBlockMeta>,
}

impl VectorFileMeta {
    fn encode(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.block_metas.len().try_into().unwrap());
        for meta in &self.block_metas {
            meta.encode(&mut buf);
        }
    }

    fn encode_len(&self) -> usize {
        size_of::<u32>() // block count
            + self.block_metas.len() * VectorBlockMeta::encoded_len()
    }

    fn decode(mut buf: impl Buf) -> Self {
        let block_count = buf.get_u32_le() as usize;
        let mut block_metas = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            block_metas.push(VectorBlockMeta::decode(&mut buf));
        }
        Self { block_metas }
    }

    fn preserved_footer_len() -> usize {
        size_of::<u64>() // checksum
            + size_of::<u64>() // meta offset
            + size_of::<u32>() // version
            + size_of::<u32>() // magic
    }

    fn encode_footer(&self, mete_offset: usize) -> Bytes {
        let encoded_footer_len = self.encode_len() + Self::preserved_footer_len();
        let mut encoded_footer = BytesMut::with_capacity(encoded_footer_len);
        self.encode(&mut encoded_footer);
        let checksum = xxhash64_checksum(encoded_footer.as_ref());
        encoded_footer.put_u64_le(checksum);
        encoded_footer.put_u64_le(mete_offset.try_into().unwrap());
        encoded_footer.put_u32_le(VECTOR_FILE_VERSION);
        encoded_footer.put_u32_le(VECTOR_FILE_MAGIC_NUM);
        let encoded_footer = encoded_footer.freeze();
        debug_assert_eq!(encoded_footer.len(), encoded_footer_len);
        encoded_footer
    }

    pub fn decode_footer(buf: &[u8]) -> HummockResult<Self> {
        if buf.len() < Self::preserved_footer_len() {
            return Err(HummockError::decode_error("footer too short"));
        }
        // get magic number
        let mut back_cursor_end = buf.len();
        let mut back_cursor_start = back_cursor_end - size_of::<u32>();
        {
            let magic =
                u32::from_le_bytes(buf[back_cursor_start..back_cursor_end].try_into().unwrap());
            if magic != VECTOR_FILE_MAGIC_NUM {
                return Err(HummockError::magic_mismatch(VECTOR_FILE_MAGIC_NUM, magic));
            }
        }
        // get file version
        back_cursor_end = back_cursor_start;
        back_cursor_start = back_cursor_end - size_of::<u32>();
        {
            let file_version =
                u32::from_le_bytes(buf[back_cursor_start..back_cursor_end].try_into().unwrap());
            if file_version != VECTOR_FILE_VERSION {
                return Err(HummockError::invalid_format_version(file_version));
            }
        }

        // skip meta offset
        back_cursor_end = back_cursor_start;
        back_cursor_start = back_cursor_end - size_of::<u64>();

        // get and check checksum
        back_cursor_end = back_cursor_start;
        back_cursor_start = back_cursor_end - size_of::<u64>();
        let payload = &buf[..back_cursor_start];
        {
            let checksum =
                u64::from_le_bytes(buf[back_cursor_start..back_cursor_end].try_into().unwrap());
            xxhash64_verify(payload, checksum)?;
        }

        Ok(Self::decode(payload))
    }
}

pub type NewStreamingUploaderFn = Box<
    dyn Fn() -> BoxFuture<'static, HummockResult<(HummockVectorFileId, ObjectStreamingUploader)>>
        + Send
        + Sync,
>;

pub struct VectorFileBuilder {
    dimension: usize,
    new_uploader: NewStreamingUploaderFn,
    max_block_size: usize,

    // builder state
    block_metas: Vec<VectorBlockMeta>,
    blocks: Vec<VectorBlock>,
    building_block: Option<(VectorBlockBuilder, usize)>,
    next_vector_id: usize,
    next_block_offset: usize,
    uploader: Option<(HummockVectorFileId, ObjectStreamingUploader)>,
}

impl VectorFileBuilder {
    pub fn new(
        dimension: usize,
        new_uploader: NewStreamingUploaderFn,
        next_vector_id: usize,
        max_block_size: usize,
    ) -> Self {
        Self {
            dimension,
            new_uploader,
            max_block_size,
            block_metas: Vec::new(),
            blocks: Vec::new(),
            building_block: None,
            uploader: None,
            next_vector_id,
            next_block_offset: 0,
        }
    }

    pub fn add(&mut self, vec: VectorRef<'_>, info: &[u8]) {
        let (builder, _) = self
            .building_block
            .get_or_insert_with(|| (VectorBlockBuilder::new(self.dimension), self.next_vector_id));
        builder.add(vec, info);
        self.next_vector_id += 1;
    }

    pub async fn finish(
        &mut self,
    ) -> HummockResult<Option<(VectorFileInfo, Vec<VectorBlock>, VectorFileMeta)>> {
        self.flush_inner().await?;
        assert!(self.building_block.is_none(), "unfinished block builder");
        let ret = if let Some((object_id, mut uploader)) = self.uploader.take() {
            assert!(!self.block_metas.is_empty());
            let start_vector_id = self.block_metas.get(0).expect("non-empty").start_vector_id;
            let meta = VectorFileMeta {
                block_metas: take(&mut self.block_metas),
            };
            let meta_offset = self.next_block_offset;
            let encoded_footer = meta.encode_footer(meta_offset);
            let file_size = meta_offset + encoded_footer.len();
            uploader.write_bytes(encoded_footer).await?;
            uploader.finish().await?;
            let file_info = VectorFileInfo {
                object_id,
                file_size: file_size.try_into().unwrap(),
                start_vector_id,
                vector_count: self.blocks.iter().map(|b| b.count()).sum::<usize>(),
                meta_offset,
            };
            self.next_block_offset = 0;
            Ok(Some((file_info, take(&mut self.blocks), meta)))
        } else {
            Ok(None)
        };
        assert!(self.is_empty(), "builder not empty after finish");
        assert_eq!(
            self.next_block_offset, 0,
            "next_block_offset should be zero after finish"
        );
        ret
    }

    pub fn is_empty(&self) -> bool {
        self.building_block.is_none() && self.blocks.is_empty() && self.block_metas.is_empty()
    }

    pub fn next_vector_id(&self) -> usize {
        self.next_vector_id
    }

    pub async fn try_flush(&mut self) -> HummockResult<()> {
        if let Some((builder, _)) = &self.building_block {
            if builder.encoded_len() >= self.max_block_size {
                self.flush_inner().await?;
            }
        }
        Ok(())
    }

    async fn flush_inner(&mut self) -> HummockResult<()> {
        if let Some((builder, start_vector_id)) = self.building_block.take() {
            let encoded_block_len = builder.encoded_len();
            if let Some(block) = builder.finish() {
                let (_, uploader) = match &mut self.uploader {
                    Some(uploader) => uploader,
                    None => self.uploader.insert((self.new_uploader)().await?),
                };
                let encoded_block = block.encode(encoded_block_len);
                let block_meta = VectorBlockMeta {
                    offset: self.next_block_offset,
                    block_size: encoded_block.len(),
                    vector_count: block.count(),
                    start_vector_id,
                };
                self.next_block_offset += encoded_block.len();
                uploader.write_bytes(encoded_block).await?;
                self.block_metas.push(block_meta);
                self.blocks.push(block);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::util::iter_util::ZipEqDebug;

    use crate::hummock::vector::file::{VectorBlock, VectorBlockBuilder};
    use crate::vector::test_utils::{gen_info, gen_vector};

    const VECTOR_DIM: usize = 128;

    #[test]
    fn test_basic() {
        let input = (0..200)
            .map(|i| (gen_vector(VECTOR_DIM), gen_info(i)))
            .collect_vec();
        let mut builder = VectorBlockBuilder::new(VECTOR_DIM);
        for (vec, info) in &input {
            builder.add(vec.to_ref(), info);
        }
        let expected_encoded_len = builder.encoded_len();
        let block = builder.finish().unwrap();
        let mut encoded_block = Vec::new();
        block.encode_payload(&mut encoded_block);
        assert_eq!(expected_encoded_len, encoded_block.len());
        let decoded_block = VectorBlock::decode_payload(encoded_block.as_slice());
        assert_eq!(block, decoded_block);
        for ((expected_vec, expected_info), (actual_vec, actual_info)) in
            input.iter().zip_eq_debug(&block)
        {
            assert_eq!(expected_vec.to_ref(), actual_vec);
            assert_eq!(expected_info.iter().as_slice(), actual_info);
        }
    }

    #[test]
    fn test_empty_builder() {
        let builder = VectorBlockBuilder::new(VECTOR_DIM);
        assert!(builder.finish().is_none());
    }
}
