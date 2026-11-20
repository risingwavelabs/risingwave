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

use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::ops::Range;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tantivy::DocId;
use tantivy::directory::{FileSlice, OwnedBytes};
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};
use zstd::bulk::{Compressor, Decompressor};

use crate::bundle::BundleFile;
use crate::{FullTextError, FullTextResult, full_text_error};

// The page directory is a u64 end offset per page, relative to the end of the directory.
// Each page is [raw_len: u64, flags: u8, packed interior offsets, payload].
const PAGES: &str = "pk_pages";
const DOCS_PER_PAGE: u32 = 128;
const MAX_COMPRESSED_PAGE_BYTES: usize = 256 << 10;
const MIN_COMPRESSED_PAGE_BYTES: usize = 128;
const PK_PAGE_CACHE_SIZE: usize = 8;
const PK_BLOCK_SIZE: usize = 256 << 10;
const FIXED_WIDTH: u8 = 1;
const ZSTD: u8 = 2;
const PAGE_HEADER_LEN: usize = size_of::<u64>() + size_of::<u8>();

fn invalid_keys() -> FullTextError {
    FullTextError::decode("invalid full-text PK array")
}

fn packed_len(count: usize, width: u8) -> usize {
    (count * usize::from(width)).div_ceil(8)
}

fn pack(
    mut out: impl Write,
    values: impl IntoIterator<Item = u64>,
    width: u8,
) -> FullTextResult<()> {
    let mut packer = BitPacker::new();
    for value in values {
        packer
            .write(value, width, &mut out)
            .map_err(full_text_error)?;
    }
    packer.close(&mut out).map_err(full_text_error)
}

/// Appends one key for every document slot, in Tantivy doc ID order.
pub struct KeyArrayBuilder {
    offsets: Vec<usize>,
    data: Vec<u8>,
}

impl Default for KeyArrayBuilder {
    fn default() -> Self {
        Self {
            offsets: vec![0],
            data: Vec::new(),
        }
    }
}

impl KeyArrayBuilder {
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn push(&mut self, key: &[u8]) {
        self.data.extend_from_slice(key);
        self.offsets.push(self.data.len());
    }

    fn clear(&mut self) {
        self.data.clear();
        self.offsets.truncate(1);
    }

    pub fn estimated_size(&self) -> usize {
        self.offsets.capacity() * size_of::<usize>() + self.data.capacity()
    }

    fn key(&self, doc: DocId) -> &[u8] {
        let doc = doc as usize;
        &self.data[self.offsets[doc]..self.offsets[doc + 1]]
    }

    /// Returns old doc IDs in PK order, keeping insertion order for equal keys.
    pub fn pk_order(&self) -> Vec<DocId> {
        let mut order: Vec<DocId> = (0..self.len() as DocId).collect();
        order.sort_unstable_by(|&a, &b| self.key(a).cmp(self.key(b)).then_with(|| a.cmp(&b)));
        order
    }

    /// Writes PK pages in the final doc ID order. Without a permutation, the keys are
    /// already in final order.
    pub fn finish(
        self,
        files: &mut Vec<BundleFile>,
        order: Option<&[DocId]>,
    ) -> FullTextResult<(Bytes, Bytes)> {
        debug_assert!(order.is_none_or(|order| order.len() == self.len()));
        let key = |doc| self.key(order.map_or(doc, |order| order[doc as usize]));
        let last = self.len() as DocId - 1;
        debug_assert!((0..last).all(|doc| key(doc) <= key(doc + 1)));
        let min_key = Bytes::copy_from_slice(key(0));
        let max_key = Bytes::copy_from_slice(key(last));
        let order = order.filter(|order| !order.iter().copied().eq(0..=last));
        files.push(self.encode_pages(order)?);
        Ok((min_key, max_key))
    }

    fn encode_pages(&self, order: Option<&[DocId]>) -> FullTextResult<BundleFile> {
        let docs_per_page = DOCS_PER_PAGE as usize;
        let mut encoder = KeyPageEncoder::new(self.len())?;
        let mut scratch = Self::default();
        for first in (0..self.len()).step_by(docs_per_page) {
            let last = (first + docs_per_page).min(self.len());
            let (offsets, data) = if let Some(order) = order {
                scratch.clear();
                for &doc in &order[first..last] {
                    scratch.push(self.key(doc));
                }
                (&scratch.offsets[..], &scratch.data)
            } else {
                (&self.offsets[first..=last], &self.data)
            };
            encoder.append(offsets, data)?;
        }
        Ok(encoder.finish())
    }
}

/// Encodes the keys of a sorted merge, buffering just one page of uncompressed keys.
pub struct SortedKeyWriter {
    page: KeyArrayBuilder,
    encoder: KeyPageEncoder,
    min_key: Option<Bytes>,
}

impl SortedKeyWriter {
    pub fn new(num_docs: usize) -> FullTextResult<Self> {
        Ok(Self {
            page: KeyArrayBuilder::default(),
            encoder: KeyPageEncoder::new(num_docs)?,
            min_key: None,
        })
    }

    pub fn push(&mut self, key: &[u8]) -> FullTextResult<()> {
        let len = self.page.len();
        if len > 0 {
            debug_assert!(self.page.key(len as DocId - 1) <= key);
        } else {
            self.min_key = Some(Bytes::copy_from_slice(key));
        }
        if len == DOCS_PER_PAGE as usize {
            self.encoder.append(&self.page.offsets, &self.page.data)?;
            self.page.clear();
        }
        self.page.push(key);
        Ok(())
    }

    pub fn finish(mut self, files: &mut Vec<BundleFile>) -> FullTextResult<(Bytes, Bytes)> {
        let max_key = Bytes::copy_from_slice(self.page.key(self.page.len() as DocId - 1));
        self.encoder.append(&self.page.offsets, &self.page.data)?;
        files.push(self.encoder.finish());
        Ok((self.min_key.unwrap(), max_key))
    }
}

struct KeyPageEncoder {
    // The unchanged format puts the page directory before the data blocks.
    blocks: Vec<Bytes>,
    directory: BytesMut,
    block: BytesMut,
    data_len: u64,
    compressor: Compressor<'static>,
    compressed: Vec<u8>,
}

impl KeyPageEncoder {
    fn new(num_docs: usize) -> FullTextResult<Self> {
        let directory_len = num_docs.div_ceil(DOCS_PER_PAGE as usize) * size_of::<u64>();
        Ok(Self {
            blocks: Vec::new(),
            directory: BytesMut::with_capacity(directory_len),
            block: BytesMut::new(),
            data_len: 0,
            compressor: Compressor::new(1).map_err(full_text_error)?,
            compressed: Vec::new(),
        })
    }

    fn append(&mut self, offsets: &[usize], data: &[u8]) -> FullTextResult<()> {
        let base = offsets[0];
        let raw = &data[base..*offsets.last().unwrap()];
        let stride = offsets[1] - base;
        let fixed_width = offsets.windows(2).all(|pair| pair[1] - pair[0] == stride);
        let compressed =
            if (MIN_COMPRESSED_PAGE_BYTES..=MAX_COMPRESSED_PAGE_BYTES).contains(&raw.len()) {
                self.compressed.clear();
                self.compressed
                    .reserve(zstd::zstd_safe::compress_bound(raw.len()));
                self.compressor
                    .compress_to_buffer(raw, &mut self.compressed)
                    .map_err(full_text_error)?;
                (self.compressed.len() < raw.len()).then_some(self.compressed.as_slice())
            } else {
                None
            };

        let payload = compressed.unwrap_or(raw);
        let width = compute_num_bits(raw.len() as u64);
        let page_size = PAGE_HEADER_LEN
            + if fixed_width {
                0
            } else {
                packed_len(offsets.len() - 2, width)
            }
            + payload.len();
        if !self.block.is_empty() && self.block.len() + page_size > PK_BLOCK_SIZE {
            self.blocks.push(std::mem::take(&mut self.block).freeze());
        }
        self.block.reserve(page_size);
        let start = self.block.len();
        self.block.put_u64_le(raw.len() as u64);
        self.block.put_u8(
            (if fixed_width { FIXED_WIDTH } else { 0 })
                | (if compressed.is_some() { ZSTD } else { 0 }),
        );
        if !fixed_width {
            // The first and final offsets are implicit in the payload length.
            pack(
                (&mut self.block).writer(),
                offsets[1..offsets.len() - 1]
                    .iter()
                    .map(|offset| (offset - base) as u64),
                width,
            )?;
        }
        self.block.put_slice(payload);
        self.data_len += (self.block.len() - start) as u64;
        self.directory.put_u64_le(self.data_len);
        Ok(())
    }

    fn finish(mut self) -> BundleFile {
        if !self.block.is_empty() {
            self.blocks.push(self.block.freeze());
        }
        self.blocks.insert(0, self.directory.freeze());
        (PAGES.to_owned(), self.blocks)
    }
}

/// PKs are sorted by doc ID. Only the payload of a touched page may need decompression.
pub struct KeyArray {
    pages: Vec<KeyPage>,
    num_docs: DocId,
}

impl KeyArray {
    pub fn open(files: &mut HashMap<String, FileSlice>, max_doc: DocId) -> FullTextResult<Self> {
        let encoded = files
            .remove(PAGES)
            .ok_or_else(|| FullTextError::decode("missing full-text PK pages"))?
            .read_bytes()
            .map_err(full_text_error)?;

        let page_count = max_doc.div_ceil(DOCS_PER_PAGE) as usize;
        let directory_len = page_count * size_of::<u64>();
        let Some((directory, data)) = encoded.split_at_checked(directory_len) else {
            return Err(invalid_keys());
        };
        let mut pages = Vec::with_capacity(page_count);
        let mut start = 0;
        for (i, &end) in directory.as_chunks::<8>().0.iter().enumerate() {
            let end = usize::try_from(u64::from_le_bytes(end)).map_err(|_| invalid_keys())?;
            if end < start || end > data.len() {
                return Err(invalid_keys());
            }
            let num_docs = (max_doc - i as u32 * DOCS_PER_PAGE).min(DOCS_PER_PAGE);
            pages.push(KeyPage::open(
                encoded.slice(directory_len + start..directory_len + end),
                num_docs,
            )?);
            start = end;
        }
        if start != data.len() {
            return Err(invalid_keys());
        }
        Ok(Self {
            pages,
            num_docs: max_doc,
        })
    }

    pub fn heap_size(&self) -> usize {
        self.pages.capacity() * size_of::<KeyPage>()
    }

    pub fn reader(&self) -> KeyArrayReader<'_> {
        KeyArrayReader {
            array: self,
            decompressor: None,
            cache: VecDeque::new(),
        }
    }

    pub fn cursor(&self, doc: DocId) -> FullTextResult<KeyCursor<'_>> {
        let mut cursor = KeyCursor {
            array: self,
            page_id: usize::MAX,
            uncompressed: None,
            key_range: 0..0,
            decompressor: None,
            decoded: Vec::new(),
        };
        cursor.seek(doc)?;
        Ok(cursor)
    }
}

struct KeyPage {
    bytes: OwnedBytes,
    raw_len: usize,
    payload_start: usize,
    num_docs: DocId,
    flags: u8,
    offsets: BitUnpacker,
}

impl KeyPage {
    fn open(bytes: OwnedBytes, num_docs: DocId) -> FullTextResult<Self> {
        let mut header = bytes.as_slice();
        if header.len() < PAGE_HEADER_LEN {
            return Err(invalid_keys());
        }
        let raw_len = usize::try_from(header.get_u64_le()).map_err(|_| invalid_keys())?;
        let flags = header.get_u8();
        if flags & !(FIXED_WIDTH | ZSTD) != 0 {
            return Err(invalid_keys());
        }
        let width = compute_num_bits(raw_len as u64);
        let fixed_width = flags & FIXED_WIDTH != 0;
        let offsets_len = if fixed_width {
            if !raw_len.is_multiple_of(num_docs as usize) {
                return Err(invalid_keys());
            }
            0
        } else {
            packed_len(num_docs as usize - 1, width)
        };
        let payload_start = PAGE_HEADER_LEN + offsets_len;
        if payload_start > bytes.len() {
            return Err(invalid_keys());
        }
        let page = Self {
            bytes,
            raw_len,
            payload_start,
            num_docs,
            flags,
            offsets: BitUnpacker::new(width),
        };
        let size = page.payload().len();
        let valid_payload = if page.is_compressed() {
            size > 0 && size < raw_len && raw_len <= MAX_COMPRESSED_PAGE_BYTES
        } else {
            size == raw_len
        };
        if !valid_payload {
            return Err(invalid_keys());
        }
        if !fixed_width && !(0..=num_docs).map(|doc| page.offset(doc)).is_sorted() {
            return Err(invalid_keys());
        }
        Ok(page)
    }

    fn is_compressed(&self) -> bool {
        self.flags & ZSTD != 0
    }

    fn payload(&self) -> &[u8] {
        &self.bytes[self.payload_start..]
    }

    fn decode(
        &self,
        decompressor: &mut Option<Decompressor<'static>>,
        decoded: &mut Vec<u8>,
    ) -> FullTextResult<()> {
        if decompressor.is_none() {
            *decompressor = Some(Decompressor::new().map_err(full_text_error)?);
        }
        decoded.clear();
        decoded.reserve_exact(self.raw_len);
        decompressor
            .as_mut()
            .unwrap()
            .decompress_to_buffer(self.payload(), decoded)
            .map_err(full_text_error)?;
        if decoded.len() != self.raw_len {
            return Err(invalid_keys());
        }
        Ok(())
    }

    fn offset(&self, i: DocId) -> usize {
        if self.flags & FIXED_WIDTH != 0 {
            i as usize * (self.raw_len / self.num_docs as usize)
        } else if i == 0 {
            0
        } else if i == self.num_docs {
            self.raw_len
        } else {
            self.offsets
                .get(i - 1, &self.bytes[PAGE_HEADER_LEN..self.payload_start]) as usize
        }
    }

    fn key_range(&self, i: DocId) -> Range<usize> {
        self.offset(i)..self.offset(i + 1)
    }
}

/// A merge input holds one page at a time. Its current key is borrowed from that page.
pub struct KeyCursor<'a> {
    array: &'a KeyArray,
    page_id: usize,
    uncompressed: Option<&'a [u8]>,
    key_range: Range<usize>,
    decompressor: Option<Decompressor<'static>>,
    decoded: Vec<u8>,
}

impl KeyCursor<'_> {
    pub fn seek(&mut self, doc: DocId) -> FullTextResult<()> {
        let page_id = (doc / DOCS_PER_PAGE) as usize;
        let page = &self.array.pages[page_id];
        if page_id != self.page_id {
            if page.is_compressed() {
                page.decode(&mut self.decompressor, &mut self.decoded)?;
                self.uncompressed = None;
            } else {
                self.uncompressed = Some(page.payload());
            }
            self.page_id = page_id;
        }
        self.key_range = page.key_range(doc % DOCS_PER_PAGE);
        Ok(())
    }

    pub fn key(&self) -> &[u8] {
        &self.uncompressed.unwrap_or(&self.decoded)[self.key_range.clone()]
    }
}

/// Per-operation scratch space, so opening a segment or starting a short search does not
/// decompress its payload, and the shared segment cache holds only the encoded pages.
pub struct KeyArrayReader<'a> {
    array: &'a KeyArray,
    decompressor: Option<Decompressor<'static>>,
    cache: VecDeque<(usize, Vec<u8>)>,
}

impl KeyArrayReader<'_> {
    pub fn get(&mut self, doc: DocId) -> FullTextResult<&[u8]> {
        let page_id = (doc / DOCS_PER_PAGE) as usize;
        let page = &self.array.pages[page_id];
        let range = page.key_range(doc % DOCS_PER_PAGE);
        if !page.is_compressed() {
            return Ok(&page.payload()[range]);
        }
        match self.cache.iter().position(|(id, _)| *id == page_id) {
            Some(0) => {}
            Some(index) => {
                let entry = self.cache.remove(index).unwrap();
                self.cache.push_front(entry);
            }
            None => {
                let mut decoded = Vec::new();
                page.decode(&mut self.decompressor, &mut decoded)?;
                if self.cache.len() == PK_PAGE_CACHE_SIZE {
                    self.cache.pop_back();
                }
                self.cache.push_front((page_id, decoded));
            }
        }
        Ok(&self.cache[0].1[range])
    }

    pub fn find(&mut self, key: &[u8]) -> FullTextResult<Range<DocId>> {
        let lo = self.bound(key, 0, false)?;
        let hi = self.bound(key, lo, true)?;
        Ok(lo..hi)
    }

    fn bound(&mut self, key: &[u8], mut lo: DocId, upper: bool) -> FullTextResult<DocId> {
        let mut hi = self.array.num_docs;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cmp = self.get(mid)?.cmp(key);
            if cmp.is_lt() || (upper && cmp.is_eq()) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn encode(docs: &[Vec<u8>]) -> Vec<(String, OwnedBytes)> {
        let mut builder = KeyArrayBuilder::default();
        for doc in docs {
            builder.push(doc);
        }
        let mut files = vec![];
        builder.finish(&mut files, None).unwrap();
        files
            .into_iter()
            .map(|(name, chunks)| {
                let data = chunks
                    .into_iter()
                    .flat_map(|chunk| chunk.into_iter())
                    .collect::<Vec<_>>();
                (name, OwnedBytes::new(data))
            })
            .collect()
    }

    fn open(files: Vec<(String, OwnedBytes)>, len: usize) -> FullTextResult<KeyArray> {
        let mut files = files
            .into_iter()
            .map(|(name, bytes)| (name, FileSlice::new(Arc::new(bytes))))
            .collect();
        KeyArray::open(&mut files, len as DocId)
    }

    #[test]
    fn test_pk_pages_and_duplicate_keys() {
        let mut docs = (0..DOCS_PER_PAGE * 2 + 1)
            .map(|i| i.to_be_bytes().to_vec())
            .collect::<Vec<_>>();
        docs[0] = Vec::new();
        let duplicate = b"\0\0\0\x7fduplicate";
        docs[127] = duplicate.to_vec();
        docs[128] = duplicate.to_vec();
        docs[256] = vec![b'z'; MAX_COMPRESSED_PAGE_BYTES + 1];
        let array = open(encode(&docs), docs.len()).unwrap();
        assert_eq!(array.pages.len(), 3);
        assert!(array.pages[0].is_compressed());
        assert!(!array.pages[2].is_compressed());
        let mut reader = array.reader();
        // Cross page boundaries in both directions and exercise the last partial page.
        for doc in [128, 127, 0, 129, 256, 255, 128] {
            assert_eq!(reader.get(doc).unwrap(), docs[doc as usize]);
        }
        assert_eq!(reader.find(duplicate).unwrap(), 127..129);
        assert_eq!(reader.find(b"missing").unwrap().count(), 0);
        assert_eq!(reader.find(b"").unwrap().collect::<Vec<_>>(), [0]);

        let mut cursor = array.cursor(0).unwrap();
        assert_eq!(cursor.key(), b"");
        for doc in [127, 128, 129, 255, 256] {
            cursor.seek(doc).unwrap();
            assert_eq!(cursor.key(), docs[doc as usize]);
        }
    }

    #[test]
    fn test_pk_page_validation() {
        let docs = vec![vec![b'x'; 32]; 128];
        let mut files = encode(&docs);
        let pages = files.iter_mut().find(|(name, _)| name == PAGES).unwrap();
        let mut bytes = pages.1.to_vec();
        // Mutate the compressed payload but not its header; decoding is lazy and returns an error.
        bytes[8 + PAGE_HEADER_LEN] ^= 0xff;
        pages.1 = OwnedBytes::new(bytes);
        let array = open(files, docs.len()).unwrap();
        assert!(array.reader().get(0).is_err());
        assert!(array.cursor(0).is_err());

        let mut files = encode(&docs);
        let pages = files.iter_mut().find(|(name, _)| name == PAGES).unwrap();
        let mut bytes = pages.1.to_vec();
        bytes[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        pages.1 = OwnedBytes::new(bytes);
        assert!(open(files, docs.len()).is_err());
    }

    #[test]
    fn test_pk_encoded_sizes() {
        const DOCS: usize = 16_384;
        let measure = |name, mut docs: Vec<Vec<u8>>| {
            docs.sort_unstable();
            let files = encode(&docs);
            let plain =
                (docs.len() + 1) * 8 + docs.len() * 4 + docs.iter().map(Vec::len).sum::<usize>();
            let encoded = files.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
            eprintln!("{name}: {plain} B uncompressed with doc IDs, {encoded} B sorted pages");
            assert!(encoded < plain);
            open(files, docs.len()).unwrap()
        };

        let sequential = (0..DOCS as u64).map(|i| i.to_be_bytes().to_vec()).collect();
        let array = measure("sequential 8-byte PK", sequential);
        assert!(array.pages.iter().all(KeyPage::is_compressed));

        let prefixed = (0..DOCS as u64)
            .map(|i| format!("tenant={};partition=events;key={i:x}", i % 32).into_bytes())
            .collect();
        let array = measure("variable prefixed PK", prefixed);
        assert!(array.pages.iter().all(KeyPage::is_compressed));

        let mut state = 0x42d0_aaaau64;
        let mut next_word = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_be_bytes()
        };
        let random = (0..DOCS)
            .map(|_| [next_word(), next_word()].concat())
            .collect();
        measure("random 16-byte PK", random);
    }
}
