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

use std::slice;

use bytes::{Buf, BufMut};

pub fn encode_vector_payload(payload: &[f32], mut buf: impl BufMut) {
    let vector_payload_ptr = payload.as_ptr() as *const u8;
    // safety: correctly set the size of vector_payload
    let vector_payload_slice =
        unsafe { slice::from_raw_parts(vector_payload_ptr, size_of_val(payload)) };
    buf.put_slice(vector_payload_slice);
}

pub fn decode_vector_payload(vector_item_count: usize, mut buf: impl Buf) -> Vec<f32> {
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

    vector_payload
}
