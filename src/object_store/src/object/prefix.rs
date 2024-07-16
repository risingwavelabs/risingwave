// Copyright 2024 RisingWave Labs
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

pub(crate) mod s3 {
    /// The number of S3 bucket prefixes
    pub(crate) const NUM_BUCKET_PREFIXES: u32 = 256;

    pub(crate) fn get_object_prefix(obj_id: u64) -> String {
        let prefix = crc32fast::hash(&obj_id.to_be_bytes()) % NUM_BUCKET_PREFIXES;
        let mut obj_prefix = prefix.to_string();
        obj_prefix.push('/');
        obj_prefix
    }
}
