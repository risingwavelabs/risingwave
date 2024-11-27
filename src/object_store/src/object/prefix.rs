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

pub mod opendal_engine {
    /// The number of Azblob bucket prefixes
    pub(crate) const NUM_BUCKET_PREFIXES_AZBLOB: u32 = 256;

    pub fn get_object_prefix(obj_id: u64, use_new_object_prefix_strategy: bool) -> String {
        // For OpenDAL object storage, whether objects are divided by prefixes depends on whether it is a new cluster:
        // If it is a new cluster, objects will be divided into `NUM_BUCKET_PREFIXES_AZBLOB` prefixes.
        // If it is an old cluster, prefixes are not used due to the need to read and write old data.
        match use_new_object_prefix_strategy {
            true => {
                let prefix = crc32fast::hash(&obj_id.to_be_bytes()) % NUM_BUCKET_PREFIXES_AZBLOB;
                let mut obj_prefix = prefix.to_string();
                obj_prefix.push('/');
                obj_prefix
            }
            false => String::default(),
        }
    }
}
