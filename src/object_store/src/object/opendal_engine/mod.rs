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

pub mod opendal_object_store;
pub use opendal_object_store::*;

#[cfg(feature = "hdfs-backend")]
pub mod hdfs;
pub mod webhdfs;

pub mod gcs;

pub mod obs;

pub mod azblob;
pub mod opendal_s3;
pub mod oss;

pub mod fs;

// To make sure the the operation is consistent, we should specially set `atomic_write_dir` for fs, hdfs and webhdfs services.
const ATOMIC_WRITE_DIR: &str = "atomic_write_dir/";
