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

#![feature(lazy_cell)]

pub mod playground;
pub use playground::*;

lazy_static::lazy_static! {
    static ref VERSION: String = {
        let version = clap::crate_version!();
        // VERGEN_GIT_SHA is provided by the build script. It will trigger rebuild
        // for each commit, so we only use it for the final binary.
        let vergen_git_sha = env!("VERGEN_GIT_SHA");

        // GIT_SHA is a normal environment variable. It's risingwave_common::GIT_SHA
        // and is used in logs/version queries.
        //
        // Usually it's only provided by docker/binary releases (including nightly builds).
        // We check it is the same as VERGEN_GIT_SHA when it's present.
        if let Some(git_sha) = option_env!("GIT_SHA") {
            assert_eq!(git_sha, vergen_git_sha, "GIT_SHA ({git_sha}) mismatches VERGEN_GIT_SHA ({vergen_git_sha})");
        };

        format!("{} ({})", version, vergen_git_sha)
    };
}

pub fn version() -> &'static str {
    &VERSION
}
