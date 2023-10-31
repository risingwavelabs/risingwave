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

use std::ffi::OsString;

pub fn osstrs<T: Into<OsString> + AsRef<std::ffi::OsStr>>(s: impl AsRef<[T]>) -> Vec<OsString> {
    s.as_ref().iter().map(OsString::from).collect()
}

pub enum RisingWaveService {
    Compute(Vec<OsString>),
    Meta(Vec<OsString>),
    Frontend(Vec<OsString>),
    #[allow(dead_code)]
    Compactor(Vec<OsString>),
}

impl RisingWaveService {
    /// Extend additional arguments to the service.
    pub fn extend_args(&mut self, args: &[&str]) {
        match self {
            RisingWaveService::Compute(args0)
            | RisingWaveService::Meta(args0)
            | RisingWaveService::Frontend(args0)
            | RisingWaveService::Compactor(args0) => args0.extend(args.iter().map(|s| s.into())),
        }
    }
}
