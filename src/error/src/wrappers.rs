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

#![expect(clippy::disallowed_types)]

#[derive(::std::fmt::Debug)]
pub struct IcebergError(iceberg::Error);

impl std::fmt::Display for IcebergError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for IcebergError {
    fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
        request.provide_ref::<std::backtrace::Backtrace>(self.0.backtrace());
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl From<iceberg::Error> for IcebergError {
    fn from(value: iceberg::Error) -> Self {
        IcebergError(value)
    }
}

// TODO: add opendal after opendal 0.54
