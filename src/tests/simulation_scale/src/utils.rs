// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub trait AssertResult: AsRef<str> {
    /// Asserts that the result is equal to the expected result after trimming whitespace.
    #[track_caller]
    fn assert_result_eq(&self, other: impl AsRef<str>) -> &Self {
        assert_eq!(self.as_ref().trim(), other.as_ref().trim());
        self
    }

    /// Asserts that the result is not equal to the expected result after trimming whitespace.
    #[track_caller]
    fn assert_result_ne(&self, other: impl AsRef<str>) -> &Self {
        assert_ne!(self.as_ref().trim(), other.as_ref().trim());
        self
    }
}

impl<S: AsRef<str>> AssertResult for S {}
