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

use std::fmt::*;

struct TruncatedFormatter<'a, 'b> {
    remaining: usize,
    finished: bool,
    f: &'a mut Formatter<'b>,
}
impl<'a, 'b> Write for TruncatedFormatter<'a, 'b> {
    fn write_str(&mut self, s: &str) -> Result {
        if self.finished {
            return Ok(());
        }

        if self.remaining < s.len() {
            self.f.write_str(&s[0..self.remaining])?;
            self.remaining = 0;
            self.f.write_str("...(truncated)")?;
            self.finished = true; // so that ...(truncated) is printed exactly once
        } else {
            self.f.write_str(s)?;
            self.remaining -= s.len();
        }
        Ok(())
    }
}

pub struct TruncatedFmt<'a, T>(pub &'a T, pub usize);

impl<'a, T> Debug for TruncatedFmt<'a, T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        TruncatedFormatter {
            remaining: self.1,
            finished: false,
            f,
        }
        .write_fmt(format_args!("{:?}", self.0))
    }
}

impl<'a, T> Display for TruncatedFmt<'a, T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        TruncatedFormatter {
            remaining: self.1,
            finished: false,
            f,
        }
        .write_fmt(format_args!("{}", self.0))
    }
}
