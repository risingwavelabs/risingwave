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

use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

/// WARNING: Do NOT violate the Rc<RefCell<>> borrow rule when use this.
#[derive(Clone)]
pub struct SharedWriter<W: Write> {
    inner: Rc<RefCell<W>>,
}

impl<W: Write + Default> SharedWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: Rc::new(RefCell::new(writer)),
        }
    }

    pub fn into_inner(self) -> W {
        self.inner.take()
    }
}

impl<W: Write> Write for SharedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.borrow_mut().flush()
    }
}
