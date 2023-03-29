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

/// Strategy to decide how to buffer the watermarks, used for state cleaning.
pub trait WatermarkBufferStrategy: Default + Sync + Send + 'static {
    /// Trigger when a epoch is committed.
    fn tick(&mut self);

    /// Whether to clear the buffer.
    ///
    /// Returns true to indicate that the buffer should be cleared and the strategy states reset.
    fn apply(&mut self) -> bool;
}

/// No buffer, apply watermark to memory immediately.
/// Use the strategy when you want to apply the watermark immediately.
#[derive(Default, Debug)]
pub struct WatermarkNoBuffer;

impl WatermarkBufferStrategy for WatermarkNoBuffer {
    fn tick(&mut self) {}

    fn apply(&mut self) -> bool {
        true
    }
}

/// Buffer the watermark by a epoch period.
/// The strategy reduced the delete-range calls to storage.
#[derive(Default, Debug)]
pub struct WatermarkBufferByEpoch<const PERIOD: usize> {
    /// number of epochs since the last time we did state cleaning by watermark.
    buffered_epochs_cnt: usize,
}

impl<const PERIOD: usize> WatermarkBufferStrategy for WatermarkBufferByEpoch<PERIOD> {
    fn tick(&mut self) {
        self.buffered_epochs_cnt += 1;
    }

    fn apply(&mut self) -> bool {
        if self.buffered_epochs_cnt >= PERIOD {
            self.buffered_epochs_cnt = 0;
            true
        } else {
            false
        }
    }
}
