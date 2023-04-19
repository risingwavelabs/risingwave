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

use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use crate::buffer::{Bitmap, BitmapBuilder};

pub fn gen_rand_bitmap(num_bits: usize, count_ones: usize, seed: u64) -> Bitmap {
    let mut builder = BitmapBuilder::zeroed(num_bits);
    let mut range = (0..num_bits).collect_vec();
    range.shuffle(&mut rand::rngs::StdRng::seed_from_u64(seed));
    let shuffled = range.into_iter().collect_vec();
    for item in shuffled.iter().take(count_ones) {
        builder.set(*item, true);
    }
    builder.finish()
}
