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

use std::collections::HashMap;

use chrono::NaiveDateTime;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const MIN_STRING_LENGTH: usize = 3;

pub trait NexmarkRng {
    fn gen_string(&mut self, max: usize) -> String;
    fn gen_string_with_delimiter(&mut self, max: usize, delimiter: &str) -> String;
    fn gen_exact_string(&mut self, length: usize) -> String;
    fn gen_next_extra(&mut self, current_size: usize, desired_average_size: usize) -> String;
    fn gen_price(&mut self) -> usize;
}

impl NexmarkRng for SmallRng {
    fn gen_string(&mut self, max: usize) -> String {
        self.gen_string_with_delimiter(max, " ")
    }

    fn gen_string_with_delimiter(&mut self, max: usize, delimiter: &str) -> String {
        let len = self.gen_range(MIN_STRING_LENGTH..max);
        String::from(
            (0..len)
                .map(|_| {
                    if self.gen_range(0..13) == 0 {
                        delimiter.to_string()
                    } else {
                        ::std::char::from_u32('a' as u32 + self.gen_range(0..26))
                            .unwrap()
                            .to_string()
                    }
                })
                .collect::<Vec<String>>()
                .join("")
                .trim(),
        )
    }

    fn gen_exact_string(&mut self, length: usize) -> String {
        let mut s = String::new();
        let mut rnd = 0;
        let mut n = 0;
        for _ in 0..length {
            if n == 0 {
                rnd = self.gen();
                n = 6; // log_26(2^31)
            }
            let c = ::std::char::from_u32('a' as u32 + (rnd % 26)).unwrap();
            s.push(c);
            rnd /= 26;
            n -= 1;
        }
        s
    }

    fn gen_next_extra(&mut self, current_size: usize, desired_average_size: usize) -> String {
        if current_size > desired_average_size {
            return String::new();
        }
        let desired_average_size = desired_average_size - current_size;
        let delta = (desired_average_size as f64 * 0.2).round() as usize;
        let min_size = desired_average_size - delta;
        let desired_size = min_size
            + if delta == 0 {
                0
            } else {
                self.gen_range(0..2 * delta)
            };
        self.gen_exact_string(desired_size)
    }

    fn gen_price(&mut self) -> usize {
        (10.0_f32.powf((*self).gen::<f32>() * 6.0) * 100.0).round() as usize
    }
}

pub fn milli_ts_to_timestamp_string(milli_ts: usize) -> String {
    NaiveDateTime::from_timestamp(
        milli_ts as i64 / 1000,
        (milli_ts % (1000_usize)) as u32 * 1000000,
    )
    .format("%Y-%m-%d %H:%M:%S%.f")
    .to_string()
}

pub fn get_base_url(seed: u64) -> String {
    let mut rng = SmallRng::seed_from_u64(seed);
    let id0 = rng.gen_string_with_delimiter(5, "_");
    let id1 = rng.gen_string_with_delimiter(5, "_");
    let id2 = rng.gen_string_with_delimiter(5, "_");
    format!(
        "https://www.nexmark.com/{}/{}/{}/item.htm?query=1",
        id0, id1, id2
    )
}

pub fn build_channel_url_map(channel_number: usize) -> HashMap<usize, (String, String)> {
    let mut ans = HashMap::new();
    ans.reserve(channel_number);
    for i in 0..channel_number {
        let mut url = get_base_url(i as u64);
        let mut rng = SmallRng::seed_from_u64(i as u64);
        if rng.gen_range(0..10) > 0 {
            url.push_str("&channel_id=");
            url.push_str(&i64::abs((i as i32).reverse_bits() as i64).to_string());
        }
        let channel = format!("channel-{}", i);
        ans.insert(i, (channel, url));
    }
    ans
}

#[cfg(test)]
mod tests {
    use std::io::Result;

    use super::*;

    #[test]
    fn test_milli_ts_to_timestamp_string() -> Result<()> {
        let mut init_ts = milli_ts_to_timestamp_string(0);
        assert_eq!(init_ts, "1970-01-01 00:00:00");
        init_ts = milli_ts_to_timestamp_string(1);
        assert_eq!(init_ts, "1970-01-01 00:00:00.001");
        init_ts = milli_ts_to_timestamp_string(1000);
        assert_eq!(init_ts, "1970-01-01 00:00:01");
        Ok(())
    }

    #[test]
    fn test_deterministic() {
        let url1 = get_base_url(0);
        let url2 = get_base_url(0);
        assert_eq!(url1, url2);

        let url3 = get_base_url(1);
        let url4 = get_base_url(1);
        assert_eq!(url3, url4);
        assert_ne!(url3, url1);

        let map0 = build_channel_url_map(100);
        let map1 = build_channel_url_map(100);
        assert_eq!(map0, map1);
    }
}
