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

use std::cmp;

use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::KeyRange;

pub trait KeyRangeExt {
    fn full_key_overlap(&self, other: &Self) -> bool;
    fn full_key_extend(&mut self, other: &Self);
    fn inf() -> Self;
    fn new(left: Vec<u8>, right: Vec<u8>) -> Self;
    fn cmp(&self, other: &Self) -> cmp::Ordering;
}

impl KeyRangeExt for KeyRange {
    fn full_key_overlap(&self, other: &Self) -> bool {
        self.inf
            || other.inf
            || (VersionedComparator::compare_key(&self.right, &other.left) != cmp::Ordering::Less
                && VersionedComparator::compare_key(&other.right, &self.left)
                    != cmp::Ordering::Less)
    }

    fn full_key_extend(&mut self, other: &Self) {
        if self.inf {
            return;
        }
        if other.inf {
            *self = Self::inf();
            return;
        }
        if VersionedComparator::compare_key(&other.left, &self.left) == cmp::Ordering::Less {
            self.left = other.left.clone();
        }
        if VersionedComparator::compare_key(&other.right, &self.right) == cmp::Ordering::Greater {
            self.right = other.right.clone();
        }
    }

    fn inf() -> Self {
        Self {
            left: vec![],
            right: vec![],
            inf: true,
        }
    }

    fn new(left: Vec<u8>, right: Vec<u8>) -> Self {
        Self {
            left,
            right,
            inf: false,
        }
    }

    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match (self.inf, other.inf) {
            (false, false) => VersionedComparator::compare_key(&self.left, &other.left)
                .then_with(|| VersionedComparator::compare_key(&self.right, &other.right)),

            (false, true) => cmp::Ordering::Less,
            (true, false) => cmp::Ordering::Greater,
            (true, true) => cmp::Ordering::Equal,
        }
    }
}
