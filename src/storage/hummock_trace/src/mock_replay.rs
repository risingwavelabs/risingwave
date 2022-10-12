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

use std::collections::BTreeMap;

use crate::replay::Replayable;

pub(crate) struct MockHummock {
    mem: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Replayable for MockHummock {
    fn get(&self, key: Vec<u8>) -> Option<&Vec<u8>> {
        self.mem.get(&key)
    }

    fn ingest(&mut self, kv_pairs: Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, value) in kv_pairs {
            self.mem.insert(key, value);
        }
    }

    fn iter(&self) {
        todo!()
    }

    fn sync(&mut self, id: u64) {
        todo!()
    }

    fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool) {
        todo!()
    }

    fn update_version(&self, version_id: u64) {
        todo!()
    }
}

pub(crate) struct MockIter {
    inner: Vec<Vec<u8>>,
}
