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
