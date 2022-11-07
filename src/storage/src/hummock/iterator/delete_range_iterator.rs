use risingwave_hummock_sdk::HummockEpoch;

pub trait DeleteRangeIterator {
    fn start_user_key(&self) -> &[u8];
    fn end_user_key(&self) -> &[u8];
    fn current_epoch(&self) -> HummockEpoch;
    fn next(&mut self);
    fn seek_to_first(&mut self);
    fn seek(&mut self, _target_key: &[u8]) {
        unimplemented!("Support seek operation");
    }
    fn valid(&self) -> bool;
}
