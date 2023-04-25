use hytra::TrAdder;
use prometheus::core::{Atomic, GenericGauge};

pub struct TrAdderAtomic(TrAdder<i64>);

impl Atomic for TrAdderAtomic {
    type T = i64;

    fn new(val: i64) -> Self {
        let v = TrAdderAtomic(TrAdder::new());
        v.0.inc(val);
        v
    }

    fn set(&self, _val: i64) {
        panic!("TrAdderAtomic doesn't support set operation.")
    }

    fn get(&self) -> i64 {
        self.0.get()
    }

    fn inc_by(&self, delta: i64) {
        self.0.inc(delta)
    }

    fn dec_by(&self, delta: i64) {
        self.0.inc(-delta)
    }
}

pub type TrAdderGauge = GenericGauge<TrAdderAtomic>;
