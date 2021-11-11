use crate::metadata::Epoch;
use std::sync::Mutex;

/// `Watermark` is a simple implementation for concurrency control.
/// Need more detailed control logic.
pub struct Watermark(Mutex<Epoch>);

impl Watermark {
    pub fn new() -> Self {
        Watermark(Mutex::new(Epoch::from(0)))
    }

    pub fn clone(&self) -> Epoch {
        *self.0.lock().unwrap()
    }

    pub fn inited(&self) -> bool {
        let wm = self.0.lock().unwrap();
        wm.into_inner() != 0
    }

    pub fn update(&self, epoch: Epoch) {
        let mut wm = self.0.lock().unwrap();
        if *wm < epoch {
            *wm = epoch;
        }
    }

    pub fn changed(&self, epoch: Epoch) -> bool {
        *self.0.lock().unwrap() > epoch
    }
}
