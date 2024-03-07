use std::collections::HashSet;
use std::time::{Duration, SystemTime};

const ERROR_SUPPRESSOR_RESET_DURATION: Duration = Duration::from_millis(60 * 60 * 1000); // 1h

#[derive(Debug)]
pub struct ErrorSuppressor {
    max_unique: usize,
    unique: HashSet<String>,
    last_reset_time: SystemTime,
}

impl ErrorSuppressor {
    pub fn new(max_unique: usize) -> Self {
        Self {
            max_unique,
            last_reset_time: SystemTime::now(),
            unique: Default::default(),
        }
    }

    pub fn suppress_error(&mut self, error: &str) -> bool {
        self.try_reset();
        if self.unique.contains(error) {
            false
        } else if self.unique.len() < self.max_unique {
            self.unique.insert(error.to_string());
            false
        } else {
            // We have exceeded the capacity.
            true
        }
    }

    pub fn max(&self) -> usize {
        self.max_unique
    }

    fn try_reset(&mut self) {
        if self.last_reset_time.elapsed().unwrap() >= ERROR_SUPPRESSOR_RESET_DURATION {
            *self = Self::new(self.max_unique)
        }
    }
}
