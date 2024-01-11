use std::sync::Mutex;

use super::reader::SystemParamsReader;

pub struct CommonHandler {
    last_params: Mutex<Option<SystemParamsReader>>,
}

impl CommonHandler {
    pub fn new(initial: SystemParamsReader) -> Self {
        let this = Self {
            last_params: None.into(),
        };
        this.handle_change(initial);
        this
    }

    pub fn handle_change(&self, new_params: SystemParamsReader) {
        let mut last_params = self.last_params.lock().unwrap();

        tracing::info!("params: {:?}", new_params);

        last_params.replace(new_params);
    }
}
