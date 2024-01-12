use std::sync::Mutex;

use super::reader::SystemParamsReader;
use crate::util::tracing::layer::toggle_otel_layer;

/// Node-independent handler for system parameter changes.
///
/// Currently, it is only used to enable or disable the distributed tracing layer.
pub struct CommonHandler {
    last_params: Mutex<Option<SystemParamsReader>>,
}

impl CommonHandler {
    /// Create a new handler with the initial parameters.
    pub fn new(initial: SystemParamsReader) -> Self {
        let this = Self {
            last_params: None.into(),
        };
        this.handle_change(initial);
        this
    }

    /// Handle the change of system parameters.
    // TODO: directly call this method with the difference of old and new params.
    pub fn handle_change(&self, new_params: SystemParamsReader) {
        let mut last_params = self.last_params.lock().unwrap();

        if last_params.as_ref().map(|p| p.enable_tracing()) != Some(new_params.enable_tracing()) {
            toggle_otel_layer(new_params.enable_tracing());
        }

        last_params.replace(new_params);
    }
}
