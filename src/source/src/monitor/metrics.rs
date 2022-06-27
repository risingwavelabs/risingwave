use prometheus::Registry;

pub struct SourceMetrics {
    pub registry: Registry,
}

impl SourceMetrics {
    pub fn new(registry: Registry) -> Self {
        SourceMetrics { registry }
    }
}
