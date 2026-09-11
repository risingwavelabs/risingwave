#![feature(register_tool)]
#![register_tool(rw)]
#![warn(rw::temporary_guarded_metric)]

struct MetricVec;

struct GuardedMetric;

impl MetricVec {
    fn with_guarded_label_values(&self, _labels: &[&str]) -> GuardedMetric {
        GuardedMetric
    }
}

impl GuardedMetric {
    fn set(&self, _value: i64) {}

    fn inc(&self) {}

    fn observe(&self, _value: f64) {}

    fn local(&self) -> GuardedMetric {
        GuardedMetric
    }
}

fn main() {
    let labels = ["source_id"];
    let metrics = MetricVec;

    metrics.with_guarded_label_values(&labels).set(1);

    metrics.with_guarded_label_values(&labels).inc();

    metrics.with_guarded_label_values(&labels).observe(1.0);

    let metric = metrics.with_guarded_label_values(&labels);
    metric.set(1);

    let local_metric = metrics.with_guarded_label_values(&labels).local();
    local_metric.inc();
}
