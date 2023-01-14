use chrono::Duration;
use uuid::Uuid;

const REPORT_URL: &str = "unreachable";
const REPORT_INTERVAL_HOURS: i32 = 24;

struct TelemetryReport {}

struct TelemetryReporter {
    // tracking_id is persistent in etcd
    tracking_id: Uuid,
    // session_id is reset every time Meta node restarts
    session_id: Uuid,
}

impl TelemetryReporter {
    pub fn new() -> Self {
        Self {
            tracking_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
        }
    }

    pub async fn run() {
        tokio::spawn(async {});
    }
}

fn telemetry_enabled() -> bool {
    false
}
