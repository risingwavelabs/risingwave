pub(crate) fn time_to_str(time_nano: u128) -> String {
    match time_nano {
        nano_sec if time_nano < 1000 => format! {"{} nano sec", nano_sec},
        micro_sec if time_nano < 1_000_000 => format! {"{:.3} micro sec", micro_sec as f64 /1000.0},
        milli_sec if time_nano < 1_000_000_000 => {
            format! {"{:.3} milli sec", milli_sec as f64 /1_000_000.0}
        }
        sec => format! {"{:.3} sec", sec as f64 /1_000_000_000.0},
    }
}

pub(crate) struct LatencyStat {
    pub mean: String,
    pub p50: String,
    pub p90: String,
    pub p99: String,
    pub std_dev: f64,
}

impl LatencyStat {
    /// calculate latencies statistics
    pub(crate) fn new(mut latencies: Vec<u128>) -> LatencyStat {
        latencies.sort_unstable();

        LatencyStat {
            mean: time_to_str(latencies.iter().sum::<u128>() / latencies.len() as u128),
            p50: time_to_str(latencies[(latencies.len() as f64 * 0.50) as usize]),
            p90: time_to_str(latencies[(latencies.len() as f64 * 0.9) as usize]),
            p99: time_to_str(latencies[(latencies.len() as f64 * 0.99) as usize]),
            std_dev: LatencyStat::std_deviation(&latencies),
        }
    }

    fn std_deviation(data: &[u128]) -> f64 {
        let mean: u128 = data.iter().sum::<u128>() / data.len() as u128;
        let suqa_diff = data
            .iter()
            .map(|value| {
                let diff = *value as i128 - mean as i128;
                diff * diff
            })
            .sum::<i128>();

        (suqa_diff as f64 / data.len() as f64).sqrt()
    }
}
