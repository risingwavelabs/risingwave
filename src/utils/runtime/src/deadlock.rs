use std::time::Duration;

/// Enable parking lot's deadlock detection.
pub fn enable_parking_lot_deadlock_detection() {
    tracing::info!("parking lot deadlock detection enabled");
    // TODO: deadlock detection as a feature instead of always enabling
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(3));
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        println!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            println!("Deadlock #{}", i);
            for t in threads {
                println!("Thread Id {:#?}", t.thread_id());
                println!("{:#?}", t.backtrace());
            }
        }
    });
}
