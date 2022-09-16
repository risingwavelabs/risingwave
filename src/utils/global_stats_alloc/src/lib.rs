use stats_alloc::StatsAlloc;
use tikv_jemallocator::Jemalloc;

pub static INSTRUMENTED_JEMALLOC: StatsAlloc<Jemalloc> = StatsAlloc::new(Jemalloc);
