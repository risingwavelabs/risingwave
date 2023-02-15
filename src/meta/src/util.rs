use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use delay_timer::prelude::*;
use tokio::task::JoinHandle;
use tokio::sync::oneshot::Sender;

pub struct GlobalEventManager {
    timer: DelayTimer,
    task_id: AtomicU64
}

impl Default for GlobalEventManager {
    fn default() -> Self {
        let timer  = DelayTimerBuilder::default().tokio_runtime_by_default().build();
        Self {
            timer,
            task_id: AtomicU64::new(0),
        }
    }
}

impl GlobalEventManager {
    pub fn register_interval_task<F, U>(&self, interval_sec: u64, f: F)
    where
        F: Fn() -> U + 'static + Send,
        U: Future + 'static + Send,
    {
        let mut task_builder = TaskBuilder::default();
        let task_id = self.task_id.fetch_add(1, Ordering::SeqCst);
        let task = task_builder.set_task_id(task_id).set_frequency_once_by_seconds(interval_sec).spawn_async_routine(f).unwrap();
        self.timer.add_task(task).unwrap();
    }
    
    pub fn register_loop_event<F, U>(&self, f: F)
        where
            F: Future<Output=()> + 'static + Send,
    {
        tokio::spawn(f);
    }
}

pub type Callback<T> = Box<dyn FnOnce(T) + Send>;
pub type WorkerHandle = (JoinHandle<()>, Sender<()>);