use anyhow::Result;
use async_trait::async_trait;
use itertools::Itertools;
use log::{error, info};
use tokio::{sync as tokio_sync, time as tokio_time};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ItemChange {
    Add,
    Modify,
    Delete,
}

type LocationItem = (String, i64);
type LocationItemChangeEvent = (ItemChange, LocationItem);
type ItemChangeChannel = (
    tokio_sync::mpsc::Sender<LocationItemChangeEvent>,
    tokio_sync::mpsc::Receiver<LocationItemChangeEvent>,
);

const NO_MODIFICATION: i64 = i64::MIN;

#[async_trait]
pub trait Location: Send + Sync {
    async fn list_all_items(&self) -> Result<Option<Vec<LocationItem>>>;
    async fn list_change_items(
        &self,
        after_modification_time: i64,
    ) -> Result<Option<Vec<LocationItemChangeEvent>>>;
    fn last_modification_time(&self) -> i64;
    fn location(&self) -> Result<String>;
}

pub struct FetchResponse {
    data: Vec<u8>,
    location_item: LocationItem,
    offset: i64,
    more_data: bool,
}

pub trait LocationSubscriber: Send + Sync {
    fn fetch_next(&self, location_item: LocationItem) -> Result<FetchResponse>;
    fn list_metadata(&self) -> Option<Vec<LocationItem>>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TaskStatus {
    Started,
    Stopped,
}

#[async_trait]
trait Task<In: ?Sized, Out> {
    async fn run(
        &self,
        mut status: tokio_sync::watch::Receiver<TaskStatus>,
        input: In,
    ) -> &tokio_sync::mpsc::Receiver<Out>;
}

struct ListenerTask {
    period: tokio_time::Duration,
    item_change_channel: ItemChangeChannel,
}

impl ListenerTask {
    fn new(period: tokio_time::Duration, channel_buff: usize) -> Self {
        let (sender, receiver) = tokio_sync::mpsc::channel(channel_buff);
        Self {
            period,
            item_change_channel: (sender, receiver),
        }
    }

    fn if_ok_return<T>(
        rs: Result<Option<Vec<T>>>,
        map_fn: Box<dyn Fn(&T) -> LocationItemChangeEvent>,
    ) -> Vec<LocationItemChangeEvent> {
        let val = match rs.unwrap_or_else(|_| Some(vec![])) {
            Some(opt) => opt.iter().map(|t| map_fn(t)).collect_vec(),
            _ => {
                vec![]
            }
        };
        val
    }

    async fn listen(
        listen_location: Box<dyn Location>,
        period: tokio_time::Duration,
        mut status: tokio_sync::watch::Receiver<TaskStatus>,
        tx: tokio_sync::mpsc::Sender<LocationItemChangeEvent>,
    ) {
        let delay_for = tokio_time::Duration::from_millis(300);
        let start = tokio_time::Instant::now() + tokio_time::Duration::from_millis(500);
        let mut interval = tokio_time::interval_at(start, period);
        loop {
            tokio::select! {
                status_change = status.changed() => {
                    if status_change.is_ok() {
                        if let TaskStatus::Stopped = *status.borrow() {
                            info!("ListenerTask listen() Receive TaskStatus::Stopped.");
                            break;
                        }
                    }
                    continue;
                }
                _ = tokio_time::sleep(delay_for)=> {
                  info!("ListenerTask TaskStatus channel timeout next around.")
                }
            }
            let item_change_events = if listen_location.last_modification_time() == NO_MODIFICATION
            {
                let all_item_rs = listen_location.list_all_items().await;
                ListenerTask::if_ok_return(
                    all_item_rs,
                    Box::new(|a| -> LocationItemChangeEvent { (ItemChange::Add, a.clone()) }),
                )
            } else {
                let modification_time = listen_location.last_modification_time();
                let item_change_rs = listen_location.list_change_items(modification_time).await;
                ListenerTask::if_ok_return(
                    item_change_rs,
                    Box::new(|a| -> LocationItemChangeEvent { (a.clone().0, a.clone().1) }),
                )
            };
            interval.tick().await;
            for item in item_change_events {
                if let Err(send_rse_err) = tx.send(item.clone()).await {
                    error!(
                        "ListenerTask send location_item value error cause by {:?}",
                        send_rse_err
                    );
                } else {
                    // TODO metrics
                    info!("ListenerTask send location_item_change success");
                }
            }
        }
    }
}

#[async_trait]
impl Task<Box<dyn Location>, LocationItemChangeEvent> for ListenerTask {
    async fn run(
        &self,
        status: tokio_sync::watch::Receiver<TaskStatus>,
        input: Box<dyn Location>,
    ) -> &tokio_sync::mpsc::Receiver<LocationItemChangeEvent> {
        tokio::task::spawn(ListenerTask::listen(
            input,
            self.period,
            status,
            self.item_change_channel.0.clone(),
        ));
        &self.item_change_channel.1
    }
}

struct SubscribeTask {
    event_filter: fn(ItemChange) -> bool,
    receiver: tokio_sync::mpsc::Receiver<Vec<u8>>,
    sender: tokio_sync::mpsc::Sender<(Vec<u8>, i64)>,
    subscriber: Box<dyn LocationSubscriber>,
}

impl SubscribeTask {
    async fn fetch_task(&self, location_item: LocationItem) {
        loop {
            let fetch_rs = self.subscriber.fetch_next(location_item.clone());
            match fetch_rs {
                Ok(response) => {
                    let send_res = self.sender.send((response.data, response.offset)).await;
                    if !response.more_data {
                        break;
                    } else {
                        match send_res {
                            Ok(_) => {
                                continue;
                            }
                            Err(send_res_err) => {
                                error!(
                                    "fetch_task send() data to channel error. {:?}",
                                    send_res_err
                                );
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("fetch_task fetch_next() error cause by {:?}", e);
                    break;
                }
            };
        }
    }
}

#[async_trait]
impl Task<tokio_sync::mpsc::Receiver<LocationItemChangeEvent>, Vec<u8>> for SubscribeTask {
    async fn run(
        &self,
        mut status: tokio_sync::watch::Receiver<TaskStatus>,
        mut item_change_receiver: tokio_sync::mpsc::Receiver<LocationItemChangeEvent>,
    ) -> &tokio_sync::mpsc::Receiver<Vec<u8>> {
        // 1. if status == Stopped. return;
        // 2. if filter(changeEvent) == true continue;
        // 3. if fetch_next == NO_DATA (next around);
        loop {
            let event_change: LocationItemChangeEvent = tokio::select! {
                location_item_event = item_change_receiver.recv() => {
                    if let Some(item_change) = location_item_event {
                        item_change
                    } else {
                        info!("SubscribeTask not found data");
                        break;
                    }
                }
                status_change = status.changed() => {
                    if status_change.is_ok() {
                        if let TaskStatus::Stopped = *status.borrow() {
                            info!("SubscribeTask Receive TaskStatus::Stopped, task run() complete.");
                            break;
                        }
                    }
                    continue;
                }
            };

            if (self.event_filter)(event_change.0) {
                continue;
            }
            self.fetch_task(event_change.1).await;
        }
        &self.receiver
    }
}

struct TaskRunner {
    interval: tokio_time::Duration,
    status_sender: tokio_sync::watch::Sender<TaskStatus>,
}

impl TaskRunner {}

impl Drop for TaskRunner {
    fn drop(&mut self) {
        if self.status_sender.send(TaskStatus::Stopped).is_err() {
            error!("Send ListenerStatus::Stopped Error");
        }
    }
}
