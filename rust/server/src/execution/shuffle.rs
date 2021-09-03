use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

pub struct TaskSinkWriter<T> {
    sender: Sender<T>,
}

pub struct TaskSinkReader<T> {
    receiver: Receiver<T>,
}
