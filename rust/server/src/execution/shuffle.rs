use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

pub(crate) struct TaskSinkWriter<T> {
    sender: Sender<T>,
}

pub(crate) struct TaskSinkReader<T> {
    receiver: Receiver<T>,
}
