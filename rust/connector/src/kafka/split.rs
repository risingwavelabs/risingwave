use crate::base::SourceSplit;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum KafkaOffset {
    Earliest,
    Latest,
    Offset(i64),
    Timestamp(i64),
    None,
}

#[derive(Copy, Clone)]
pub struct KafkaSplit {
    pub(crate) partition: i32,
    pub(crate) start_offset: KafkaOffset,
    pub(crate) stop_offset: KafkaOffset,
}

impl SourceSplit for KafkaSplit {
    fn id(&self) -> String {
        format!("{}", self.partition)
    }
}

impl KafkaSplit {
    pub fn new(partition: i32, start_offset: KafkaOffset, stop_offset: KafkaOffset) -> KafkaSplit {
        KafkaSplit {
            partition,
            start_offset,
            stop_offset,
        }
    }
}
