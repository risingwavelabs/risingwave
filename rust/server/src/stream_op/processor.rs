use super::{Message, Result};

pub trait Processor {
    fn process(&mut self, msg: Message) -> Result<()>;
}
