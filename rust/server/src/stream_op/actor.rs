use super::Processor;

pub struct Actor {
    head: Box<dyn Processor>,
}
