use super::Processor;
/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor {
    processor: Box<dyn Processor>,
}
