use super::error::Result;

pub trait Filter: Send + Sync + 'static {
    fn filter(&self) -> Result<bool>;
}

pub struct DefaultFilter {}

impl Filter for DefaultFilter {
    fn filter(&self) -> Result<bool> {
        todo!()
    }
}
