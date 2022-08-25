#[cfg(sync_test)]
mod sync;
#[cfg(sync_test)]
pub use sync::*;

#[cfg(not(sync_test))]
#[inline(always)]
#[expect(clippy::unused_async)]
pub async fn on_sync_point(_sync_point: &str) -> Result<(), Error> {
    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Wait for signal {0} timeout")]
    WaitForSignalTimeout(String),
}
