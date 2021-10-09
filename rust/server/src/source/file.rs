use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::error::RwError;
use crate::source::{Source, SourceConfig, SourceMessage, SourceReader};
use async_std::fs::File as AsyncFile;
use async_std::io::BufReader;
use futures::{AsyncBufReadExt, AsyncWriteExt};
use std::fs::File;

#[derive(Clone, Debug)]
pub struct FileSourceConfig {
    pub filename: String,
}

#[derive(Clone, Debug)]
pub struct FileMessage {
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct FileSource {
    config: FileSourceConfig,
}

pub struct FileSourceReader {
    file: BufReader<AsyncFile>,
    file_source: FileSource,
}

impl std::fmt::Debug for FileSourceReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSourceReader")
            .field("file_source", &self.file_source)
            .finish()
    }
}

#[async_trait::async_trait]
impl SourceReader for FileSourceReader {
    async fn next(&mut self) -> Result<Option<SourceMessage>> {
        let mut contents = Vec::new();
        match self.file.read_until(b'\n', &mut contents).await {
            Err(e) => Err(RwError::from(InternalError(e.to_string()))),
            Ok(n) => {
                if n == 0 {
                    Ok(None)
                } else {
                    Ok(Some(SourceMessage::File(FileMessage { data: contents })))
                }
            }
        }
    }

    async fn cancel(&mut self) -> Result<()> {
        match self.file.get_ref().close().await {
            Ok(_) => Ok(()),
            Err(e) => Err(RwError::from(InternalError(e.to_string()))),
        }
    }
}

impl Source for FileSource {
    fn new(config: SourceConfig) -> Result<Self>
    where
        Self: Sized,
    {
        if let SourceConfig::File(config) = config {
            Ok(FileSource { config })
        } else {
            Err(RwError::from(InternalError(
                "config is not FileSourceConfig".into(),
            )))
        }
    }

    fn reader(&self) -> Result<Box<dyn SourceReader>> {
        match File::open(self.config.filename.as_str()) {
            Ok(file) => Ok(Box::new(FileSourceReader {
                file: BufReader::new(AsyncFile::from(file)),
                file_source: self.clone(),
            })),
            Err(e) => Err(RwError::from(InternalError(e.to_string()))),
        }
    }
}
