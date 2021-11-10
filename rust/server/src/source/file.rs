use std::fs::File;

use async_std::fs::File as AsyncFile;
use async_std::io::BufReader;
use futures::{AsyncBufReadExt, AsyncWriteExt};

use crate::source::{Source, SourceConfig, SourceMessage, SourceReader};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::error::RwError;

#[derive(Clone, Debug)]
pub struct FileSourceConfig {
    pub filename: String,
}

#[derive(Clone, Debug)]
pub struct FileMessage {
    pub offset: i64,
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
    async fn init(&mut self) -> Result<()> {
        // do nothing
        Ok(())
    }

    async fn poll_message(&mut self) -> Result<Option<SourceMessage>> {
        let mut contents = Vec::new();
        match self.file.read_until(b'\n', &mut contents).await {
            Err(e) => Err(RwError::from(InternalError(e.to_string()))),
            Ok(n) => {
                if n == 0 {
                    Ok(None)
                } else {
                    Ok(Some(SourceMessage::File(FileMessage {
                        offset: 0,
                        data: contents,
                    })))
                }
            }
        }
    }

    async fn next_message(&mut self) -> Result<SourceMessage> {
        todo!()
    }

    async fn cancel(&mut self) -> Result<()> {
        match self.file.get_ref().close().await {
            Ok(_) => Ok(()),
            Err(e) => Err(RwError::from(InternalError(e.to_string()))),
        }
    }
}

impl FileSource {
    pub fn new(config: SourceConfig) -> Result<Self> {
        if let SourceConfig::File(config) = config {
            Ok(FileSource { config })
        } else {
            Err(RwError::from(InternalError(
                "config is not FileSourceConfig".into(),
            )))
        }
    }
}

impl Source for FileSource {
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::Builder;

    use crate::source::{FileSource, FileSourceConfig, Source, SourceMessage};

    #[test]
    fn test_file_source() {
        let temp_file = Builder::new()
            .prefix("temp")
            .suffix(".txt")
            .rand_bytes(5)
            .tempfile()
            .unwrap();

        let mut file = temp_file.as_file();
        let data = r#"{"id": 1, "name": "john"}"#;
        assert!(file.write(data.as_ref()).is_ok());

        let path = temp_file.path().to_str().unwrap();
        let source = Box::new(FileSource {
            config: FileSourceConfig {
                filename: path.to_string(),
            },
        });

        let mut reader = source.reader().unwrap();
        let message = async_std::task::block_on(async { reader.poll_message().await });

        if let SourceMessage::File(message) = message.unwrap().unwrap() {
            assert_eq!(&message.data[..], data.as_bytes())
        }
    }
}
