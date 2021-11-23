use crate::storage::hummock::{HummockError, HummockResult};
use bytes::Bytes;
use std::collections::HashMap;

/// Mock file system for simple read and write.
/// Used for test only.
struct MockFileSystem {
    files: HashMap<String, Bytes>,
}

impl MockFileSystem {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    /// Write content to the mock file system.
    /// Create the path recursively if necessary.
    /// Error conditions:
    /// - The file already exists.
    pub async fn write(&mut self, path: String, content: Bytes) -> HummockResult<()> {
        // TODO: support file descriptor flags
        if self.files.contains_key(&*path) {
            return Err(HummockError::MockError("File already exists!".to_string()));
        }
        self.files.insert(path, content);
        Ok(())
    }

    /// Check whether a file exists.
    /// Return false if path does not exist.
    pub async fn has_file(&self, path: &str) -> bool {
        self.files.contains_key(path)
    }

    /// Read file content in the given range.
    /// Return None if path does not exist or given range is illegal.
    pub async fn read_at(&self, path: String, offset: u64, length: u64) -> Option<Bytes> {
        let offset = offset as usize;
        let length = length as usize;
        if !self.files.contains_key(&path) {
            return None;
        }
        let content = self.files.get(&*path).unwrap();
        if content.len() >= offset + length {
            Some(content.slice(offset..offset + length))
        } else {
            None
        }
    }

    /// Delete the file by the given path.
    /// If the file exists, return the deleted file content; otherwise return None.
    pub async fn delete(&mut self, path: &str) -> Option<Bytes> {
        self.files.remove(path)
    }

    /// Get file length.
    /// Return None if file does not exist.
    pub async fn len(&self, path: &str) -> Option<u64> {
        if self.has_file(path).await {
            Some(self.files.get(path).unwrap().len() as u64)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fill the given file system.
    async fn fill_fs(fs: &mut MockFileSystem) {
        for i in 0..100 {
            let path = format!("/test/log/{}.sst", i);
            let content = Bytes::from(format!("Mock file content: {}", i));
            fs.write(path, content).await.unwrap()
        }
    }

    /// Test file write
    #[tokio::test]
    async fn test_fs_write() {
        let mut fs = MockFileSystem::new();
        fill_fs(&mut fs).await;
    }

    /// Test file read
    #[tokio::test]
    async fn test_fs_read() {
        let mut fs = MockFileSystem::new();
        let path = format!("/test/log/{}.sst", 9);
        let content = Bytes::from(format!("Mock file content: {}", 9));
        fs.write(path.clone(), content.clone()).await.unwrap();

        // test when parameters are legal
        assert_eq!(
            content[1..],
            fs.read_at(path.clone(), 1, (content.len() - 1) as u64)
                .await
                .unwrap()
        );

        // test when parameters are illegal
        assert_eq!(None, fs.read_at(path, 1, content.len() as u64).await);
    }
}
