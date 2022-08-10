// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync as tokio_sync;

///  [`EntryStat`] Describes a directory or file. A file is a generic concept,
/// and can be a local file, a distributed file system, or a bucket in S3.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntryStat {
    pub(crate) path: String,
    pub(crate) atime: i64,
    pub(crate) mtime: i64,
    pub(crate) size: i64,
}

/// The operations supported on Entry/file.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EntryOpt {
    Add,
    Modify,
    Delete,
}

/// For a certain Entry is aware of its changes, such as deleting a file,
/// or having the contents of an appended write.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EntryDiscover {
    None,
    Auto,
}

impl Default for EntryStat {
    fn default() -> Self {
        EntryStat {
            path: "_NONE_".to_string(),
            atime: 0,
            mtime: 0,
            size: 0,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EntryOptEvent {
    pub(crate) entry_operation: EntryOpt,
    pub(crate) entry: EntryStat,
}

impl Default for EntryOptEvent {
    fn default() -> Self {
        EntryOptEvent {
            entry_operation: EntryOpt::Add,
            entry: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StatusWatch {
    Running,
    Stopped,
}

/// Unlike the concept in the OS, the abstraction of a location is represented in the current
/// context. For external resources we assume that none of them have the permission to modify,
/// so in Directory we only support read operations. However, unlike the classic File API,
/// it is necessary to be able to sense changes in this directory.
///
/// In contrast to the concept in MessageSystem source, Directory is similar to the concept of
/// **Topic**; the Entry under Directory is equivalent to the **Partition** in Topic.
#[async_trait]
pub trait Directory: Send + Sync {
    async fn push_entries_change(
        &self,
        sender: tokio_sync::mpsc::Sender<EntryOptEvent>,
        task_status: tokio_sync::watch::Receiver<StatusWatch>,
    ) -> Result<()>;
    async fn list_entries(&self) -> Result<Vec<EntryStat>>;
    // async fn last_modification(&self) -> i64;
    fn entry_discover(&self) -> EntryDiscover;
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;
    use chrono::Local;
    use itertools::Itertools;
    use tokio::{sync, time};

    use crate::source::filesystem::file_common::{
        Directory, EntryDiscover, EntryOpt, EntryOptEvent, EntryStat, StatusWatch,
    };

    // path, init_count, total_count, file_size
    #[derive(Debug, Clone)]
    struct MockFileSystemConf(String, i32, i32, i32);

    // name, ctime, mtime,size
    #[derive(Debug, Clone)]
    struct MockFileMeta {
        path: String,
        ctime: i64,
        mtime: i64,
        size: i64,
    }

    // struct MockFileMeta(String, i64, i64, i64);
    impl MockFileMeta {
        fn new(path: String, mtime: i64) -> Self {
            Self {
                path,
                ctime: mtime,
                mtime,
                size: i64::MAX - 10000,
            }
        }
    }

    impl From<MockFileMeta> for EntryStat {
        fn from(meta: MockFileMeta) -> Self {
            EntryStat {
                path: meta.path,
                atime: meta.ctime,
                mtime: meta.mtime,
                size: meta.size,
            }
        }
    }

    #[derive(Debug, Clone)]
    struct MockFileSystem {
        meta: MockFileSystemConf,
        control_signal: sync::mpsc::Sender<i32>,
        entries: Arc<tokio::sync::Mutex<HashMap<String, MockFileMeta>>>,
        state_change_complete_notify: Arc<tokio::sync::Notify>,
    }

    impl MockFileSystem {
        fn new(meta: MockFileSystemConf) -> Self {
            let (tx, rx) = sync::mpsc::channel(1);
            let mut init_entries = HashMap::new();
            for i in 0..meta.1 {
                let file_name = format!("{}_{}", "file", i);
                let mtime = (Local::now() - chrono::Duration::minutes(i as i64)).timestamp_millis();
                init_entries.insert(file_name.clone(), MockFileMeta::new(file_name, mtime));
            }
            let file_system = MockFileSystem {
                meta,
                entries: Arc::new(tokio::sync::Mutex::new(init_entries)),
                control_signal: tx,
                state_change_complete_notify: Arc::new(tokio::sync::Notify::new()),
            };
            let file_for_task = Arc::new(file_system.clone());
            tokio::task::spawn(async move {
                file_for_task.update_state_once(rx).await;
            });
            file_system
        }

        async fn update_state_once(&self, mut rx: sync::mpsc::Receiver<i32>) {
            let mut interval = time::interval(time::Duration::from_millis(1));
            let total = self.meta.2 - self.meta.1;
            let init = self.meta.1;
            let mut index = 0;
            println!("MockFileSystem update_state running");
            loop {
                let msg = tokio::select! {
                    _ = interval.tick() => {
                        3_i32
                    }
                    Some(m) = rx.recv() => {
                        Some(m).unwrap()
                    }
                };
                match msg {
                    1_i32 => {
                        if index < 1 {
                            let mut entries_guard = self.entries.lock().await;
                            for name_idx in init..=total {
                                let mtime = (Local::now()
                                    - chrono::Duration::minutes(name_idx as i64))
                                .timestamp_millis();
                                let file_name = format!("{}_{}", "file", name_idx);
                                let file_meta = MockFileMeta::new(file_name.clone(), mtime);
                                // println!("newFileMeta={:?}", file_meta.clone());
                                entries_guard.insert(file_name.clone(), file_meta);
                            }
                            self.state_change_complete_notify.notify_one();
                        }
                        index += 1;
                    }
                    2_i32 => {
                        break;
                    }
                    _ => {
                        continue;
                    }
                }
            }
        }

        async fn state_change(&self, signal: i32) {
            let rs = self.control_signal.send(signal).await;
            println!("MockFileSystem send state_change rs = {:?}", rs);
        }

        async fn wait_state_change_complete(&self) {
            self.state_change_complete_notify.notified().await;
        }

        async fn list_files(&self) -> HashMap<String, MockFileMeta> {
            let read_guard = self.entries.lock().await;
            (*read_guard).clone()
        }
    }

    #[derive(Debug, Clone)]
    pub struct DummyDirectory {
        file_system: MockFileSystem,
        last_modification: Arc<AtomicI64>,
        auto_discover: EntryDiscover,
    }

    impl DummyDirectory {
        fn new(file_system: MockFileSystem) -> Self {
            Self {
                file_system,
                last_modification: Arc::new(AtomicI64::new(i64::MIN)),
                auto_discover: EntryDiscover::Auto,
            }
        }
    }

    #[async_trait]
    impl Directory for DummyDirectory {
        async fn push_entries_change(
            &self,
            sender: tokio::sync::mpsc::Sender<EntryOptEvent>,
            mut task_status: tokio::sync::watch::Receiver<StatusWatch>,
        ) -> anyhow::Result<()> {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(2));
            let file_system_clone = Arc::new(&self.file_system);
            println!(
                "entry_change_listener running. {:?}",
                self.last_modification.load(Ordering::SeqCst),
            );
            let rs = loop {
                tokio::select! {
                    _= interval.tick() => {
                        // println!("next around.");
                    },
                    status = task_status.changed() => {
                        if status.is_ok() {
                            if let StatusWatch::Stopped = *task_status.borrow() {
                                break Ok(());
                            }
                        }
                    }
                }
                let file_values = file_system_clone.list_files().await;
                let curr_modification = self.last_modification.load(Ordering::SeqCst);

                let sorted_files = file_values
                    .values()
                    .filter(|p_value| p_value.mtime > curr_modification)
                    .cloned()
                    .sorted_by(|m1, m2| Ord::cmp(&m1.mtime, &m2.mtime))
                    .collect_vec();

                if sorted_files.is_empty() {
                    let _instant = interval.tick().await;
                    continue;
                }
                let curr_max_modification = sorted_files.last().unwrap().mtime;
                self.last_modification
                    .store(curr_max_modification, Ordering::SeqCst);
                let mut send_opt_event_err = None;
                for change_event in &sorted_files {
                    let tx_send_rs = sender
                        .send(EntryOptEvent {
                            entry_operation: EntryOpt::Add,
                            entry: EntryStat::from(change_event.clone()),
                        })
                        .await;
                    if tx_send_rs.is_err() {
                        send_opt_event_err = Some(tx_send_rs.err().unwrap());
                        break;
                    } else {
                        continue;
                    }
                }
                match send_opt_event_err {
                    Some(tx_send_err) => {
                        break Err(anyhow::Error::from(tx_send_err));
                        // break Error::from(tx_send_err.to_string());
                    }
                    None => {
                        continue;
                    }
                }
            };
            rs
        }

        async fn list_entries(&self) -> Result<Vec<EntryStat>> {
            let entries = self
                .file_system
                .list_files()
                .await
                .values()
                .map(|value| EntryStat {
                    path: value.path.to_string(),
                    atime: value.ctime,
                    mtime: value.mtime,
                    size: value.size,
                })
                .collect_vec();
            Ok(entries)
        }

        // async fn last_modification(&self) -> i64 {
        //     self.last_modification.load(Ordering::SeqCst)
        // }

        fn entry_discover(&self) -> EntryDiscover {
            self.auto_discover
        }
    }

    fn default_filesystem_conf() -> MockFileSystemConf {
        MockFileSystemConf("/mock/path".to_string(), 1, 5, 10000)
    }

    #[tokio::test]
    async fn test_mock_file_system() {
        let file_system = MockFileSystem::new(default_filesystem_conf());
        let files_change_before = file_system.list_files().await;
        assert_eq!(1, files_change_before.len());
        println!(
            "MockFileSystem files_change_before = {:?}",
            files_change_before
        );
        file_system.state_change(1).await;
        file_system.wait_state_change_complete().await;
        let files_change_after = file_system.list_files().await;
        assert_eq!(5, files_change_after.len());
        file_system.state_change(2).await;
        println!(
            "MockFileSystem files_change_after = {:?}",
            files_change_after
        );
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_push_entries_change() {
        let file_system = MockFileSystem::new(default_filesystem_conf());
        let directory = DummyDirectory::new(file_system.clone());
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let (s_tx, s_rx) = tokio::sync::watch::channel(StatusWatch::Running);
        let _ = s_tx.send(StatusWatch::Running);
        let directory_for_clone = Arc::new(directory.clone());
        file_system.state_change(1).await;
        file_system.wait_state_change_complete().await;
        let send_join = tokio::task::spawn(async move {
            let rs = directory_for_clone.push_entries_change(tx, s_rx).await;
            match rs {
                Ok(()) => {
                    println!("send success");
                }
                Err(err) => {
                    panic!("send operator should be no SendError {:?}", err);
                }
            }
            println!("sender task complete");
        });
        let recv_join = tokio::task::spawn(async move {
            for _i in 0..5 {
                let receive_event = rx.recv().await;
                println!("receive event change = {:?}", receive_event);
            }
            let send_rs = s_tx.send(StatusWatch::Stopped);
            assert!(send_rs.is_ok());
            println!("receive task complete");
        });
        send_join.await.unwrap();
        recv_join.await.unwrap();
    }
}
