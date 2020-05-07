use super::{TMP_EXTENSION, WAL_FILE_NAME};
use crate::ast::Stmt;
use crate::util::NumBytes;
use bincode;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
//use tokio::fs::{rename, File, OpenOptions};
use std::fs::{File, OpenOptions, rename};
use std::io::{Write, Read};

use tokio::sync::{Mutex, Notify};

/// With every transaction written to the WAL, this number is incremented by 1.
/// The first transaction must be indexed with 1, since 0 means no transactions has happened yet.
pub type TransactionNumber = u64;

pub enum WriteToWal {
    Yes,
    No,
}

#[derive(Clone)]
pub struct WriteAheadLog {
    state: Arc<WalState>,
}

struct WalState {
    /// File-descriptor for the log-file
    file: Mutex<File>,

    /// The current size of the log-file
    file_size: AtomicUsize,

    /// A buffer for tasks waiting for their log entries to be flushed to disk
    /// First notifier is for the executor to await the wal flushing the transaction to disk
    /// Second notifier is for the wal to await the executor to access table data locks
    write_buffer: Mutex<Vec<(Vec<u8>, Arc<Notify>, Arc<Notify>)>>,

    /// Truncate the wal when it reaches this size
    truncate_at: NumBytes,

    transaction_number: AtomicU64,
    data_dir: PathBuf,
}

pub enum WalError {
    CorruptedFile,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct EntryBegin {
    transaction_number: TransactionNumber,

    /// The size in bytes of the associated serialized query.
    /// Set to 0 if there is no associated query
    entry_size: usize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct EntryEnd {
    checksum: u64,
}

lazy_static! {
    static ref ENTRY_START_SIZE: usize = {
        bincode::serialized_size(&EntryBegin {
            transaction_number: 0,
            entry_size: 0,
        })
        .unwrap() as usize
    };
    static ref ENTRY_END_SIZE: usize =
        bincode::serialized_size(&EntryEnd { checksum: 0 }).unwrap() as usize;
}

impl WriteAheadLog {
    pub async fn new(
        data_dir: PathBuf,
        truncate_at: NumBytes,
    ) -> (Self, Vec<(TransactionNumber, Option<Vec<Stmt>>)>) {
        let wal_path = data_dir.join(WAL_FILE_NAME);
        let (file_size, entries, file) = load_wal(&wal_path).await.expect("Loading WAL failed");

        let transaction_number = entries.last().map(|(n, _)| *n).unwrap_or(0);
        let wal = WriteAheadLog {
            state: Arc::new(WalState {
                file: Mutex::new(file),
                file_size: file_size.into(),
                write_buffer: Mutex::new(Vec::new()),
                truncate_at,
                transaction_number: transaction_number.into(),
                data_dir,
            }),
        };

        (wal, entries)
    }

    pub fn transaction_number(&self) -> TransactionNumber {
        self.state.transaction_number.load(Ordering::Relaxed)
    }

    pub async fn write(&mut self, stmts: &Vec<Stmt>) -> io::Result<Arc<Notify>> {
        let stmts_data = serialize_log_msg(stmts);

        let mut data = Vec::with_capacity(stmts_data.len() + *ENTRY_START_SIZE + *ENTRY_END_SIZE);

        // TODO: we don't acquire the file lock here anymore, double check this
        // also changed Ordering::Relaxed to Ordering::SeqCst, maybe that's enough.
        // vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
        // We don't want multiple threads racing to increment this,
        // so we do it after acquiring the lock.
        let transaction_number = self
            .state
            .transaction_number
            .fetch_add(1, Ordering::SeqCst)
            + 1;

        let start = EntryBegin {
            transaction_number,
            entry_size: stmts_data.len(),
        };

        bincode::serialize_into(&mut data, &start).unwrap();
        data.extend_from_slice(&stmts_data);

        let end = EntryEnd {
            checksum: checksum(&data),
        };
        bincode::serialize_into(&mut data, &end).unwrap();

        // Prepare a notifier
        let start_task_notifier = Arc::new(Notify::new());
        let task_started_notifier = Arc::new(Notify::new());

        // Lock the write buffer
        let mut write_buffer = self.state.write_buffer.lock().await;
        if write_buffer.is_empty() {
            let state = self.state.clone();
            tokio::task::spawn(async move {
                async fn flush_wal(state: Arc<WalState>) -> io::Result<()> {
                    tokio::time::delay_for(Duration::from_millis(10)).await;
                    // here we wait for a few µs
                    // then we take the lock and flush the buffer

                    let mut file = state.file.lock().await;
                    let mut write_buffer = state.write_buffer.lock().await;

                    for (data, _, _) in write_buffer.iter() {
                        // Write entry to the wal-file
                        file.write_all(&data)?;
                        state.file_size.fetch_add(data.len(), Ordering::Relaxed);
                    }

                    file.sync_all()?;

                    // Release the lock
                    drop(file);

                    for (_data, start_task, task_started) in write_buffer.drain(..) {
                        start_task.notify();
                        task_started.notified().await;
                    }
                    Ok(())
                }
                
                if let Err(e) = flush_wal(state).await {
                    error!("error writing to WAL: {}", e);
                    panic!("error writing to WAL: {}", e);
                }
            });
        }

        let data_len = data.len();
        write_buffer.push((data, start_task_notifier.clone(), task_started_notifier.clone()));

        // Release the lock
        drop(write_buffer);

        start_task_notifier.notified().await;
        
        debug!("Wrote the following to the WAL:");
        debug!("start:  {:?}", start);
        debug!("entry:  {:#?}", stmts);
        debug!("end:    {:?}", end);
        debug!("#bytes: {}", data_len);

        Ok(task_started_notifier)
    }

    /// Truncate the wal if it needs it
    ///
    /// `until_tn` is the TransactionNumber of the latest snapshot.
    ///
    /// The WAL will only be truncated if...
    /// - ...its size has reached the truncate threshold
    /// - ...`until_tn` matches the latest entry in the wal
    pub async fn truncate_wal(&mut self, until_tn: TransactionNumber) -> io::Result<()> {
        // Don't truncate if the wal hasn't reached max size
        if self.state.file_size.load(Ordering::Relaxed) < self.state.truncate_at.0 {
            return Ok(());
        }

        let file_path = self.state.data_dir.join(WAL_FILE_NAME);
        let mut tmp_file_path = file_path.clone();
        tmp_file_path.set_extension(TMP_EXTENSION);

        // Take the file lock
        let mut file = self.state.file.lock().await;

        // Only truncate the wal if everything is synced to disk
        // Under heavy load, if this function is called after snapshotting, this check might not
        // pass. In which case, the wal might grow past the maximum size.
        let transaction_number = self.state.transaction_number.load(Ordering::Relaxed);
        if transaction_number != until_tn {
            return Ok(());
        }

        info!("truncating wal...");

        // Serialize an initial entry, with the current transaction number
        let mut buf = Vec::with_capacity(*ENTRY_START_SIZE + *ENTRY_END_SIZE);

        let start = EntryBegin {
            transaction_number,
            entry_size: 0,
        };
        bincode::serialize_into(&mut buf, &start).unwrap();

        let end = EntryEnd {
            checksum: checksum(&buf),
        };
        bincode::serialize_into(&mut buf, &end).unwrap();

        // Create a new temporary file
        let mut new_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_file_path)?;

        // Write the initial entry to the file
        new_file.write_all(&buf)?;
        new_file.sync_all()?;

        // Replace the new file with the old one in our state
        std::mem::swap(&mut new_file, &mut file);
        drop(new_file);

        // Replace the old file with the new one on disk.
        // _Should_ be no need to sync this, since the file
        // will get synced anyway after its first write.
        rename(&tmp_file_path, &file_path)?;

        drop(file);

        info!("wal truncated");

        Ok(())
    }
}

fn serialize_log_msg(msg: &Vec<Stmt>) -> Vec<u8> {
    let mut data = Vec::new();
    bincode::serialize_into(&mut data, msg).unwrap();
    data
}

fn deserialize_log_msg(data: &[u8]) -> Result<Stmt, WalError> {
    Ok(bincode::deserialize(data)?)
}

fn checksum(data: &[u8]) -> u64 {
    seahash::hash(data)
}

pub async fn load_wal(
    path: &PathBuf,
) -> io::Result<(usize, Vec<(TransactionNumber, Option<Vec<Stmt>>)>, File)> {
    let mut file: File = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(path)?;

    let mut data = Vec::new();
    let mut entries = Vec::new();

    {
        file.read_to_end(&mut data)?;
        let mut data = &data[..];
        loop {
            if data.is_empty() {
                break;
            } else if *ENTRY_START_SIZE > data.len() {
                panic!("Corrupt WAL");
            }

            let start: EntryBegin = match bincode::deserialize(&data[..*ENTRY_START_SIZE]) {
                Ok(eb) => eb,
                Err(_) => break,
            };

            let calculated_checksum = {
                let checksum_area = *ENTRY_START_SIZE + start.entry_size;
                checksum(&data[..checksum_area])
            };

            if start.entry_size + *ENTRY_START_SIZE + *ENTRY_END_SIZE > data.len() {
                panic!("Corrupt WAL");
            }

            data = &data[*ENTRY_START_SIZE..];

            let mut query = None;
            if start.entry_size > 0 {
                query = Some(match bincode::deserialize(&data[..start.entry_size]) {
                    Ok(stmts) => stmts,
                    Err(_) => panic!("Corrupt WAL"),
                });
                data = &data[start.entry_size..];
            }

            let end: EntryEnd = match bincode::deserialize(&data[..*ENTRY_END_SIZE]) {
                Ok(end) => end,
                Err(_) => panic!("Corrupt WAL"),
            };
            data = &data[*ENTRY_END_SIZE..];

            if end.checksum != calculated_checksum {
                panic!("Corrupt WAL: Invalid checksum")
            }

            debug!("Read the following from the WAL:");
            debug!("start: {:?}", start);
            debug!("entry: {:?}", query);
            debug!("end:   {:?}", end);

            entries.push((start.transaction_number, query));
        }
    }

    Ok((data.len(), entries, file))
}

impl From<bincode::Error> for WalError {
    fn from(_error: bincode::Error) -> Self {
        WalError::CorruptedFile
    }
}
