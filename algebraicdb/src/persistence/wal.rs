use super::{TMP_EXTENSION, WAL_FILE_NAME};
use crate::ast::Stmt;
use crate::util::NumBytes;
use bincode;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::{rename, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

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
    file: Mutex<File>,
    file_size: AtomicUsize,
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
    ) -> (Self, Vec<(TransactionNumber, Option<Vec<u8>>)>) {
        let wal_path = data_dir.join(WAL_FILE_NAME);
        let (file_size, entries, file) = load_wal(&wal_path).await.expect("Loading WAL failed");

        let transaction_number = entries.last().map(|(n, _)| *n).unwrap_or(0);

        let wal = WriteAheadLog {
            state: Arc::new(WalState {
                file: Mutex::new(file),
                file_size: file_size.into(),
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

    pub async fn write(&mut self, stmt: &Stmt) -> io::Result<()> {
        let data = serialize_log_msg(stmt);

        let mut buf = Vec::with_capacity(data.len() + *ENTRY_START_SIZE + *ENTRY_END_SIZE);

        let mut file = self.state.file.lock().await;

        // We don't want multiple threads racing to increment this,
        // so we do it after acquiring the lock.
        let transaction_number = self
            .state
            .transaction_number
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let start = EntryBegin {
            transaction_number,
            entry_size: data.len(),
        };

        bincode::serialize_into(&mut buf, &start).unwrap();
        buf.extend_from_slice(&data);

        let end = EntryEnd {
            checksum: checksum(&buf),
        };
        bincode::serialize_into(&mut buf, &end).unwrap();

        // Write entry to the wal-file, and make sure it's synced to disk.
        file.write_all(&buf).await?;
        file.sync_all().await?;

        // Release the lock
        drop(file);

        self.state.file_size.fetch_add(buf.len(), Ordering::Relaxed);

        debug!("Wrote the following to the WAL:");
        debug!("start:  {:?}", start);
        debug!("entry:  {:?}", stmt);
        debug!("end:    {:?}", end);
        debug!("#bytes: {}", buf.len());

        Ok(())
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
            .open(&tmp_file_path)
            .await?;

        // Write the initial entry to the file
        new_file.write_all(&buf).await?;
        new_file.sync_all().await?;

        // Replace the new file with the old one in our state
        std::mem::swap(&mut new_file, &mut file);
        drop(new_file);

        // Replace the old file with the new one on disk.
        // _Should_ be no need to sync this, since the file
        // will get synced anyway after its first write.
        rename(&tmp_file_path, &file_path).await?;

        drop(file);

        info!("wal truncated");

        Ok(())
    }
}

fn serialize_log_msg(msg: &Stmt) -> Vec<u8> {
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
) -> io::Result<(usize, Vec<(TransactionNumber, Option<Vec<u8>>)>, File)> {
    let mut file: File = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(path)
        .await?;

    let mut data = Vec::new();
    let mut entries = Vec::new();

    {
        file.read_to_end(&mut data).await?;
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
                query = Some(data[..start.entry_size].into());
                data = &data[start.entry_size..];
            }

            let end: EntryEnd = match bincode::deserialize(&data[..*ENTRY_END_SIZE]) {
                Ok(stmt) => stmt,
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
