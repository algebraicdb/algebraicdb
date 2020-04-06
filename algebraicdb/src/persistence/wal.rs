use crate::ast::Stmt;
use crate::util::NumBytes;
use bincode;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const WAL_NAME: &str = "wal";

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
}

pub enum WalError {
    CorruptedFile,
}

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
struct EntryBegin {
    transaction_number: TransactionNumber,
    entry_size: usize,
}

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
struct EntryEnd {
    checksum: u64,
}

impl WriteAheadLog {
    pub async fn new(
        data_dir: &PathBuf,
        truncate_at: NumBytes,
    ) -> (Self, Vec<(TransactionNumber, Vec<u8>)>) {
        let wal_path = data_dir.join(WAL_NAME);
        let (file_size, entries, file) = load_wal(&wal_path).await.expect("Loading WAL failed");

        let transaction_number = entries.last().map(|(n, _)| *n).unwrap_or(0);

        let wal = WriteAheadLog {
            state: Arc::new(WalState {
                file: Mutex::new(file),
                file_size: file_size.into(),
                truncate_at,
                transaction_number: transaction_number.into(),
            }),
        };

        (wal, entries)
    }

    pub fn transaction_number(&self) -> TransactionNumber {
        self.state.transaction_number.load(Ordering::Relaxed)
    }

    pub async fn write(&mut self, stmt: &Stmt<'_>) -> io::Result<()> {
        let data = serialize_log_msg(stmt);

        let mut buf = Vec::with_capacity(data.len() + 16);

        let mut file = self.state.file.lock().await;

        // We don't want multiple threads racing to increment this,
        // so we do it after acquiring the lock.
        let transaction_number = self
            .state
            .transaction_number
            .fetch_add(1, Ordering::Relaxed);
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

        eprintln!("Wrote the following to the WAL:");
        eprintln!("start:  {:?}", start);
        eprintln!("msg:    {:?}", stmt);
        eprintln!("end:    {:?}", end);
        eprintln!("#bytes: {}", buf.len());
        eprintln!();

        Ok(())
    }

    /// Truncate the wal if it needs it
    pub async fn truncate_wal(&mut self, until_tn: TransactionNumber) -> io::Result<()> {
        if self.transaction_number() <= until_tn
            || self.state.file_size.load(Ordering::Relaxed) < self.state.truncate_at.0
        {
            return Ok(());
        }
        unimplemented!("truncate wal")
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
) -> io::Result<(usize, Vec<(TransactionNumber, Vec<u8>)>, File)> {
    let mut file: File = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(path)
        .await?;

    let size_of_entry_begin = bincode::serialized_size(&EntryBegin::default()).unwrap() as usize;
    let size_of_entry_end = bincode::serialized_size(&EntryEnd::default()).unwrap() as usize;

    let mut data = Vec::new();
    let mut entries = Vec::new();

    {
        file.read_to_end(&mut data).await?;
        let mut data = &data[..];
        loop {
            if data.is_empty() {
                break;
            } else if size_of_entry_begin > data.len() {
                panic!("Corrupt WAL");
            }

            let start: EntryBegin = match bincode::deserialize(&data[..size_of_entry_begin]) {
                Ok(eb) => eb,
                Err(_) => break,
            };

            if start.entry_size + size_of_entry_begin + size_of_entry_end > data.len() {
                panic!("Corrupt WAL");
            }

            let calculated_checksum = {
                let checksum_area = size_of_entry_begin + start.entry_size;
                checksum(&data[..checksum_area])
            };

            data = &data[size_of_entry_begin..];

            let query = data[..start.entry_size].into();
            data = &data[start.entry_size..];

            let end: EntryEnd = match bincode::deserialize(&data[..size_of_entry_end]) {
                Ok(stmt) => stmt,
                Err(_) => panic!("Corrupt WAL"),
            };
            data = &data[size_of_entry_end..];

            if end.checksum != calculated_checksum {
                panic!("Corrupt WAL: Invalid checksum")
            }

            eprintln!("Read the following from the WAL:");
            eprintln!("start: {:?}", start);
            eprintln!("msg:   {:?}", query);
            eprintln!("end:   {:?}", end);
            eprintln!();

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
