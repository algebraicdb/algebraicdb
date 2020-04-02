use bincode;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use std::io;
use crate::ast::Stmt;
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicU64, Ordering};

const WAL_NAME: &str = "wal";

pub type TransactionNumber = u64;

pub(crate) static TRANSACTION_NUMBER: AtomicU64 = AtomicU64::new(0);

type LogRequest = Vec<u8>;
type LogResponse = ();

type WalMsg = (LogRequest, oneshot::Sender<LogResponse>);
type RequestSender = mpsc::Sender<WalMsg>;
type RequestReceiver = mpsc::Receiver<WalMsg>;

#[derive(Clone)]
pub struct WriteAheadLog {
    channel: RequestSender,
}

pub enum WalError {
    CorruptedFile
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
    pub fn new() -> Self {
        let (requests_in, requests_out) = mpsc::channel(256);

        task::spawn(async {
            wal_writer(requests_out).await.expect("WAL crashed");
        });

        WriteAheadLog {
            channel: requests_in,
        }
    }

    pub async fn write(&mut self, stmt: &Stmt) {
        let data = serialize_log_msg(stmt);
        let (tx, rx) = oneshot::channel();
        self.channel.send((data, tx)).await
            .expect("WAL crashed");
        rx.await.expect("WAL crashed");
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

pub async fn wal_writer(mut channel: RequestReceiver) -> io::Result<()> {
    let mut file: File = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(WAL_NAME)
        .await?;

    let size_of_entry_begin = bincode::serialized_size(&EntryBegin::default()).unwrap() as usize;
    let size_of_entry_end = bincode::serialized_size(&EntryEnd ::default()).unwrap() as usize;

    let mut transaction_number: TransactionNumber = 0;

    { // Read existing WAL
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        let mut data = &data[..];
        loop {
            if data.is_empty() {
                break;
            }
            else if size_of_entry_begin > data.len() {
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

            let msg: Stmt = match bincode::deserialize(&data[..start.entry_size]) {
                Ok(stmt) => stmt,
                Err(_) => panic!("Corrupt WAL"),
            };
            data = &data[start.entry_size..];

            let end: EntryEnd = match bincode::deserialize(&data[..size_of_entry_end]) {
                Ok(stmt) => stmt,
                Err(_) => panic!("Corrupt WAL"),
            };
            data = &data[size_of_entry_end..];

            if end.checksum != calculated_checksum {
                panic!("Corrupt WAL: Invalid checksum")
            }

            transaction_number = start.transaction_number;

            eprintln!("Read the following from the WAL:");
            eprintln!("start: {:?}", start);
            eprintln!("msg:   {:?}", msg);
            eprintln!("end:   {:?}", end);
            eprintln!();
        }
    }

    TRANSACTION_NUMBER.store(transaction_number, Ordering::Relaxed);

    while let Some((msg, out)) = channel.recv().await {
        transaction_number += 1;
        let start = EntryBegin {
            transaction_number,
            entry_size: msg.len(),
        };

        let mut buf = Vec::with_capacity(msg.len() + 16);

        bincode::serialize_into(&mut buf, &start).unwrap();
        buf.extend_from_slice(&msg);

        let end = EntryEnd {
            checksum: checksum(&buf),
        };
        bincode::serialize_into(&mut buf, &end).unwrap();

        // Write entry to the wal-file, and make sure it's synced to disk.
        file.write_all(&buf).await?;
        file.sync_all().await?;

        TRANSACTION_NUMBER.store(start.transaction_number, Ordering::Relaxed);

        out.send(())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Response channel closed"))?;

        eprintln!("Wrote the following to the WAL:");
        eprintln!("start: {:?}", start);
        eprintln!("msg:   {:?}", msg);
        eprintln!("end:   {:?}", end);
        eprintln!();
    }

    Ok(())
}

impl From<bincode::Error> for WalError {
    fn from(_error: bincode::Error) -> Self {
        WalError::CorruptedFile
    }
}
