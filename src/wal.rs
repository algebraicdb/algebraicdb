use bincode;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use std::io;
use crate::ast::Stmt;
use serde::{Serialize, Deserialize};

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

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct EntryBegin {
    entry_size: usize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
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

pub async fn wal_writer(mut channel: RequestReceiver) -> io::Result<()> {
    let mut file: File = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open("wal")
        .await?;

    let size_of_entry_begin = bincode::serialized_size(&EntryBegin { entry_size: 0 }).unwrap() as usize;
    let size_of_entry_end = bincode::serialized_size(&EntryEnd { checksum: 0 }).unwrap() as usize;

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
            data = &data[size_of_entry_begin..];

            if start.entry_size + size_of_entry_end > data.len() {
                panic!("Corrupt WAL");
            }

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

            // TODO: validate checksum

            eprintln!("Read the following from the WAL:");
            eprintln!("start: {:?}", start);
            eprintln!("msg:   {:?}", msg);
            eprintln!("end:   {:?}", end);
            eprintln!();
        }
    }

    while let Some((msg, out)) = channel.recv().await {
        let start = EntryBegin {
            entry_size: msg.len(),
        };

        let end = EntryEnd {
            checksum: 0, // TODO
        };

        let mut buf = Vec::with_capacity(msg.len() + 16);

        bincode::serialize_into(&mut buf, &start).unwrap();
        buf.extend_from_slice(&msg);
        bincode::serialize_into(&mut buf, &end).unwrap();

        // Write entry to the wal-file, and make sure it's synced to disk.
        file.write_all(&buf).await?;
        file.sync_all().await?;

        out.send(())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Resonse channel closed"))?;

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
