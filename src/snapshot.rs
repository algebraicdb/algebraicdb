use tokio::{time::{delay_for, Duration}};
use crate::local::{DbState, dbms_state::DbmsState};
use crate::local::types::{Resources, Resource};
use crate::table::Table;
use std::io;
use std::path::PathBuf;
use futures::future::join_all;
use crate::wal::{TRANSACTION_NUMBER, TransactionNumber};
use std::sync::atomic::Ordering;
use crate::types::TypeMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::fs::{File, OpenOptions, create_dir, remove_dir_all};
use std::ops::Deref;

// Data-directory layout:
// - data
// | - transaction_number      (contains the wal transaction number of the written state)
// | - type_map
// | - tables
// | | - <table_name>    (raw data of the table)
const DATA_DIR_NAME: &str = "data";
const TMP_DATA_DIR_NAME: &str = "data_tmp";
const TABLES_DIR_NAME: &str = "tables";
const TRANSACTION_NUMBER_FILE_NAME: &str = "transaction_number";
const TYPE_MAP_FILE_NAME: &str = "type_map";

pub fn spawn_snapshotter(dbms: DbmsState) {

    let transactionnumber =
        std::fs::read_to_string("./data_tmp/transaction_number").unwrap_or("0".into()).as_str().parse::<u64>().unwrap_or(0);

    tokio::task::spawn(async move {
        manager(dbms, transactionnumber).await
    });
}

async fn manager(mut dbms: DbmsState, startup_id: TransactionNumber) {
    let mut last_snapshotted: TransactionNumber = startup_id;
    loop {
        delay_for(Duration::new(30, 0)).await;

        // check tip of WAL, and tip of current snapshot
        let current = TRANSACTION_NUMBER.load(Ordering::Relaxed);
        eprintln!("Checking snapshot {} vs current: {}", last_snapshotted, current);
        if current != last_snapshotted {
            last_snapshotted = snapshot(&mut dbms).await.unwrap_or_else(|err| {
                eprintln!("Failed to write snapshot: {:?}\n", err);
                last_snapshotted
            });
        }
    }
}

/// Write the current database state to a temporary folder, and then atomically replace the active data folder
async fn snapshot(dbms: &mut DbmsState) -> io::Result<TransactionNumber> {
    eprintln!("Snapshotting...");
    let snapshot_folder = PathBuf::from(TMP_DATA_DIR_NAME);

    // Acquire and lock all tables
    let mut resources: Resources<_> = dbms.acquire_all_resources().await;
    let resources = resources.take().await;

    // Ordering::Relaxed should be fine since we have also locked all tables, which means no one is writing to the WAL.
    let transaction_number = TRANSACTION_NUMBER.load(Ordering::Relaxed);

    // TODO: Temporary fix, remove this when we propely implement the data directory
    remove_dir_all(PathBuf::from(TMP_DATA_DIR_NAME)).await?;

    create_dir(PathBuf::from(TMP_DATA_DIR_NAME)).await?;
    create_dir(PathBuf::from(TMP_DATA_DIR_NAME).join(TABLES_DIR_NAME)).await?;

    // Spawn tasks to flush the tables to disk
    let tasks: Vec<_> = resources.tables.into_iter().map(|(name, table)| {
        snapshot_table(&snapshot_folder, name, table)
    }).collect();

    join_all(tasks).await;

    snapshot_type_map(&snapshot_folder, resources.type_map).await?;
    snapshot_transaction_number(&snapshot_folder, transaction_number).await?;

    Ok(transaction_number)
}

async fn snapshot_transaction_number(folder: &PathBuf, transaction_number: u64) -> io::Result<()> {
    let data = transaction_number.to_string();
    let file_path = folder.join(TRANSACTION_NUMBER_FILE_NAME);
    write_to_new_file(&file_path, data.as_bytes()).await
}

async fn snapshot_type_map(folder: &PathBuf, type_map: Resource<'_, TypeMap>) -> io::Result<()> {
    let data = bincode::serialize(type_map.deref()).unwrap();
    let file_path = folder.join(TYPE_MAP_FILE_NAME);
    write_to_new_file(&file_path, &data).await
}

async fn snapshot_table(folder: &PathBuf, name: &str, table: Resource<'_, Table>) -> io::Result<()> {
    let data = bincode::serialize(table.deref()).unwrap();
    let file_path = folder
        .join(TABLES_DIR_NAME)
        .join(name);
    write_to_new_file(&file_path, &data).await
}

/// Write the given data to a new file.
///
/// The file must not already exist.
async fn write_to_new_file(path: &PathBuf, data: &[u8]) -> io::Result<()> {
    let mut file: File = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;

    file.write_all(data).await
}
