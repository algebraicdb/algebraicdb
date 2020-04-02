use crate::local::{DbState, dbms_state::DbmsState};
use crate::local::types::{Resources, Resource};
use crate::table::Table;
use std::io;
use std::path::PathBuf;
use futures::future::join_all;
use crate::wal::{TRANSACTION_NUMBER, TransactionNumber};
use std::sync::atomic::Ordering;
use crate::types::TypeMap;
use tokio::io::AsyncWriteExt;
use tokio::fs::{OpenOptions, rename, remove_file, create_dir, remove_dir_all};
use std::ops::Deref;

use super::{
    DATA_DIR_NAME,
    TABLES_DIR_NAME,
    CURRENT_TRANSACTION_FILE_NAME,
    TMP_TRANSACTION_FILE_NAME,
    TYPE_MAP_FILE_NAME,
};


/// Write the current database state to a temporary folder, and then atomically replace the active data folder
pub(super) async fn snapshot(dbms: &mut DbmsState) -> io::Result<TransactionNumber> {
    eprintln!("Snapshot starting...");

    // Acquire and lock all tables
    let mut resources: Resources<_> = dbms.acquire_all_resources().await;
    let resources = resources.take().await;

    // Ordering::Relaxed should be fine since we have also locked all tables, which means no one is writing to the WAL.
    let transaction_number = TRANSACTION_NUMBER.load(Ordering::Relaxed);
    let transaction_number_str = transaction_number.to_string();

    let transaction_folder = PathBuf::from(DATA_DIR_NAME).join(&transaction_number_str);

    // TODO: figure out if we should remove this
    //remove_dir_all(&transaction_folder).await?;

    eprintln!("Creating {:?}", &transaction_folder);
    create_dir(&transaction_folder).await?;

    eprintln!("Creating {:?}", transaction_folder.join(TABLES_DIR_NAME));
    create_dir(transaction_folder.join(TABLES_DIR_NAME)).await?;

    // Spawn tasks to flush the tables to disk
    let tasks: Vec<_> = resources.tables.into_iter().map(|(name, table)| {
        snapshot_table(&transaction_folder, name, table)
    }).collect();

    for task in join_all(tasks).await {
        task?
    }

    snapshot_type_map(&transaction_folder, resources.type_map).await?;

    let tmp_transaction_file_path = PathBuf::from(DATA_DIR_NAME).join(TMP_TRANSACTION_FILE_NAME);
    let cur_transaction_file_path = PathBuf::from(DATA_DIR_NAME).join(CURRENT_TRANSACTION_FILE_NAME);

    flush_to_file(&tmp_transaction_file_path, transaction_number_str.as_bytes(), false).await?;

    eprintln!("Renaming {:?} to {:?}.", tmp_transaction_file_path, cur_transaction_file_path);
    rename(
        &tmp_transaction_file_path,
        &cur_transaction_file_path,
    ).await?;

    eprintln!("Snapshot complete.");

    Ok(transaction_number)
}

async fn snapshot_type_map(folder: &PathBuf, type_map: Resource<'_, TypeMap>) -> io::Result<()> {
    eprintln!("Snapshotting typemap");
    let data = bincode::serialize(type_map.deref()).unwrap();
    let file_path = folder.join(TYPE_MAP_FILE_NAME);
    flush_to_file(&file_path, &data, true).await
}

async fn snapshot_table(folder: &PathBuf, name: &str, table: Resource<'_, Table>) -> io::Result<()> {
    eprintln!("Snapshotting table {}", name);
    let data = bincode::serialize(table.deref()).unwrap();
    let file_path = folder
        .join(TABLES_DIR_NAME)
        .join(name);
    flush_to_file(&file_path, &data, true).await
}

/// Write the given data to a file.
///
/// Makes sure the data is synced to disk.
/// if new_only, then the file must not already exist.
async fn flush_to_file(path: &PathBuf, data: &[u8], new_only: bool) -> io::Result<()> {
    eprintln!("Writing file {:?}", path);
    let mut file = if new_only {
        OpenOptions::new().write(true).create_new(true).open(path).await?
    } else {
        OpenOptions::new().write(true).create(true).open(path).await?
    };

    file.write_all(data).await?;
    file.sync_all().await
}
