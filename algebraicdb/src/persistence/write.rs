use crate::state::types::{Resource, Resources};
use crate::state::DbmsState;
use crate::table::{Schema, TableData};
use crate::types::TypeMap;
use futures::future::join_all;
use std::io;
use std::ops::Deref;
use std::path::PathBuf;
use tokio::fs::{create_dir, read_dir, remove_dir_all, rename, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::stream::StreamExt;

use super::{
    TransactionNumber, TABLES_DIR_NAME, TMP_EXTENSION, TNUM_FILE_NAME, TYPE_MAP_FILE_NAME,
};

pub async fn initialize_data_dir(data_dir: &PathBuf) -> io::Result<()> {
    info!("initializing data directory {:?}", data_dir);

    let mut entries = read_dir(data_dir).await?;

    let mut existing_files_present: bool = false;
    while let Some(entry) = entries.next().await {
        let entry = entry?;
        if !existing_files_present {
            existing_files_present = true;
            error!("the data directory already contains files:")
        }
        error!("  {:?}", entry.file_name());
    }

    if existing_files_present {
        panic!("data directory not empty");
    }

    write_tnum(data_dir, 0).await
}

/// Write the current database state to a temporary folder, and then atomically replace the active data folder
pub(super) async fn snapshot(
    data_dir: &PathBuf,
    last_snapshotted: TransactionNumber,
    dbms: &mut DbmsState,
) -> io::Result<TransactionNumber> {
    info!("data snapshot starting...");

    // Acquire and lock all tables
    let resources: Resources = dbms.acquire_all_resources().await;

    let type_map = resources.take_type_map().await;
    let table_schemas = resources.take_schemas().await;
    let table_datas = resources.take_data().await;

    // Ordering::Relaxed should be fine since we have also locked all tables, which means no one is writing to the WAL.
    let transaction_number = dbms.wal().unwrap().transaction_number();

    let transaction_folder = data_dir.join(&transaction_number.to_string());

    // TODO: figure out if we should remove this
    //remove_dir_all(&transaction_folder).await?;

    debug!("creating {:?}", &transaction_folder);
    create_dir(&transaction_folder).await?;

    debug!("creating {:?}", transaction_folder.join(TABLES_DIR_NAME));
    create_dir(transaction_folder.join(TABLES_DIR_NAME)).await?;

    // Spawn tasks to flush the tables to disk
    let tasks: Vec<_> = table_schemas
        .iter()
        .zip(table_datas.iter())
        .map(|((name, schema), (_, data))| snapshot_table(&transaction_folder, name, &schema, &data))
        .collect();

    // Await all table flush tasks concurrently.
    // The table lock for a task is dropped when it completes.
    for task in join_all(tasks).await {
        task?
    }

    snapshot_type_map(&transaction_folder, type_map).await?;

    write_tnum(data_dir, transaction_number).await?;

    info!("data snapshot complete");

    if last_snapshotted != 0 {
        info!("removing previous snapshot: {}", last_snapshotted);
        let prev_transaction_folder = data_dir.join(&last_snapshotted.to_string());
        remove_dir_all(&prev_transaction_folder).await?;
    }

    Ok(transaction_number)
}

async fn write_tnum(data_dir: &PathBuf, tnum: TransactionNumber) -> io::Result<()> {
    debug!("writing transaction number [{}] to disk", tnum);

    let cur_tnum_path = data_dir.join(TNUM_FILE_NAME);
    let mut tmp_tnum_path = cur_tnum_path.clone();
    tmp_tnum_path.set_extension(TMP_EXTENSION);

    flush_to_file(&tmp_tnum_path, tnum.to_string().as_bytes(), false).await?;

    // Rename the file, and make sure the rename gets synced to disk
    rename(&tmp_tnum_path, &cur_tnum_path).await?;
    File::open(&cur_tnum_path).await?.sync_all().await?;

    Ok(())
}

async fn snapshot_type_map(folder: &PathBuf, type_map: Resource<'_, TypeMap>) -> io::Result<()> {
    debug!("snapshotting typemap");
    let data = bincode::serialize(type_map.deref()).unwrap();
    let file_path = folder.join(TYPE_MAP_FILE_NAME);
    flush_to_file(&file_path, &data, true).await
}

async fn snapshot_table(
    folder: &PathBuf,
    name: &str,
    schema: &Schema,
    data: &TableData,
) -> io::Result<()> {
    debug!("snapshotting table \"{}\"", name);
    let data = bincode::serialize(&(schema, data)).unwrap();
    let file_path = folder.join(TABLES_DIR_NAME).join(name);
    flush_to_file(&file_path, &data, true).await
}

/// Write the given data to a file.
///
/// Makes sure the data is synced to disk.
/// if new_only, then the file must not already exist.
async fn flush_to_file(path: &PathBuf, data: &[u8], new_only: bool) -> io::Result<()> {
    debug!("writing file {:?}", path);
    let mut file = if new_only {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await?
    } else {
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .await?
    };

    file.write_all(data).await?;
    file.sync_all().await
}
