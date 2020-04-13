use crate::persistence::TransactionNumber;
use crate::state::DbData;
use crate::table::Table;
use crate::types::TypeMap;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, read_dir, read_to_string, remove_dir_all, remove_file};
use tokio::stream::StreamExt;
use tokio::sync::RwLock;

use super::{DATA_DIR_FILES, TABLES_DIR_NAME, TMP_EXTENSION, TNUM_FILE_NAME, TYPE_MAP_FILE_NAME};

/// Load DbData from an initialized data directory
///
/// Also checks that the directory is a valid data directory,
/// that is, it doesn't contain any foreign files.
///
/// It will also clean up any .tmp-files left behind after a database crash
pub async fn load_db_data(data_dir: &PathBuf) -> io::Result<DbData> {
    let transaction_number = get_current_transaction_number(data_dir).await?;

    // Validate and clean the data directory
    let mut data_dir_entries = read_dir(data_dir).await?;
    let mut contains_foreign_files = false;
    while let Some(entry) = data_dir_entries.next().await {
        let entry = entry?;

        // clean up .tmp-files
        if let Some(ext) = entry.path().extension() {
            if ext == TMP_EXTENSION {
                info!("cleaning up temporary file: {:?}", entry.file_name());
                remove_file(entry.path()).await?;
                continue;
            }
        }

        // Check if the files in data_dir are forign
        if DATA_DIR_FILES.iter().any(|&f| f == entry.file_name()) {
            continue; // file is known
        }
        if let Some(file_name) = entry.file_name().to_str() {
            if let Ok(snapshot_tnum) = file_name.parse::<TransactionNumber>() {
                // folder is a data snapshot

                if snapshot_tnum != transaction_number {
                    info!("cleaning up unused snapshot: {}", snapshot_tnum);
                    remove_dir_all(entry.path()).await?;
                }

                continue;
            }
        }

        // if the purpose of the file is unknown, error
        if !contains_foreign_files {
            contains_foreign_files = true;
            error!("the data directory contains foreign files");
        }
        error!("foreign file: {:?}", entry.file_name());
    }

    if contains_foreign_files {
        panic!("data directory contains foreign files");
    }

    if transaction_number == 0 {
        // No data has been written to disk yet
        return Ok(DbData::default());
    }

    let snapshot_dir = data_dir.join(transaction_number.to_string());
    let tables_dir_path = snapshot_dir.join(TABLES_DIR_NAME);
    let mut tables_dir = read_dir(tables_dir_path).await?;

    let mut tables = HashMap::new();
    // TODO: Implement concurrency here. Read up on tokio::stream
    while let Some(entry) = tables_dir.next().await {
        let entry = entry?;
        let name = entry.file_name().into_string().unwrap();
        let table = read_table(entry.path()).await?;
        tables.insert(name, Arc::new(RwLock::new(table)));
    }

    let type_map = read_type_map(&snapshot_dir).await?;

    Ok(DbData {
        transaction_number,
        tables,
        type_map: Arc::new(RwLock::new(type_map)),
    })
}

pub async fn get_current_transaction_number(data_dir: &PathBuf) -> io::Result<TransactionNumber> {
    let cur_transaction_file_path = data_dir.join(TNUM_FILE_NAME);
    Ok(read_to_string(cur_transaction_file_path)
        .await?
        .parse()
        .expect("Parsing transaction number file failed"))
}

pub async fn read_table(path: PathBuf) -> io::Result<Table> {
    let binary = fs::read(path).await?;
    let decoded: Table = bincode::deserialize(&binary).unwrap();
    Ok(decoded)
}

pub async fn read_type_map(snapshot_dir: &PathBuf) -> io::Result<TypeMap> {
    let binary = fs::read(snapshot_dir.join(TYPE_MAP_FILE_NAME)).await?;
    Ok(bincode::deserialize(&binary).unwrap())
}
