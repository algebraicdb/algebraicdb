use std::io;
use tokio::io::AsyncReadExt;
use tokio::stream::StreamExt;
use tokio::fs::{self, File, OpenOptions, read_to_string, create_dir, read_dir};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::table::Table;
use crate::types::TypeMap;
use std::path::PathBuf;
use crate::wal::TransactionNumber;

use super::{
    DATA_DIR_NAME,
    TABLES_DIR_NAME,
    CURRENT_TRANSACTION_FILE_NAME,
    TYPE_MAP_FILE_NAME,
};


pub struct DbData {
    pub transaction_number: TransactionNumber,
    pub tables: HashMap<String, Arc<RwLock<Table>>>,
    pub type_map: Arc<RwLock<TypeMap>>,
}

impl Default for DbData {
    fn default() -> Self {
        Self {
            transaction_number: 0,
            tables: HashMap::new(),
            type_map: Arc::new(RwLock::new(TypeMap::new())),
        }
    }
}

pub async fn read() -> io::Result<DbData> {
    let transaction_number = get_current_transaction_number().await?;
    let snapshot_dir = current_snapshot_dir(transaction_number);
    let tables_dir_path = snapshot_dir.join(TABLES_DIR_NAME);
    let mut tables_dir = read_dir(tables_dir_path).await.unwrap();

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

pub async fn get_current_transaction_number() -> io::Result<TransactionNumber> {
    let cur_transaction_file_path = PathBuf::from(DATA_DIR_NAME).join(CURRENT_TRANSACTION_FILE_NAME);
    Ok(read_to_string(cur_transaction_file_path).await?.parse().expect("Parsing transaction number file failed"))
}

pub fn current_snapshot_dir(tn: TransactionNumber) -> PathBuf {
    PathBuf::from(DATA_DIR_NAME).join(tn.to_string()) 
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