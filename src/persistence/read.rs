use crate::persistence::TransactionNumber;
use crate::table::Table;
use crate::types::TypeMap;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, read_dir, read_to_string};
use tokio::stream::StreamExt;
use tokio::sync::RwLock;

use super::{CURRENT_TRANSACTION_FILE_NAME, TABLES_DIR_NAME, TYPE_MAP_FILE_NAME};

/// All state data associated with the database
pub struct DbData {
    /// The transaction_number associated with the state
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

pub async fn load_db_data(data_dir: &PathBuf) -> io::Result<DbData> {
    let transaction_number = get_current_transaction_number(data_dir).await?;
    let snapshot_dir = data_dir.join(transaction_number.to_string());
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

pub async fn get_current_transaction_number(data_dir: &PathBuf) -> io::Result<TransactionNumber> {
    let cur_transaction_file_path = data_dir.join(CURRENT_TRANSACTION_FILE_NAME);
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
