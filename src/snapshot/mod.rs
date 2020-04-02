mod manager;
mod write;
mod read;

pub(crate) use manager::{spawn_snapshotter};
pub(crate) use read::{read, DbData};
pub(self) use write::snapshot;

// Data-directory layout:
// - data
// | - current                (contains the current transaction number, acts as an atomic pointer to the folder)
// | - <transaction_number>   (a folder containing a snapshot of the database at the given transaction)
// | | - type_map             (a file containing all type definitions for the database)
// | | - tables               (a folder containing the raw data of all tables)
// | | | - <table_name>       (raw data of the table)
pub(self) const DATA_DIR_NAME: &str = "data";
pub(self) const CURRENT_TRANSACTION_FILE_NAME: &str = "current_tn";
pub(self) const TMP_TRANSACTION_FILE_NAME: &str = "tmp_tn";
pub(self) const TABLES_DIR_NAME: &str = "tables";
pub(self) const TYPE_MAP_FILE_NAME: &str = "type_map";