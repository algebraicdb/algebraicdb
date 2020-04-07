use super::types::*;
use super::*;
use crate::api::config::DbmsConfig;
use crate::executor::execute_replay_query;
use crate::persistence::TransactionNumber;
use crate::persistence::{load_db_data, spawn_snapshotter, WriteAheadLog};
use crate::table::Table;
use crate::types::TypeMap;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
pub struct DbmsState {
    state: Arc<Mutex<DbData>>,
    wal: Option<WriteAheadLog>,
}

/// All state data associated with the database
pub struct DbData {
    /// The transaction_number associated with the initial state
    pub transaction_number: TransactionNumber,

    /// All tables in the database
    ///
    /// NOTE: When locking a set of tables, make sure to lock the tables
    /// in order, sorted by their name. If not, we will have deadlocks.
    pub tables: HashMap<String, Arc<RwLock<Table>>>,

    /// A map of all types in the db
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

#[async_trait]
impl DbState<Table> for DbmsState {
    async fn acquire_resources(&self, acquire: Acquire) -> Result<Resources<Table>, String> {
        let state = self.state.lock().await;
        let type_map = state.type_map.clone();
        let resources: Result<Vec<_>, _> = acquire
            .table_reqs
            .into_iter()
            .map(|req| {
                if let Some(lock) = state.tables.get(&req.table) {
                    // cloning an Arc is relatively cheap
                    Ok((req.rw, req.table, lock.clone()))
                } else {
                    Err(req.table)
                }
            })
            .collect();

        match resources {
            Ok(tables) => Ok(Resources::new(type_map, acquire.type_map_perms, tables)),
            Err(err) => Err(err.to_string()),
        }
    }

    async fn acquire_all_resources(&self) -> Resources<Table> {
        let state = self.state.lock().await;
        let type_map = state.type_map.clone();

        // TODO: avoid string cloning
        let mut tables: Vec<(RW, String, _)> = state
            .tables
            .iter()
            .map(|(name, table_lock)| (RW::Read, name.clone(), table_lock.clone()))
            .collect();

        tables.sort_by(|(_, name_a, _), (_, name_b, _)| name_a.cmp(name_b));

        Resources::new(type_map, RW::Read, tables)
    }

    async fn create_table(&self, name: String, table: Table) -> Result<(), ()> {
        let mut state = self.state.lock().await;
        if state.tables.contains_key(&name) {
            Err(())
        } else {
            state
                .tables
                .insert(name.to_string(), Arc::new(RwLock::new(table)));
            Ok(())
        }
    }

    async fn drop_table(&self, name: &str) -> Result<(), ()> {
        let mut state = self.state.lock().await;
        state.tables.remove(name).map(|_| ()).ok_or(())
    }
}

impl DbmsState {
    pub async fn new(config: DbmsConfig) -> Self {
        if config.no_persistence {
            Self {
                state: Default::default(),
                wal: None,
            }
        } else {
            let (wal, wal_entries) =
                WriteAheadLog::new(config.data_dir.clone(), config.wal_truncate_at).await;

            let db_data = match load_db_data(&config.data_dir).await {
                Ok(state) => state,
                Err(e) => {
                    eprintln!(
                        "Error reading data from disk, falling back to default. {}",
                        e
                    );
                    DbData::default()
                }
            };

            let transaction_number = db_data.transaction_number;

            let mut state = Self {
                state: Arc::new(Mutex::new(db_data)),
                wal: Some(wal),
            };

            // TODO: This can probably be optimized
            for (entry_tn, query_data) in wal_entries {
                if entry_tn > transaction_number {
                    if let Some(query_data) = query_data {
                        eprintln!("Replaying transaction {}", entry_tn);
                        let query = bincode::deserialize(&query_data).unwrap();
                        execute_replay_query(query, &mut state, &mut Vec::<u8>::new())
                            .await
                            .unwrap();
                    }
                }
            }

            spawn_snapshotter(
                state.clone(),
                config.data_dir,
                config.disk_flush_timing,
                transaction_number,
            );

            state
        }
    }

    pub fn wal(&mut self) -> Option<&mut WriteAheadLog> {
        self.wal.as_mut()
    }
}
