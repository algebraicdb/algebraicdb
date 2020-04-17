use super::types::*;
use super::*;
use crate::api::config::DbmsConfig;
use crate::executor::execute_replay_query;
use crate::persistence::TransactionNumber;
use crate::persistence::{initialize_data_dir, load_db_data, spawn_snapshotter, WriteAheadLog};
use crate::table::TableData;
use crate::types::TypeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DbmsState {
    state: Arc<RwLock<DbData>>,
    wal: Option<WriteAheadLog>,
}

/// All state data associated with the database
pub struct DbData {
    /// The transaction_number associated with the initial state
    pub transaction_number: TransactionNumber,

    /// All tables in the database
    ///
    /// NOTE: When locking the schema and data of a set of tables,
    /// make sure to lock all the schemas FIRST, sorted on the name,
    /// and THEN followed by the data, sorted on the name.
    /// If not, we will have deadlocks.
    pub tables: HashMap<String, (Arc<RwLock<Schema>>, Arc<RwLock<TableData>>)>,

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

impl DbmsState {
    pub async fn acquire_resources(&self, acquire: Acquire) -> Result<Resources, String> {
        let state = self.state.read().await;
        let type_map = state.type_map.clone();
        let table_schemas = acquire
            .schema_reqs
            .into_iter()
            .map(|req| {
                if let Some((schema_lock, _)) = state.tables.get(&req.table) {
                    Ok((req.table, PermLock::new(req.rw, Arc::clone(schema_lock))))
                } else {
                    Err(req.table)
                }
            })
            .collect::<Result<_, _>>()?;
        let table_datas = acquire
            .data_reqs
            .into_iter()
            .map(|req| {
                if let Some((_, data_lock)) = state.tables.get(&req.table) {
                    Ok((req.table, PermLock::new(req.rw, Arc::clone(data_lock))))
                } else {
                    Err(req.table)
                }
            })
            .collect::<Result<_, _>>()?;

        Ok(Resources::new(PermLock::new(acquire.type_map_perms, type_map), table_schemas, table_datas))
    }

    pub async fn acquire_all_resources(&self) -> Resources {
        let state = self.state.read().await;
        let type_map = state.type_map.clone();

        let mut table_schemas = Vec::with_capacity(state.tables.len());
        let mut table_datas = Vec::with_capacity(state.tables.len());

        // TODO: avoid string cloning
        for (name, (schema, data)) in state.tables.iter() {
            table_schemas.push((name.clone(), PermLock::new(RW::Read, schema.clone())));
            table_datas.push((name.clone(), PermLock::new(RW::Read, data.clone())));
        }

        // Release lock
        drop(state);

        table_schemas.sort_by(|(name_a, _), (name_b, _)| name_a.cmp(name_b));
        table_datas.sort_by(|(name_a, _), (name_b, _)| name_a.cmp(name_b));

        Resources::new(PermLock::new(RW::Read, type_map), table_schemas, table_datas)
    }

    pub async fn create_table(&self, name: String, schema: Schema, data: TableData) -> Result<(), ()> {
        let mut state = self.state.write().await;
        if state.tables.contains_key(&name) {
            Err(())
        } else {
            let schema = Arc::new(RwLock::new(schema));
            let data = Arc::new(RwLock::new(data));
            state
                .tables
                .insert(name.to_string(), (schema, data));
            Ok(())
        }
    }

    pub async fn drop_table(&self, name: &str) -> Result<(), ()> {
        let mut state = self.state.write().await;
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
            let db_data = match load_db_data(&config.data_dir).await {
                Ok(state) => state,
                Err(e) => {
                    info!(
                        "failed to read stored data from disk, is this a fresh instance? {}",
                        e
                    );
                    initialize_data_dir(&config.data_dir)
                        .await
                        .expect("Failed to initialize data directory");
                    DbData::default()
                }
            };

            let (wal, wal_entries) =
                WriteAheadLog::new(config.data_dir.clone(), config.wal_truncate_at).await;

            let transaction_number = db_data.transaction_number;

            let mut state = Self {
                state: Arc::new(RwLock::new(db_data)),
                wal: Some(wal),
            };

            // TODO: This can probably be optimized
            for (entry_tn, query_data) in wal_entries {
                if entry_tn > transaction_number {
                    if let Some(query_data) = query_data {
                        debug!("replaying transaction {}", entry_tn);
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
