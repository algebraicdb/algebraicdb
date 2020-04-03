use super::types::*;
use super::*;
use crate::api::config::DbmsConfig;
use crate::executor::execute_replay_query;
use crate::persistence::{load_db_data, spawn_snapshotter, DbData, WriteAheadLog};
use crate::table::Table;
use async_trait::async_trait;
use futures::executor::block_on;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

type RequestSender = mpsc::UnboundedSender<(Request<Table>, oneshot::Sender<Response<Table>>)>;
type RequestReceiver = mpsc::UnboundedReceiver<(Request<Table>, oneshot::Sender<Response<Table>>)>;

#[derive(Clone)]
pub struct DbmsState {
    channel: RequestSender,
    wal: Option<WriteAheadLog>,
}

impl DbmsState {
    async fn send_request(&self, request: Request<Table>) -> Response<Table> {
        let (response_in, response_out) = oneshot::channel();
        self.channel
            .send((request, response_in))
            .unwrap_or_else(|_| panic!("Global resources request channel closed"));
        response_out
            .await
            .expect("Global resources request channel closed")
    }
}

#[async_trait]
impl DbState<Table> for DbmsState {
    async fn acquire_resources(&self, acquire: Acquire) -> Result<Resources<Table>, String> {
        match self.send_request(Request::Acquire(acquire)).await {
            Response::AcquiredResources(resources) => Ok(resources),
            Response::NoSuchTable(name) => Err(name),
            _ => unreachable!(),
        }
    }

    async fn acquire_all_resources(&self) -> Resources<Table> {
        match self.send_request(Request::AcquireAll()).await {
            Response::AcquiredResources(resources) => resources,
            _ => unreachable!(),
        }
    }

    async fn create_table(&self, name: String, table: Table) -> Result<(), ()> {
        match self.send_request(Request::CreateTable(name, table)).await {
            Response::CreateTable(resp) => resp,
            _ => unreachable!(),
        }
    }
}

impl DbmsState {
    pub async fn new(config: DbmsConfig) -> Self {
        let (requests_in, requests_out) = mpsc::unbounded_channel();

        if config.no_data_dir {
            std::thread::spawn(move || resource_manager(requests_out, DbData::default()));
            Self {
                channel: requests_in,
                wal: None,
            }
        } else {
            let (wal, wal_entries) = WriteAheadLog::new(&config.data_dir).await;

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

            std::thread::spawn(move || resource_manager(requests_out, db_data));

            let mut state = Self {
                channel: requests_in,
                wal: Some(wal),
            };

            // TODO: This can probably be optimized
            for (entry_tn, entry) in wal_entries {
                if entry_tn > transaction_number {
                    eprintln!("Replaying transaction {}", entry_tn);
                    execute_replay_query(entry, &mut state, &mut Vec::<u8>::new())
                        .await
                        .unwrap();
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

fn resource_manager(mut requests: RequestReceiver, data: DbData) {
    // NOTE: When locking a set of tables, make sure to lock the tables
    // in order, sorted by their name. If not, we will have deadlocks.

    let mut tables = data.tables;
    let type_map = data.type_map;
    //let mut tables: HashMap<String, Arc<RwLock<Table>>> = HashMap::new();
    //let type_map: Arc<RwLock<TypeMap>> = Arc::new(RwLock::new(TypeMap::new()));

    loop {
        let (request, response_ch) = match block_on(requests.recv()) {
            Some(r) => r,
            None => return, // channel closed, exit manager.
        };

        match request {
            Request::AcquireAll() => {
                let type_map = type_map.clone();

                // TODO: avoid string cloning
                let mut tables: Vec<(RW, String, _)> = tables
                    .iter()
                    .map(|(name, table_lock)| (RW::Read, name.clone(), table_lock.clone()))
                    .collect();

                tables.sort_by(|(_, name_a, _), (_, name_b, _)| name_a.cmp(name_b));

                response_ch
                    .send(Response::AcquiredResources(Resources::new(
                        type_map,
                        RW::Read,
                        tables,
                    )))
                    .unwrap_or_else(|_| eprintln!("global::manager: response channel closed."));
            }
            Request::Acquire(Acquire {
                table_reqs,
                type_map_perms,
            }) => {
                let type_map = type_map.clone();
                let resources: Result<Vec<_>, _> = table_reqs
                    .into_iter()
                    .map(|req| {
                        if let Some(lock) = tables.get(&req.table) {
                            // cloning an Arc is relatively cheap
                            Ok((req.rw, req.table, lock.clone()))
                        } else {
                            Err(Response::NoSuchTable(req.table.clone()))
                        }
                    })
                    .collect();

                match resources {
                    Ok(tables) => response_ch.send(Response::AcquiredResources(Resources::new(
                        type_map,
                        type_map_perms,
                        tables,
                    ))),
                    Err(err) => response_ch.send(err),
                }
                .unwrap_or_else(|_| eprintln!("global::manager: response channel closed."));
            }
            Request::CreateTable(name, table) => {
                if tables.contains_key(&name) {
                    response_ch
                        .send(Response::CreateTable(Err(())))
                        .unwrap_or_else(|_| eprintln!("global::manager: response channel closed."));
                } else {
                    tables.insert(name, Arc::new(RwLock::new(table)));
                    response_ch
                        .send(Response::CreateTable(Ok(())))
                        .unwrap_or_else(|_| eprintln!("global::manager: response channel closed."));
                }
            }
        }
    }
}
