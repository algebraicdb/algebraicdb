use super::types::*;
use super::*;
use crate::executor::execute_replay_query;
use crate::persistence::{self, spawn_snapshotter, DbData, WriteAheadLog};
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
    wal: WriteAheadLog,
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
    pub async fn new() -> Self {
        let (wal, wal_entries) = WriteAheadLog::new().await;
        //let transaction_number: u64 = wal_entries.last().map(|(n, _)| *n).unwrap_or(0);

        let read_state = match persistence::read().await {
            Ok(state) => state,
            Err(e) => {
                eprintln!(
                    "Error reading data from disk, falling back to default. {}",
                    e
                );
                DbData::default()
            }
        };
        let transaction_number = read_state.transaction_number;

        let (requests_in, requests_out) = mpsc::unbounded_channel();
        std::thread::spawn(move || resource_manager(requests_out, read_state));

        let mut state = Self {
            channel: requests_in,
            wal,
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

        spawn_snapshotter(state.clone(), transaction_number);

        state
    }

    pub fn wal(&mut self) -> &mut WriteAheadLog {
        &mut self.wal
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
