use super::types::*;
use super::*;
use crate::table::Table;
use crate::types::TypeMap;
use crate::wal::WriteAheadLog;
use async_trait::async_trait;
use futures::executor::block_on;
use std::collections::HashMap;
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
    pub fn new() -> Self {
        let (requests_in, requests_out) = mpsc::unbounded_channel();

        std::thread::spawn(move || resource_manager(requests_out));

        let state = Self {
            channel: requests_in,
            wal: WriteAheadLog::new(),
        };

        crate::snapshot::spawn_snapshotter(state.clone());

        state
    }

    pub fn wal(&mut self) -> &mut WriteAheadLog {
        &mut self.wal
    }
}

fn resource_manager(mut requests: RequestReceiver) {
    // NOTE: When locking a set of tables, make sure to lock the tables
    // in order, sorted by their name. If not, we will have deadlocks.
    let mut tables: HashMap<String, Arc<RwLock<Table>>> = HashMap::new();

    let type_map: Arc<RwLock<TypeMap>> = Arc::new(RwLock::new(TypeMap::new()));

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

                response_ch.send(Response::AcquiredResources(Resources::new(type_map, RW::Read, tables)))
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
