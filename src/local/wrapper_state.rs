use super::types::*;
use super::*;
use crate::types::TypeMap;
use async_trait::async_trait;
use futures::executor::block_on;
use std::collections::HashMap;
use std::sync::Arc;
use std::error::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio_postgres::{Config, Client, NoTls};
use tokio_postgres;
use crate::table::Schema;


type RequestSender = mpsc::UnboundedSender<(Request<Schema>, oneshot::Sender<Response<Schema>>)>;
type RequestReceiver = mpsc::UnboundedReceiver<(Request<Schema>, oneshot::Sender<Response<Schema>>)>;

#[derive(Clone)]
pub struct WrapperState {
    channel: RequestSender,
}

impl WrapperState {
    async fn send_request(&self, request: Request<Schema>) -> Response<Schema> {
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
impl DbState<Schema> for WrapperState {
    // FIXME: This is where shit breaks, it never get the resource for some reason.
    async fn acquire_resources(&self, acquire: Acquire) -> Result<Resources<Schema>, String> {
        match self.send_request(Request::Acquire(acquire)).await {
            Response::AcquiredResources(resources) =>  Ok(resources),
            Response::NoSuchTable(name) => Err(name),
            _ => unreachable!(),
        }
    }

    async fn create_table(&self, name: String, table: Schema) -> Result<(), ()> {
        match self.send_request(Request::CreateTable(name, table)).await {
            Response::CreateTable(resp) => resp,
            _ => unreachable!(),
        }
    }
}

impl WrapperState {
    pub async fn new() -> Result<Self, Box<dyn Error>> {
        let (requests_in, requests_out) = mpsc::unbounded_channel();
        tokio::spawn(resource_manager(requests_out));
        Ok(Self {
            channel: requests_in,
        })
    }
}

async fn resource_manager(mut requests: RequestReceiver) {
    use crate::psqlwrapper::translator;
    let mut tables: HashMap<String, Arc<RwLock<Schema>>> = HashMap::new();
    let type_map: Arc<RwLock<TypeMap>> = Arc::new(RwLock::new(TypeMap::new()));
    let mut config = Config::new();

    config
        .user("postgres")
        .password("example")
        .host("localhost")
        .port(5432);
    let (client, _) = block_on(config.connect(NoTls)).unwrap();

    loop {
        let (request, response_ch) = match block_on(requests.recv()) {
            Some(r) => r,
            None => return, // channel closed, exit manager.
        };

        match request {
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
                    println!("got here");
                    let guard = type_map.read().await;
                    &client.query(translator::translate_create_table(&name, &table, &guard).as_str(), &[]).await.unwrap();
                    tables.insert(name, Arc::new(RwLock::new(table)));
                    response_ch
                        .send(Response::CreateTable(Ok(())))
                        .unwrap_or_else(|_| eprintln!("global::manager: response channel closed."));
                }
            }
        }
    }
}
