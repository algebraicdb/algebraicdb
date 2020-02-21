use super::types::*;
use crate::table::Table;
use crate::types::TypeMap;
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

type RequestSender = mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>;
type RequestReceiver = mpsc::UnboundedReceiver<(Request, oneshot::Sender<Response>)>;

lazy_static! {
    pub(super) static ref REQUEST_SENDER: RequestSender = {
        let (requests_in, requests_out) = mpsc::unbounded_channel();

            tokio::spawn(async move {
                match resource_manager(requests_out).await {
                    Err(msg) => panic!("Resource manager crashed: {}", msg),
                    Ok(_) => unreachable!(),
                }
            });

        requests_in
    };
}

async fn resource_manager(mut requests: RequestReceiver) -> Result<!, String> {
    let mut tables: HashMap<String, Arc<RwLock<Table>>> = HashMap::new();
    let type_map: Arc<RwLock<TypeMap>> = Arc::new(RwLock::new(TypeMap::new()));

    loop {
        let (request, response_ch) = requests.recv().await.ok_or_else(|| String::from("Channel closed."))?;

        match request {
            Request::AcquireResources {
                table_reqs,
                type_map_perms,
            } => {
                let type_map = type_map.clone();
                let resources: Result<Vec<_>, _> = table_reqs
                    .into_iter()
                    .map(|req| {
                        if let Some(lock) = tables.get(&req.table) {
                            // cloning an Arc is relatively cheap
                            Ok((req.rw, req.table, lock.clone()))
                        } else {
                            Err(Response::NoSuchTable(req.table))
                        }
                    })
                    .collect();

                match resources {
                    Ok(tables) => response_ch.send(Response::AcquiredResources(Resources::new(
                        type_map,
                        type_map_perms,
                        tables,
                    ))),
                    Err(response) => response_ch.send(response),
                }.unwrap_or_else(|_| eprintln!("global::manager: response channel closed."));
            }
            Request::CreateTable(name, table) => {
                if tables.contains_key(&name) {
                    response_ch
                        .send(Response::TableAlreadyExists)
                        .unwrap_or_else(|_| eprintln!("global::manager: response channel closed."));
                } else {
                    tables.insert(name, Arc::new(RwLock::new(table)));
                    response_ch
                        .send(Response::TableCreated)
                        .unwrap_or_else(|_| eprintln!("global::manager: response channel closed."));
                }
            }
        }
    }
}
