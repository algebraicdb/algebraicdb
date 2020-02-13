use super::types::*;
use crate::table::Table;
use crate::types::TypeMap;
use crossbeam::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;

type RequestSender = Sender<(Request, Sender<Response>)>;

lazy_static! {
    pub(super) static ref REQUEST_SENDER: RequestSender = {
        let (requests_in, requests_out) = channel::unbounded();

        thread::spawn(move || match resource_manager(requests_out) {
            Err(msg) => panic!("Resource manager crashed: {}", msg),
            Ok(_) => unreachable!(),
        });

        requests_in
    };
}

fn resource_manager(requests: Receiver<(Request, Sender<Response>)>) -> Result<!, String> {
    let mut tables: HashMap<String, Arc<RwLock<Table>>> = HashMap::new();
    let types: Arc<RwLock<TypeMap>> = Arc::new(RwLock::new(TypeMap::new()));

    loop {
        let (request, response_ch) = requests.recv().map_err(|e| e.to_string())?;

        match request {
            Request::AcquireResources(table_reqs) => {
                let types = types.clone();
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
                    Ok(tables) => {
                        response_ch.send(Response::AcquiredResources(Resources::new(types, tables)))
                    }
                    Err(response) => response_ch.send(response),
                }
                .map_err(|e| e.to_string())?;
            }
            Request::CreateTable(name, table) => {
                if tables.contains_key(&name) {
                    response_ch
                        .send(Response::TableAlreadyExists)
                        .map_err(|e| e.to_string())?;
                } else {
                    tables.insert(name, Arc::new(RwLock::new(table)));
                    response_ch
                        .send(Response::TableCreated)
                        .map_err(|e| e.to_string())?;
                }
            }
        }
    }
}
