use super::*;
use crate::table::{Schema, Table};
use crate::types::TypeMap;
use futures::executor::block_on;
use rand::Rng;
use crossbeam::thread;
//use colorful::Color;
//use colorful::Colorful;

/// Make sure there are not deadlocks when multiple threads are requesting resources.
#[tokio::test]
async fn global_resources_contention() {
    let table_ids: Vec<usize> = (0..20).collect();

    for &id in table_ids.iter() {
        let resp = block_on(send_request(Request::CreateTable(
            format!("table_{}", id),
            Table::new(Schema::empty(), &TypeMap::new()),
        )));
        if let Response::TableCreated = resp {
            // everything is fine
        } else {
            panic!("Invalid response, expected TableCreated");
        }
    }

    let table_ids = &table_ids;

    thread::scope(|s| {
        for _thread in 0..10 {
            s.spawn(move |_| {
                let mut rng = rand::thread_rng();

                for _ in 0..10000 {
                    let table_reqs: Vec<_> = table_ids
                        .iter()
                        .filter_map(|table_id| {
                            let i: usize = rng.gen();
                            // select 25% of tables
                            if i % 100 < 25 {
                                Some(TableRequest {
                                    table: format!("table_{}", table_id),
                                    rw: if rng.gen::<bool>() {
                                        RW::Read
                                    } else {
                                        RW::Write
                                    },
                                })
                            } else {
                                None
                            }
                        })
                        .collect();

                    let table_count = table_reqs.len();

                    let request = Request::AcquireResources {
                        table_reqs,
                        type_map_perms: RW::Read,
                    };

                    //eprintln!("{} {} {} {:?}", "==".color(Color::Red), thread, "requesting".color(Color::Red), request);

                    match block_on(send_request(request)) {
                        Response::AcquiredResources(mut resources) => {
                        let guard = block_on(resources.take());
                            assert_eq!(guard.tables.len(), table_count);
                            drop(guard);
                            // TODO: verity correct tables
                        }
                        // TODO: Response does not impl Debug
                        //other => panic!("Invalid response, expected AcquiredResources, got {:?}", other),
                        _other => panic!("Invalid response, expected AcquiredResources"),
                    }

                    //eprintln!("{} {} {}", "==".color(Color::Green), thread, "done".color(Color::Green));
                }
            });
        }
    }).unwrap();
}
