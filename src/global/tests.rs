use super::*;
use crate::table::Table;
use crossbeam::thread;
use rand::Rng;
//use colorful::Color;
//use colorful::Colorful;

/// Make sure there are not deadlocks when multiple threads are requesting resources.
#[test]
fn global_resources_contention() {
    let table_ids: Vec<usize> = (0..20).collect();

    for &id in table_ids.iter() {
        let resp = send_request(Request::CreateTable(
            format!("table_{}", id),
            Table::new(vec![], &Default::default()),
        ));
        if let Response::TableCreated = resp {
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
                    let request: Vec<_> = table_ids
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
                    //eprintln!("{} {} {} {:?}", "==".color(Color::Red), thread, "requesting".color(Color::Red), request);

                    let request_len = request.len();

                    let response = send_request(Request::AcquireResources(request));
                    match response {
                        Response::AcquiredResources(mut resources) => {
                            let guard = resources.take();
                            assert_eq!(guard.tables.len(), request_len);
                            drop(guard)
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
    })
    .unwrap();
}
