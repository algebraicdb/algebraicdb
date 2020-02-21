use super::*;
use crate::table::{Schema, Table};
use crate::types::TypeMap;
use crossbeam::thread;
use futures::executor::block_on;
use rand::Rng;
//use colorful::Color;
//use colorful::Colorful;

/// Make sure there are not deadlocks when multiple threads are requesting resources.
#[tokio::test]
async fn global_resources_contention() {
    let state = DbmsState::new();
    let state = &state;

    let table_ids: Vec<usize> = (0..20).collect();

    for &id in table_ids.iter() {
        let resp = block_on(state.create_table(
            format!("table_{}", id),
            Table::new(Schema::empty(), &TypeMap::new()),
        ));
        match resp {
            Ok(_) => { /*everything is fine*/ }
            Err(_) => panic!("Invalid response from create_table, expected Ok"),
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

                    let request = Acquire {
                        table_reqs,
                        type_map_perms: RW::Read,
                    };

                    //eprintln!("{} {} {} {:?}", "==".color(Color::Red), thread, "requesting".color(Color::Red), request);

                    match block_on(state.acquire_resources(request)) {
                        Ok(mut resources) => {
                            let guard = block_on(resources.take());
                            assert_eq!(guard.tables.len(), table_count);
                            drop(guard);
                            // TODO: verity correct tables
                        }
                        // TODO: Response does not impl Debug
                        //other => panic!("Invalid response, expected AcquiredResources, got {:?}", other),
                        Err(_) => panic!("Invalid response, expected AcquiredResources"),
                    }

                    //eprintln!("{} {} {}", "==".color(Color::Green), thread, "done".color(Color::Green));
                }
            });
        }
    })
    .unwrap();
}
