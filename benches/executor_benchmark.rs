use algebraicdb::ast::SelectFrom;
use algebraicdb::executor::dbms::execute_select_from;
use algebraicdb::local::types::{Resource, ResourcesGuard};
use algebraicdb::table::{Row, Schema, Table};
use algebraicdb::types::{BaseType, TypeMap, Value};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::{self, Runtime};
use tokio::sync::RwLock;

fn execute_select_from_benchmark(c: &mut Criterion) {
    // Create the table
    let types = TypeMap::new();
    let int_id = types.get_base_id(BaseType::Integer);
    let columns = vec![(String::from("col1"), int_id)];
    let schema = Schema { columns };
    let mut table = Table::new(schema.clone(), &types);

    for i in 0..1000 {
        table.push_row(&[Value::Integer(i)], &types);
    }

    let lock = RwLock::new(table);
    let type_lock = RwLock::new(types.clone());

    let mut rt = rt();
    let res_guard: ResourcesGuard<Table> = {
        rt.block_on(async {
            ResourcesGuard {
                tables: vec![("tab", Resource::Read(lock.read().await))],
                type_map: Resource::Read(type_lock.read().await),
            }
        })
    };

    let s_from = SelectFrom::Table(String::from("tab"));

    c.bench_function("Executor Benchmark: execute_select_from size 1", |b| {
        b.iter(|| {
            execute_select_from(black_box(&s_from), black_box(&res_guard))
                .iter(&types)
                .for_each(|_row| ());
        })
    });
}

fn rt() -> Runtime {
    runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

criterion_group!(benches, execute_select_from_benchmark);
criterion_main!(benches);
