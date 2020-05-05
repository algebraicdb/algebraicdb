use algebraicdb::ast::SelectFrom;
use algebraicdb::executor::dbms::execute_select_from;
use algebraicdb::state::types::{PermLock, RW};
use algebraicdb::table::{Schema, TableData};
use algebraicdb::types::{BaseType, TypeMap, Value};

use std::sync::Arc;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::sync::RwLock;

use benches::rt;


fn execute_select_from_benchmark(c: &mut Criterion) {
    // Create the table
    let types = TypeMap::new();
    let int_id = types.get_base_id(BaseType::Integer);
    let columns = vec![(String::from("col1"), int_id)];
    let schema = Schema { columns };
    let mut table = TableData::new(&schema, &types);

    for i in 0..1000 {
        table.push_row(&[Value::Integer(i)], &schema, &types);
    }

    let schema_lock = PermLock::new(RW::Read, Arc::new(RwLock::new(schema)));
    let data_lock = PermLock::new(RW::Read, Arc::new(RwLock::new(table)));
    let type_lock = PermLock::new(RW::Read, Arc::new(RwLock::new(types.clone())));

    let mut rt = rt();

    let (type_guard, data_guards, schema_guards) = rt.block_on(async {
        let type_guard = type_lock.lock().await;
        let data_guards = vec![("tab", data_lock.lock().await)].into_iter().collect();
        let schema_guards = vec![("tab", schema_lock.lock().await)].into_iter().collect();

        (type_guard, data_guards, schema_guards)
    });

    let s_from = SelectFrom::Table("tab".to_string());

    c.bench_function("Executor Benchmark: execute_select_from size 1", |b| {
        b.iter(|| {
            execute_select_from(
                black_box(&s_from),
                &type_guard,
                &schema_guards,
                &data_guards,
            )
                .iter(&types)
                .for_each(|_row| ());
        })
    });
}

criterion_group!(benches, execute_select_from_benchmark);
criterion_main!(benches);
