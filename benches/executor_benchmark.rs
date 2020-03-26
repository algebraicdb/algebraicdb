use algebraicdb::create_with_writers;
use algebraicdb::executor::dbms::execute_select_from;
use algebraicdb::grammar::StmtParser;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::runtime::{self, Runtime};
use tokio::stream::StreamExt;
fn execute_stmts_benchmarks(c: &mut Criterion) {
    //TODO
}

fn execute_select_from_benchmark(c: &mut Criterion) {
    // Construct stmt
    let parser = StmtParser::new();
    let stmt = parser.parse(r#"SELECT a FROM tab;"#);

    // use unix-pipe for communicating with database
    let (mut db_stream, mut our_stream) = UnixStream::pair().unwrap();

    // Create Tokio Runtime
    let mut rt = rt();

    // Spawn a database
    rt.block_on(async move {
        let (reader, writer) = db_stream.split();
        create_with_writers(reader, writer).await.unwrap();

        // Create some data
        our_stream
            .write_all(
                r#"CREATE TABLE tab (a Integer, b Double);
                INSERT INTO tab (1, 2.0);
                "#
                .as_bytes(),
            )
            .await
            .unwrap();

        // Wait for results, ensure it's written
        let (reader, _) = our_stream.split();
        let reader = BufReader::new(reader);
        let _: Vec<String> = reader.lines().collect::<Result<_, _>>().await.unwrap();
    });

    // TODO
    //c.bench_function()
}

fn rt() -> Runtime {
    runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

criterion_group!(
    benches,
    execute_stmts_benchmarks,
    execute_select_from_benchmark
);
criterion_main!(benches);
