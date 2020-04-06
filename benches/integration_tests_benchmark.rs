use algebraicdb::{create_with_writers, DbmsConfig};
use std::io;
use std::net::Shutdown;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::runtime::{self, Runtime};
use tokio::stream::StreamExt;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn integration_tests_benchmark(c: &mut Criterion) {
    let mut dir = std::fs::read_dir("../algebraicdb/test_queries/").unwrap();
    let mut group = c.benchmark_group("Integration Tests");
    group.sample_size(10);
    while let Some(Ok(entry)) = dir.next() {
        if entry.file_type().map(|f| f.is_dir()).unwrap_or(false) {
            let mut input_path = entry.path();

            input_path.push("input");

            let input = std::fs::read_to_string(input_path).unwrap().clone();

            let mut rt = rt();

            group.bench_function(
                format!(
                    "Test: {}",
                    entry.file_name().into_string().unwrap()
                )
                .as_str(),
                |b| {
                    b.iter(|| {
                        rt.block_on(async {
                            let _ = run_example_query(black_box(input.clone())).await.unwrap();
                        })
                    })
                },
            );
        }
    }
}

fn rt() -> Runtime {
    runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

async fn run_example_query(input: String) -> io::Result<Result<(), ()>> {
    // use unix-pipe for communicating with database
    let (mut db_stream, mut our_stream) = UnixStream::pair().unwrap();

    // Spawn a database
    tokio::spawn(async move {
        let (reader, writer) = db_stream.split();
        let config = DbmsConfig::testing_config();
        create_with_writers(reader, writer, config).await.unwrap();
    });
    our_stream.write_all(input.as_bytes()).await?;
    our_stream.shutdown(Shutdown::Write)?;

    let (reader, _) = our_stream.split();
    let reader = BufReader::new(reader);
    // Await for results to be ready
    let _: Vec<String> = reader.lines().collect::<Result<_, _>>().await?;
    Ok(Ok(()))
}

criterion_group!(integration, integration_tests_benchmark);
criterion_main!(integration);
