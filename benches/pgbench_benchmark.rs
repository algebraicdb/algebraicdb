
use benches::tps::simplebench::start_uds_server;
use benches::{brt, srt};
use std::time::Duration;
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup, Criterion, black_box
};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

use tokio::stream::StreamExt;

use criterion::Throughput;
use futures::future::join_all;

use tokio::net::UnixStream;

fn tps_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("tps_test");
    //group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));
    group.warm_up_time(Duration::from_secs(30));
    tps_benchmark(
        "
        DROP TABLE a;
        CREATE TABLE a (b Integer);
        INSERT INTO a (b) VALUES (1), (2), (3), (4), (5), (6), (7), (8);
        INSERT INTO a (b) SELECT b FROM a;
        INSERT INTO a (b) SELECT b FROM a;
        INSERT INTO a (b) SELECT b FROM a;
        INSERT INTO a (b) SELECT b FROM a;
        INSERT INTO a (b) SELECT b FROM a;
        ",
        "SELECT b FROM a;",
        50,
        100,
        "simple_select",
        &mut group,
    );
    tps_benchmark(
        "
        DROP TABLE a;
        CREATE TABLE a (b Integer);
        INSERT INTO a (b) VALUES (1), (2), (3), (4), (5), (6), (7), (8);
        INSERT INTO a (b) SELECT b FROM a;
        INSERT INTO a (b) SELECT b FROM a;
        INSERT INTO a (b) SELECT b FROM a;
        INSERT INTO a (b) SELECT b FROM a;
        INSERT INTO a (b) SELECT b FROM a;
        ",
        "INSERT INTO a (b) VALUES ({{random_i32}});",
        50,
        100,
        "simple_insert",
        &mut group,
    );
}

fn tps_benchmark(
    setup_instr: &str,
    test_instr: &str,
    num_clients: usize,
    iter_per_client: usize,
    name: &str,
    group: &mut BenchmarkGroup<WallTime>,
) {
    let num_elements: u64 = (iter_per_client * num_clients) as u64;

    group.throughput(Throughput::Elements(num_elements));

    let srt = srt();

    srt.spawn(start_uds_server());
    std::thread::sleep(std::time::Duration::from_secs(5));

    group.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut rt = brt();

                let test_instrs: Vec<String> = (0..iter_per_client).map(|_| {
                        let num: i32 = rand::random();
                        test_instr.replace("{{random_i32}}", &num.to_string())
                    }).collect();

                let connections = rt.block_on(async {
                    let mut stream = UnixStream::connect("./adbench/socket").await.unwrap();
                    let (reader, mut writer) = stream.split();
                    writer.write_all(setup_instr.as_bytes()).await.unwrap();
                    writer.shutdown().await.unwrap();

                    let reader = BufReader::new(reader);
                    let _: Vec<String> = reader.lines().collect::<Result<_, _>>().await.unwrap();

                    let mut connections: Vec<UnixStream> = vec![];

                    let range = 0..num_clients;

                    for _ in range {
                        connections.push(UnixStream::connect("./adbench/socket").await.unwrap());
                    }
                    connections
                });

                (rt, connections, test_instrs)
            },
            |(mut rt, connections, test_instrs)| {
                rt.block_on(async move {
                    let mut tasks = Vec::with_capacity(connections.len());

                    for stream in connections.into_iter() {
                        tasks.push(actual_bench(stream, &test_instrs));
                    }
                    join_all(tasks).await;
                })
            },
            BatchSize::PerIteration,
        );
    });
}

async fn actual_bench(
    mut stream: UnixStream,
    test_instrs: &[String],
) -> Result<(), ()> {
    let (reader, writer) = stream.split();
    let mut buf_writer = BufWriter::new(writer);
    for instr in test_instrs.iter() {
        buf_writer.write_all(black_box(instr.as_bytes())).await.unwrap();
    }
    buf_writer.shutdown().await.unwrap();

    // Await for results to be ready
    let buf_reader = BufReader::new(reader);
    let _: Vec<String> = buf_reader.lines().collect::<Result<_, _>>().await.unwrap();

    Ok(())
}

criterion_group!(benches, tps_bench);
criterion_main!(benches);
