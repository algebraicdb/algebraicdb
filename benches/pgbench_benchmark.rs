#![feature(async_closure)]

use benches::tps::simplebench::{connect, connect2, start_uds_server};
use benches::{
    brt,
    srt,
    tps::simplebench::{startup_no_wal, startup_with_wal},
};

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::io::{AsyncRead, AsyncWrite};

use tokio::stream::StreamExt;

use channel_stream::{Reader, Writer};
use futures::future::join_all;
use futures::Future;
use futures::future::FutureExt;
use criterion::Throughput;
use std::net::Shutdown;


use tokio::net::UnixStream;



fn temporary(c: &mut Criterion) {


    let mut group = c.benchmark_group("hej");
    group.sample_size(10);

    group.throughput(Throughput::Elements(5000));

    let srt = srt();

    srt.spawn(start_uds_server());
    
    std::thread::sleep(std::time::Duration::from_secs(3));
    

    group.bench_function("test", |b| {
        b.iter_batched(
            || {
                let mut rt = brt();

                let connections = rt.block_on(async {
                    let mut stream = UnixStream::connect("/tmp/adbench/socket").await.unwrap();
                    let (reader, mut writer) = stream.split();
                    writer
                        .write_all(
                            b"
                            DROP TABLE a;
                            CREATE TABLE a (b Integer);
                            INSERT INTO a (b) VALUES (1), (2), (3), (4), (5), (6), (7), (8);
                            INSERT INTO a (b) SELECT b FROM a;
                            INSERT INTO a (b) SELECT b FROM a;
                            INSERT INTO a (b) SELECT b FROM a;
                            INSERT INTO a (b) SELECT b FROM a;
                            INSERT INTO a (b) SELECT b FROM a;
                            ",
                        )
                        .await
                        .unwrap();
                    writer.shutdown().await.unwrap();

                    let reader = BufReader::new(reader);
                    let _: Vec<String> = reader.lines().collect::<Result<_, _>>().await.unwrap();

                    let mut connections: Vec<UnixStream> = vec![];

                    let range = (0..50);

                    for _ in range {
                        connections.push(UnixStream::connect("/tmp/adbench/socket").await.unwrap());
                    }

                    //let _connections: Vec<UnixStream> = (0..50).map(async move |_| UnixStream::connect("/tmp/adbench/socket").await.unwrap()).collect::<futures::stream::FuturesOrdered<UnixStream>>().await;
                    //let connections: Vec<UnixStream> =
                    //    join_all(_connections).await;
                    connections
                });

                (rt, connections)
            },
            |(mut rt, connections)| {
                rt.block_on(async move {
                    let mut tasks = Vec::with_capacity(connections.len());

                    for stream in connections.into_iter() {
                        tasks.push(actual_bench_2(stream));
                    }
                    join_all(tasks).await;
                })
            },
            BatchSize::PerIteration,
        );
    });
}

/*
fn select_from_small_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("pgbench-ish");
    group.sample_size(11);
    group.bench_function("select_from_small_table", |b| bench_query(
        b"CREATE TABLE t(a Integer);
        INSERT INTO t(a) VALUES (42);",
        b"SELECT a FROM b",
        b
    ));
}

fn bench_query(setup_query: &[u8], bench_query: &[u8], b: &mut Bencher) {
        b.iter_batched(
            || {
                let mut rt = rt();

                let (writer, reader) = rt.block_on(async {
                    let state = startup_with_wal().await.unwrap();
                    let (mut writer, reader) = connect2(state.clone());
                    writer
                        .write_all(setup_query)
                        .await
                        .unwrap();
                    drop(writer);

                    let reader = BufReader::new(reader);
                    let _: Vec<String> = reader.lines().collect::<Result<_, _>>().await.unwrap();
                    connect2(state)
                });

                (rt, writer, reader)
            },
            |(mut rt, writer, reader)| rt.block_on(async move {
                    // Send query to the database
                    writer.write_all(bench_query.await.unwrap();
                    drop(writer);

                    // Wait for response from the database
                    let buf_reader = BufReader::new(reader);
                    let _: Vec<String> = buf_reader.lines().collect::<Result<_, _>>().await.unwrap();
                }).unwrap(),
            BatchSize::PerIteration,
        );
}
*/

async fn actual_bench<R, W>(reader: R, mut writer: W) -> Result<(), ()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf_writer = BufWriter::new(writer);
    for _ in 0..10 {
        buf_writer.write_all(black_box(b"SELECT b FROM a; SELECT b FROM a; SELECT b FROM a; SELECT b FROM a; SELECT b FROM a; SELECT b FROM a; SELECT b FROM a; SELECT b FROM a; SELECT b FROM a; SELECT b FROM a;")).await.unwrap();
    }

    //drop(writer);
    buf_writer.shutdown().await.unwrap();
    // Await for results to be ready
    let buf_reader = BufReader::new(reader);
    let _: Vec<String> = buf_reader.lines().collect::<Result<_, _>>().await.unwrap();

    Ok(())
}

async fn actual_bench_2(mut stream:  UnixStream) -> Result<(), ()>
{
    let (reader, writer) = stream.split();

    let mut buf_writer = BufWriter::new(writer);
    for _ in 0..100 {
        buf_writer.write_all(black_box(b"SELECT b FROM a;")).await.unwrap();
    }

    //drop(writer);
    buf_writer.shutdown().await.unwrap();

    // Await for results to be ready
    let buf_reader = BufReader::new(reader);
    let _: Vec<String> = buf_reader.lines().collect::<Result<_, _>>().await.unwrap();
    Ok(())
}

criterion_group!(benches, temporary);
criterion_main!(benches);
