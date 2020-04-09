use benches::tps::simplebench::connect2;
use benches::{rt, tps::simplebench::{startup_with_wal, startup_no_wal}};

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::io::{AsyncRead, AsyncWrite};

use tokio::stream::StreamExt;

use futures::future::join_all;
use channel_stream::{Writer, Reader};

use criterion::Throughput;

fn temporary(c: &mut Criterion) {
    let mut group = c.benchmark_group("hej");
    group.sample_size(11);

    group.throughput(Throughput::Elements(5000));

    group.bench_function("test", |b| {
        b.iter_batched(
            || {
                let mut rt = rt();

                let connections = rt.block_on(async {
                    let state = startup_with_wal().await.unwrap();
                    let (mut writer, reader) = connect2(state.clone());
                    writer
                        .write_all(
                            b"
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
                    drop(writer);

                    let reader = BufReader::new(reader);
                    let _: Vec<String> = reader.lines().collect::<Result<_, _>>().await.unwrap();

                    let connections: Vec<(Writer, Reader)> = (0..50)
                        .map(|_| connect2(state.clone()))
                        .collect();
                    connections
                });

                (rt, connections)
            },
            |(mut rt, connections)| rt.block_on(async move {
                let mut tasks = Vec::with_capacity(connections.len());
                for (writer, reader) in connections.into_iter() {
                    tasks.push(actual_bench(reader, writer));
                }
                join_all(tasks).await;
            }),
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
    for _ in 0..10 {
        writer.write_all(black_box(b"SELECT b FROM a;SELECT b FROM a;SELECT b FROM a;SELECT b FROM a;SELECT b FROM a;SELECT b FROM a;SELECT b FROM a;SELECT b FROM a;SELECT b FROM a;SELECT b FROM a;")).await.unwrap();
    }
    //writer.write_all(black_box(b"
    //    CREATE TABLE feffes(ass Char);
    //    INSERT INTO feffes(ass) VALUES ('D') ('I') ('C') ('K');
    //    INSERT INTO feffes(ass) VALUES ('8') ('=') ('=') ('D');
    //    SELECT ass FROM feffes;

    //    INSERT INTO feffes(ass) SELECT ass FROM feffes;
    //    INSERT INTO feffes(ass) SELECT ass FROM feffes;
    //    INSERT INTO feffes(ass) SELECT ass FROM feffes;
    //    INSERT INTO feffes(ass) SELECT ass FROM feffes;
    //    INSERT INTO feffes(ass) SELECT ass FROM feffes;
    //    INSERT INTO feffes(ass) SELECT ass FROM feffes;
    //    INSERT INTO feffes(ass) SELECT ass FROM feffes;
    //    INSERT INTO feffes(ass) SELECT ass FROM feffes;
    //    SELECT ass FROM feffes;

    //    DROP TABLE feffes;
    //")).await.unwrap();
    drop(writer);

    // Await for results to be ready
    let buf_reader = BufReader::new(reader);
    let _: Vec<String> = buf_reader.lines().collect::<Result<_, _>>().await.unwrap();

    Ok(())
}

criterion_group!(benches, temporary);
criterion_main!(benches);
