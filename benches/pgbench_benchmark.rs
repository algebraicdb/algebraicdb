use benches::tps::simplebench::{connect2, connect};
use benches::{rt, tps::simplebench::startup_no_wal};
use channel_stream::{pair, Reader, Writer};
use core::time::Duration;
use criterion::Benchmark;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use futures::poll;
use futures::prelude::Future;
use futures::task::Poll;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::{
    unix::{ReadHalf, WriteHalf},
    UnixStream,
};
use tokio::stream::StreamExt;

fn temporary(c: &mut Criterion) {
    let mut group = c.benchmark_group("hej");
    group.sample_size(11);


    eprintln!("done!");
    group.bench_function("test", |b| {
        // b.iter(|| block_on(async {()}))

        //let mut srt = rt();
        b.iter_batched(
            || {
                let mut mrt = rt();

                let (writer, reader) = mrt.block_on(async {
                    let state = startup_no_wal().await.unwrap();
                    let mut stream = connect(state.clone()).await.unwrap();
                    let (reader, mut writer) = stream.split();
                    writer
                        .write_all(
                            "
                            CREATE TABLE a (b Integer);
                            INSERT INTO a (b) VALUES (1);
                            "
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                    //drop(writer);
                    writer.shutdown().await.expect("writer shutown failed");
                    let buf_reader = BufReader::new(reader);
                    let _: Vec<String> = buf_reader.lines().collect::<Result<_, _>>().await.unwrap();
                    connect2(state)
                });

                (mrt, writer, reader)
            },
            |(mut mrt, writer, reader)| 
                mrt.block_on(actual_bench(reader, writer)).unwrap(),
            BatchSize::PerIteration,
        );

        //Benchmark::new("Connection", |b| {
        //    let mut stream: UnixStream;
        //    b.iter(|| block_on(connect(state.clone())).unwrap())
        //    })
        //    .with_function("Do the thing", |b| b.iter(|| {
        //        let (mut reader, mut writer) = stream.split();
        //        block_on(actual_bench(&mut reader, &mut writer)).unwrap()
        //    })
        //    .sample_size(50)))
    });
}

async fn actual_bench<R, W>(reader: R, mut writer: W) -> Result<(), ()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    eprintln!("i'm gonna start writing now i tell you hwhat");
    writer.write_all(b"SELECT b FROM a;").await.unwrap();

    // while let Poll::Pending = w.poll(){

    // };

    eprintln!("wrote some shit");
    drop(writer);
    //writer.flush().await.expect("flushing writer failed");

    // Await for results to be ready
    //dbg!(writer.shutdown().await.expect("writer shutown failed"));

    let buf_reader = BufReader::new(reader);
    let mut lines = buf_reader.lines();
    let mut count: usize = 0;
    while let Some(line) = lines.next().await {
        count += 1;
        eprintln!("line {}: {}", count, line.expect("read line failed"));
    }

    //let mut buf = String::new();
    //eprintln!("heyo let's  go");
    //let mut breader = BufReader::with_capacity(1000000, reader);
    //let _: Vec<String> = breader.lines().collect::<Result<_, _>>().await.unwrap();
    //let _: Vec<String> = breader.lines().collect::<Result<_, _>>().await.unwrap();
    // eprintln!("THE OUTPUT BE THE {}", buf);

    // if (buf != "[1]\n"){
    //     panic!(buf)
    // }



    eprintln!("jobs done");
    Ok(())
}

criterion_group!(benches, temporary);
criterion_main!(benches);
