#![warn(rust_2018_idioms)]

use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use tokio::io::{BufReader, BufWriter, AsyncBufReadExt, AsyncWriteExt};
use futures::SinkExt;
use std::sync::{Arc, Mutex};
use std::error::Error;


pub async fn tcpapi(func: fn(String) -> String) -> Result<!, Box<dyn Error>> {

    let adr ="127.0.0.1:8080".to_string();

    let mut listener = TcpListener::bind(adr).await?;


    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.split();
                    let mut buf: Vec<u8> = vec![];
                    let mut reader: BufReader<_> = BufReader::new(reader);

                    loop {
                        let n: usize = reader.read_until(b';', &mut buf).await.unwrap();

                        let input = std::str::from_utf8(&buf[..n]).expect("Not valid utf-8").to_string();

                        let result = func(input);
                        eprintln!("{}", result);

                        buf.drain(..n);
                        writer.write_all(result.as_bytes()).await.unwrap();
                        writer.flush();
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }

}
