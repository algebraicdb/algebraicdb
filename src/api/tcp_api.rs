use crate::local::*;
use regex::Regex;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub async fn tcp_api(address: &str) -> Result<!, Box<dyn Error>> {
    let state = DbmsState::new();

    let mut listener = TcpListener::bind(address).await?;

    loop {
        match listener.accept().await {
            Ok((mut socket, client_address)) => {
                println!("new client [{}] connected", client_address);

                // Copy state accessor, not the state itself.
                let state = state.clone();

                tokio::spawn(async move {
                    let (mut reader, mut writer) = socket.split();
                    let mut buf = vec![];
                    let r = Regex::new(r#"^(("((\\.)|[^"])*")|[^";])*;"#).unwrap();

                    loop {
                        let _n: usize = match reader.read_buf(&mut buf).await {
                            Err(e) => {
                                println!("error on client [{}] socket: {}", client_address, e);
                                return;
                            }
                            // No bytes read means EOF was reached
                            Ok(0) => {
                                println!("client [{}] socket closed", client_address);
                                return;
                            }
                            // Read n bytes
                            Ok(n) => n,
                        };

                        loop {
                            let input = std::str::from_utf8(&buf[..]).expect("Not valid utf-8");
                            let (input, end) = match r.find(input) {
                                Some(matches) => (matches.as_str(), matches.end()),
                                None => break,
                            };

                            crate::execute_query(input, &state, &mut writer)
                                .await
                                .unwrap();
                            writer.flush().await.expect("Flushing writer failed");

                            buf.drain(..end);
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}
