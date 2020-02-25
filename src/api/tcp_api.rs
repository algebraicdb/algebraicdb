use crate::local::*;
use regex::Regex;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpStream, TcpListener};
use std::net::SocketAddr;
use crate::execute_query;

pub async fn tcp_api(address: &str) -> Result<!, Box<dyn Error>> {
    let state = DbmsState::new();

    let mut listener = TcpListener::bind(address).await?;

    loop {
        match listener.accept().await {
            Ok((socket, client_address)) => {
                println!("new client [{}] connected", client_address);

                // Copy state accessor, not the state itself.
                let state = state.clone();

                tokio::spawn(async move {
                    match client(socket, client_address, state).await {
                        Ok(()) => {}
                        Err(e) => {
                            println!("client [{}] errored: {}", client_address, e);
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

async fn client(mut socket: TcpStream, client_address: SocketAddr, state: DbmsState) -> Result<(), Box<dyn Error>> {
    let (mut reader, writer) = socket.split();
    let mut writer = BufWriter::new(writer);
    let mut buf = vec![];

    // This regex matches the entire string from the start to the first non-quoted semi-colon.
    // It also properly handles escaped quotes
    // valid string: SELECT "this is a quote -> \", this is a semicolon -> ;.";
    let r = Regex::new(r#"^(("((\\.)|[^"])*")|[^";])*;"#).expect("Invalid regex");

    loop {
        let _n: usize = match reader.read_buf(&mut buf).await? {
            // No bytes read means EOF was reached
            0 => {
                println!("client [{}] socket closed", client_address);
                return Ok(());
            }
            // Read n bytes
            n => n,
        };

        // Loop over every statement (every substring ending with a semicolon)
        // This leaves the remaining un-terminated string in the buf.
        //   stmt 1         stmt 2           stmt 3   rest
        // ┍╌╌╌┷╌╌╌┑┍╌╌╌╌╌╌╌╌╌┷╌╌╌╌╌╌╌╌╌╌┑┍╌╌╌╌┷╌╌╌╌┑┍╌┷╌┑
        // SELECT 1; SELECT "stuff: \" ;";  SELECT 3; SELE
        loop {
            // Validate bytes as utf-8 string
            let input = match std::str::from_utf8(&buf[..]) {
                Ok(input) => input,
                Err(e) => {
                    writer.write_all(format!("Error: {}\n", e).as_bytes()).await?;
                    writer.flush().await?;
                    return Err(e.into());
                }
            };


            // Match string against regex
            let (input, end) = match r.find(input) {
                Some(matches) => (matches.as_str(), matches.end()),
                None => break,
            };

            execute_query(input, &state, &mut writer).await?;
            writer.flush().await?;

            // Remove the string of the executed query from the buffer
            buf.drain(..end);
        }
    }
}
