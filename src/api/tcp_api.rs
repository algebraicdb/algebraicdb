use crate::client::client;
use crate::client::State;
use std::error::Error;
use tokio::net::TcpListener;

/// Start an instance of the dbms which binds itself to a tcp socket
pub async fn create_tcp_server(address: &str) -> Result<!, Box<dyn Error>> {
    let state = State::new().await?;

    let mut listener = TcpListener::bind(address).await?;

    loop {
        match listener.accept().await {
            Ok((mut socket, client_address)) => {
                println!("new client [{}] connected", client_address);

                // Copy state accessor, not the state itself.
                let state = state.clone();

                tokio::spawn(async move {
                    let (reader, writer) = socket.split();
                    match client(reader, writer, state).await {
                        Ok(()) => {
                            println!("client [{}] socket closed", client_address);
                        }
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
