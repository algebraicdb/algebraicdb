use crate::api::config::DbmsConfig;
use crate::client::client;
use crate::client::State;
use std::error::Error;
use tokio::net::TcpListener;

/// Start an instance of the dbms which binds itself to a tcp socket
pub async fn create_tcp_server(
    address: &str,
    port: u16,
    dbms_config: DbmsConfig,
) -> Result<!, Box<dyn Error>> {
    let state = State::new(dbms_config).await;

    let mut listener = TcpListener::bind((address, port)).await?;

    info!("listening on {}:{}", address, port);

    loop {
        match listener.accept().await {
            Ok((mut socket, client_address)) => {
                info!("new client [{}] connected", client_address);

                // Copy state accessor, not the state itself.
                let state = state.clone();

                tokio::spawn(async move {
                    let (reader, writer) = socket.split();
                    match client(reader, writer, state).await {
                        Ok(()) => {
                            info!("client [{}] socket closed", client_address);
                        }
                        Err(e) => {
                            info!("client [{}] errored: {}", client_address, e);
                        }
                    }
                });
            }
            Err(e) => info!("error accepting socket; error = {:?}", e),
        }
    }
}
