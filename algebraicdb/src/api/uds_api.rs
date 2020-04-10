use crate::state::DbmsState;
use crate::client::client;
use std::error::Error;
use tokio::net::UnixListener;

pub async fn create_uds_server(
    path: &str,
    state: &DbmsState,
) -> Result<!, Box<dyn Error>> {
    

    let mut listener = UnixListener::bind(path)?;

    info!("listening on socket: {}", path);

    loop {
        match listener.accept().await {
            Ok((mut socket, client_address)) => {
                info!("new client [{:?}] connected", client_address);

                // Copy state accessor, not the state itself.
                let state = state.clone();

                tokio::spawn(async move {
                    let (reader, writer) = socket.split();
                    match client(reader, writer, state).await {
                        Ok(()) => {
                            info!("client [{:?}] socket closed", client_address);
                        }
                        Err(e) => {
                            info!("client [{:?}] errored: {}", client_address, e);
                        }
                    }
                });
            }
            Err(e) => info!("error accepting socket; error = {:?}", e),
        }
    }
}