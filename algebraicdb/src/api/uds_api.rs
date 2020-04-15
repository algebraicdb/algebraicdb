use crate::client::client;
use crate::local::WrapperState;
use std::error::Error;
use std::path::{Path, PathBuf};
use tokio::net::UnixListener;

struct DeleteOnDrop {
    path: PathBuf,
    pub(self) listener: UnixListener,
}
impl DeleteOnDrop {
    fn bind(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_owned();
        UnixListener::bind(&path).map(|listener| DeleteOnDrop { path, listener })
    }
}
impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path).unwrap();
    }
}
pub async fn create_uds_server(path: PathBuf, state: WrapperState) -> Result<!, Box<dyn Error>> {
    let mut del_on_drop = DeleteOnDrop::bind(&path)?;

    let listener = &mut del_on_drop.listener;

    println!("listening on socket: {:?}", path);

    loop {
        match listener.accept().await {
            Ok((mut socket, client_address)) => {
                println!("new client [{:?}] connected", client_address);

                // Copy state accessor, not the state itself.
                let state = state.clone();

                tokio::spawn(async move {
                    let (reader, writer) = socket.split();
                    match client(reader, writer, state).await {
                        Ok(()) => {
                            println!("client [{:?}] socket closed", client_address);
                        }
                        Err(e) => {
                            println!("client [{:?}] errored: {}", client_address, e);
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}
