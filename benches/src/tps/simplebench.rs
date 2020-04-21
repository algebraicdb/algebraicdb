use algebraicdb::client::client;
use algebraicdb::create_uds_server;
use algebraicdb::drop_all_tables;
use algebraicdb::{local::WrapperState, Client};
use std::error::Error;
use std::{path::PathBuf, sync::Arc};
use tokio::fs::{create_dir_all, remove_dir_all, remove_file};
use tokio::net::UnixStream;

pub fn connect(state: WrapperState) -> Result<UnixStream, Box<dyn Error>> {
    // use unix-pipe for communicating with database
    let (mut db_stream, our_stream) = UnixStream::pair().unwrap();

    // Spawn a database
    tokio::spawn(async move {
        let (reader, writer) = db_stream.split();
        client(reader, writer, state).await.unwrap();
    });

    Ok(our_stream)
}

pub async fn start_uds_server(cli: Arc<Client>) {
    let state = WrapperState::new(cli).await;
    drop_all_tables(&state).await.unwrap();

    remove_dir_all("/tmp/adbench/").await.unwrap_or(());
    create_dir_all("/tmp/adbench/").await.unwrap();
    remove_file("/tmp/adbench/socket").await.unwrap_or(());
    create_uds_server(PathBuf::from("/tmp/adbench/socket"), state)
        .await
        .unwrap();
}
