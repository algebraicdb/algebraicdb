use algebraicdb::client::client;
use algebraicdb::{local::DbmsState, DbmsConfig};
use channel_stream::{pair, Reader, Writer};
use std::error::Error;
use tokio::net::UnixStream;

use tokio::runtime::Runtime;

pub fn connect2(state: DbmsState) -> (Writer, Reader) {
    let (dbms_writer, our_reader) = pair();
    let (our_writer, dbms_reader) = pair();

    // Spawn a database
    //rt.spawn(async move {
    tokio::spawn(async move {
        client(dbms_reader, dbms_writer, state).await.unwrap();
    });

    (our_writer, our_reader)
}

pub async fn connect(state: DbmsState) -> Result<UnixStream, Box<dyn Error>> {
    // use unix-pipe for communicating with database
    let (mut db_stream, our_stream) = UnixStream::pair().unwrap();

    // Spawn a database
    tokio::spawn(async move {
        let (reader, writer) = db_stream.split();
        client(reader, writer, state).await.unwrap();
    });

    Ok(our_stream)
}

pub async fn startup_no_wal() -> Result<DbmsState, Box<dyn Error>> {
    let config = DbmsConfig::testing_config();
    Ok(DbmsState::new(config).await)
}
