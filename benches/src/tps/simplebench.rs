use algebraicdb::client::client;
use algebraicdb::{state::DbmsState, DbmsConfig};
use algebraicdb::Timing;
use channel_stream::{pair, Reader, Writer};
use std::error::Error;
use tokio::net::UnixStream;
use tokio::fs::{create_dir, remove_dir_all};

pub fn connect2(state: DbmsState) -> (Writer, Reader) {
    // use custom channel stream for communicating with database
    let (dbms_writer, our_reader) = pair();
    let (our_writer, dbms_reader) = pair();

    // Create a client connection with the dbms
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

pub async fn startup_with_wal() -> Result<DbmsState, Box<dyn Error>> {
    let data_dir = "/tmp/adbench_data".into();
    remove_dir_all(&data_dir).await.ok();
    create_dir(&data_dir).await.unwrap();

    let config = DbmsConfig {
        no_persistence: false,
        disk_flush_timing: Timing::Never(),
        data_dir,
        ..DbmsConfig::testing_config()
    };

    Ok(DbmsState::new(config).await)
}
