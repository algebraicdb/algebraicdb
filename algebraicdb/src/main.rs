#![feature(never_type)]

use algebraicdb::create_tcp_server;
use algebraicdb::local::WrapperState;
use algebraicdb::psqlwrapper::db_connection::connect_db;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<!, Box<dyn Error>> {
    let cli = connect_db().await?;
    let state = WrapperState::new(cli).await;
    create_tcp_server("127.0.0.1:5001", &state).await
}
