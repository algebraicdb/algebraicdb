#![feature(never_type)]

use algebraicdb::create_tcp_server;
use algebraicdb::DbmsConfig;
use std::error::Error;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    #[structopt(short, long, default_value = "localhost")]
    address: String,

    #[structopt(short, long, default_value = "2345")]
    port: u16,

    #[structopt(flatten)]
    dbms_config: DbmsConfig,
}

#[tokio::main]
async fn main() -> Result<!, Box<dyn Error>> {
    let config = Config::from_args();

    create_tcp_server(&config.address, config.port, config.dbms_config).await
}
