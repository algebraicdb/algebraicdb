#![feature(never_type)]

use algebraicdb::create_tcp_server;
use algebraicdb::DbmsConfig;
use std::error::Error;
use structopt::StructOpt;
use log::{info, debug, LevelFilter};

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

    if cfg!(debug_assertions) {
        pretty_env_logger::formatted_builder()
            .filter_level(LevelFilter::max())
            .init();
        debug!("running in debug mode");
    } else {
        pretty_env_logger::init();
    }
    info!("setting up server");

    create_tcp_server(&config.address, config.port, config.dbms_config).await
}
