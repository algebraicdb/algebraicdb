#![feature(never_type)]

use algebraicdb::{create_tcp_server, create_uds_server};
use algebraicdb::DbmsConfig;
use std::error::Error;
use algebraicdb::client::State;
use structopt::StructOpt;
use log::{info, debug, LevelFilter};
use tokio::try_join;



#[derive(StructOpt)]
struct Config {
    #[structopt(short, long, default_value = "localhost")]
    address: String,

    #[structopt(short, long, default_value = "/tmp/adbsocket")]
    uds_address: String,

    #[structopt(short, long, default_value = "2345")]
    port: u16,

    #[structopt(flatten)]
    dbms_config: DbmsConfig,
}

#[tokio::main]
#[allow(unreachable_code)]
async fn main() -> Result<(), Box<dyn Error>> {
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

    let state = State::new(config.dbms_config).await;
    
    try_join!(
    create_tcp_server(&config.address, config.port, &state),
    create_uds_server(&config.uds_address, &state)).unwrap();

    Ok(())

}
