#![feature(never_type)]

use algebraicdb::state::DbmsState;
use algebraicdb::DbmsConfig;
use algebraicdb::{create_tcp_server, create_uds_server};
use log::{debug, info, LevelFilter};
use std::error::Error;
use std::path::PathBuf;
use structopt::StructOpt;
use tokio::signal::ctrl_c;

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

    let state: DbmsState = DbmsState::new(config.dbms_config).await;
    let uds_state = state.clone();
    let (address, port, uds_address) = (config.address, config.port, config.uds_address);

    tokio::spawn(async move {
        create_tcp_server(address.as_str(), port, state)
            .await
            .unwrap()
    });
    tokio::spawn(async move {
        create_uds_server(PathBuf::from(uds_address), uds_state)
            .await
            .unwrap()
    });

    ctrl_c().await.unwrap();
    info!("Shutting down...");
    Ok(())
}
