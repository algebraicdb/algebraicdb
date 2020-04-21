#![feature(never_type)]

use algebraicdb::state::DbmsState;
use algebraicdb::DbmsConfig;
use algebraicdb::create_tcp_server;
use log::{debug, info, LevelFilter};
use std::error::Error;
use structopt::StructOpt;
use tokio::signal::ctrl_c;

#[cfg(unix)]
use algebraicdb::create_uds_server;

#[derive(StructOpt)]
struct Config {
    #[structopt(short, long, default_value = "localhost")]
    address: String,

    #[cfg(unix)]
    #[structopt(short, long, default_value = "/tmp/adbsocket")]
    uds_address: String,

    #[structopt(short, long, default_value = "2345")]
    port: u16,

    #[structopt(flatten)]
    dbms_config: DbmsConfig,
}

#[tokio::main]
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

    let (address, port) = (config.address, config.port);

    #[cfg(unix)]
    let (uds_address, uds_state) = (config.uds_address, state.clone());

    tokio::spawn(async move {
        create_tcp_server(address.as_str(), port, state)
            .await
            .unwrap()
    });
    #[cfg(unix)]
    tokio::spawn(async move {
        use std::path::PathBuf;
        create_uds_server(PathBuf::from(uds_address), uds_state)
            .await
            .unwrap()
    });

    ctrl_c().await.unwrap();
    info!("shutting down...");
    Ok(())
}
