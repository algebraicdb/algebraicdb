#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
#![feature(async_closure)]
#![allow(dead_code)]
#![feature(type_ascription)]

mod api;
pub mod ast;
pub mod client;
pub mod executor;
pub mod grammar;
pub mod local;
mod pattern;
mod pre_typechecker;
pub mod psqlwrapper;
pub mod table;
mod typechecker;
pub mod types;

pub use api::custom::create_with_writers;
pub use api::tcp_api::create_tcp_server;
pub use api::uds_api::create_uds_server;
pub use executor::wrapper::drop_all_tables;
pub use tokio_postgres::Client;
