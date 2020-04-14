#![feature(str_strip)]
#![feature(const_int_pow)]
#![feature(never_type)]
#![feature(box_syntax)]
#![feature(box_patterns)]
#![feature(async_closure)]
#![allow(dead_code)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

mod api;
pub mod ast;
pub mod client;
mod error_message;
pub mod executor;
pub mod grammar;
mod persistence;
mod pre_typechecker;
pub mod state;
pub mod table;
mod typechecker;
pub mod types;
mod util;

pub use api::config::DbmsConfig;
pub use api::custom::create_with_writers;
pub use api::tcp_api::create_tcp_server;
pub use api::uds_api::create_uds_server;
pub use util::Timing;
