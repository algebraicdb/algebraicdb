#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
#![feature(box_patterns)]
#![feature(async_closure)]
#![allow(dead_code)]

mod api;
pub mod ast;
mod client;
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
pub use util::Timing;
