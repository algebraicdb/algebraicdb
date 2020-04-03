#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
#![feature(async_closure)]
#![allow(dead_code)]

mod api;
mod ast;
mod client;
mod executor;
mod grammar;
mod local;
mod pattern;
mod persistence;
mod pre_typechecker;
mod table;
mod typechecker;
mod types;
mod util;

pub use api::config::DbmsConfig;
pub use api::custom::create_with_writers;
pub use api::tcp_api::create_tcp_server;
pub use util::Timing;
