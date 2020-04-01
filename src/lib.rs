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
mod pre_typechecker;
mod table;
mod typechecker;
mod types;
mod wal;

pub use api::custom::create_with_writers;
pub use api::tcp_api::create_tcp_server;
