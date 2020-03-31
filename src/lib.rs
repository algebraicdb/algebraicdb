#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
#![feature(async_closure)]
#![allow(dead_code)]

mod api;
pub mod ast;
mod client;
pub mod executor;
pub mod grammar;
pub mod local;
mod pattern;
mod pre_typechecker;
pub mod table;
mod typechecker;
pub mod types;

pub use api::custom::create_with_writers;
pub use api::tcp_api::create_tcp_server;
