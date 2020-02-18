#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
#![allow(dead_code)]

#[macro_use]
extern crate lazy_static;

mod api;
mod ast;
mod global;
mod grammar;
mod pattern;
mod pre_typechecker;
mod table;
mod typechecker;
mod types;

use api::tcp_api::tcp_api;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<!, Box<dyn Error>> {
    tcp_api(echo_ast, "127.0.0.1:5432".to_string()).await
}

fn echo_ast(input: &str) -> String {
    use crate::grammar::StmtParser;

    match StmtParser::new().parse(&input) {
        Ok(ast) => format!("{:#?}\n", ast),
        Err(e) => format!("{:#?}\n", e),
    }
}
