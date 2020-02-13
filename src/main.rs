#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
// TODO: remove this once we actually start using our code
#![allow(dead_code)]

#[macro_use]
extern crate lazy_static;

mod ast;
mod global;
mod grammar;
mod pattern;
mod pre_typechecker;
mod table;
mod typechecker;
mod types;
mod api;

use api::tcpapi::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tcpapi(print_ast).await.unwrap();

    Ok(())
    
}

fn print_ast(input: String) -> String {
    use crate::grammar::StmtParser;

    match StmtParser::new().parse(&input) {
        Ok(ast) => format!("{:#?}", ast),
        Err(e) => format!("{:#?}", e),
    }
}

fn backend(stri: String) -> String {
    println!("{}", stri);
    String::from("fku")
}