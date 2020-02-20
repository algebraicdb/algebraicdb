#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
#![allow(dead_code)]

#[macro_use]
extern crate lazy_static;

mod api;
mod ast;
mod executor;
mod global;
mod grammar;
mod pattern;
mod pre_typechecker;
mod table;
mod typechecker;
mod types;

use crate::ast::Stmt;
use api::tcp_api::tcp_api;
use std::error::Error;
use std::io::Write;

#[tokio::main]
async fn main() -> Result<!, Box<dyn Error>> {
    tcp_api("127.0.0.1:5432".to_string()).await
}

fn execute_query(input: &str, w: &mut dyn Write) -> Result<(), Box<dyn Error>> {
    // 1. parse
    use crate::global::*;
    use crate::grammar::StmtParser;

    let result: Result<Stmt, _> = StmtParser::new().parse(&input);
    let ast = match result {
        Ok(ast) => ast,
        Err(e) => return Ok(write!(w, "{:#?}\n", e)?),
    };

    // 2. determine resources
    let request = pre_typechecker::get_resource_request(&ast);

    // 3. acquire resources
    let response = send_request(request);
    let mut resources = match response {
        Response::AcquiredResources(resources) => resources,
        Response::NoSuchTable(name) => return Ok(write!(w, "No such table: {}\n", name)?),
        _ => unreachable!("Invalid reponse from global::send_request"),
    };
    let resources = resources.take();

    // 4. typecheck
    match typechecker::check_stmt(&ast, &resources) {
        Ok(()) => {}
        Err(e) => return Ok(write!(w, "{:#?}\n", e)?),
    }

    // TODO:
    // 5. Maybe convert ast to some internal representation of a query
    // (See EXPLAIN in postgres/mysql)

    // 6. Execute query
    executor::execute_query(ast, resources, w)
}

fn echo_ast(input: &str, w: &mut dyn Write) -> Result<(), Box<dyn Error>> {
    use crate::grammar::StmtParser;

    match StmtParser::new().parse(&input) {
        Ok(ast) => write!(w, "{:#?}\n", ast)?,
        Err(e) => write!(w, "{:#?}\n", e)?,
    }
    Ok(())
}
