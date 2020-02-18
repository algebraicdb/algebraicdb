#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
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
mod executor;

use api::tcp_api::tcp_api;
use std::error::Error;
use crate::ast::Stmt;

#[tokio::main]
async fn main() -> Result<!, Box<dyn Error>> {
    tcp_api(execute_query, "127.0.0.1:5432".to_string()).await
}

fn execute_query(input: &str) -> String {
    // 1. parse
    use crate::grammar::StmtParser;
    use crate::global::*;

    let result: Result<Stmt, _> = StmtParser::new().parse(&input);
    let ast = match result {
        Ok(ast) => ast,
        Err(e) => return format!("{:#?}\n", e),
    };

    // 2. determine resources
    let resource_request = pre_typechecker::get_table_permissions(&ast);

    // 3. acquire resources
    let request = Request::AcquireResources(resource_request);
    let response = send_request(request);
    let mut resources = match response {
        Response::AcquiredResources(resources) => resources,
        Response::NoSuchTable(name) => return format!("No such table: {}\n", name),
        _ => unreachable!("Invalid reponse from global::send_request"),
    };
    let resources = resources.take();

    // 4. typecheck
    match typechecker::check_stmt(&ast, &resources) {
        Ok(()) => {},
        Err(e) => return format!("{:#?}\n", e),
    }

    // 5. TODO: Maybe convert ast to some internal representation of a query (See EXPLAIN in postgres/mysql)

    // 6. TODO: Execute query
    executor::execute_query(ast, resources)
}

fn echo_ast(input: &str) -> String {
    use crate::grammar::StmtParser;

    match StmtParser::new().parse(&input) {
        Ok(ast) => format!("{:#?}\n", ast),
        Err(e) => format!("{:#?}\n", e),
    }
}
