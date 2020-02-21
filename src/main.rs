#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]
#![feature(async_closure)]
#![allow(dead_code)]

mod api;
mod ast;
mod executor;
mod grammar;
mod local;
mod pattern;
mod pre_typechecker;
mod table;
mod typechecker;
mod types;
//mod psqlwrapper;

use crate::ast::Stmt;
use crate::local::{DbState, DbmsState};
use api::tcp_api::tcp_api;
use std::error::Error;
use std::io::Write;
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<!, Box<dyn Error>> {
    #[cfg(features = "wrapper")]
    unimplemented!();

    tcp_api("127.0.0.1:5432").await
}

#[cfg(features = "wrapper")]
async fn execute_query(
    input: &str,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    Ok(())
}

#[cfg(not(features = "wrapper"))]
async fn execute_query(
    input: &str,
    s: &DbmsState,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    // 1. parse
    use crate::grammar::StmtParser;

    let result: Result<Stmt, _> = StmtParser::new().parse(&input);
    let ast = match result {
        Ok(ast) => ast,
        Err(e) => return Ok(w.write_all(format!("{:#?}\n", e).as_bytes()).await?),
    };

    // 2. determine resources
    let request = pre_typechecker::get_resource_request(&ast);

    // 3. acquire resources
    let response = s.acquire_resources(request).await;
    let mut resources = match response {
        Ok(resources) => resources,
        Err(name) => {
            return Ok(w
                .write_all(format!("No such table: {}\n", name).as_bytes())
                .await?)
        }
    };
    let resources = resources.take().await;

    // 4. typecheck
    match typechecker::check_stmt(&ast, &resources) {
        Ok(()) => {}
        Err(e) => return Ok(w.write_all(format!("{:#?}\n", e).as_bytes()).await?),
    }

    // TODO:
    // 5. Maybe convert ast to some internal representation of a query
    // (See EXPLAIN in postgres/mysql)

    // 6. Execute query
    executor::execute_query(ast, s, resources, w).await
}

fn echo_ast(input: &str, w: &mut dyn Write) -> Result<(), Box<dyn Error>> {
    use crate::grammar::StmtParser;

    match StmtParser::new().parse(&input) {
        Ok(ast) => write!(w, "{:#?}\n", ast)?,
        Err(e) => write!(w, "{:#?}\n", e)?,
    }
    Ok(())
}
