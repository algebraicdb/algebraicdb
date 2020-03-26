use crate::ast::*;
use crate::local::{DbState, ResourcesGuard, WrapperState};
use crate::pattern::CompiledPattern;
use crate::pre_typechecker;
use crate::psqlwrapper::translator::*;
use crate::table::Schema;
use crate::typechecker;
use crate::types::{Type, TypeId, Value};
use std::error::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_postgres::{SimpleQueryMessage, SimpleQueryRow};
struct Context {
    // TODO
}

impl Context {
    pub fn empty() -> Context {
        Context {}
    }
}

pub(crate) async fn execute_query(
    input: &str,
    s: &WrapperState,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    // 1. Parse
    use crate::grammar::StmtParser;

    let result: Result<Stmt, _> = StmtParser::new().parse(&input);

    let ast = match result {
        Ok(ast) => ast,
        Err(e) => return Ok(w.write_all(format!("{:#?}\n", e).as_bytes()).await?),
    };

    // 2. pre-tc
    let request = pre_typechecker::get_resource_request(&ast);

    // 3. Get schema access

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
    // 4. TC

    match typechecker::check_stmt(&ast, &resources) {
        Ok(()) => {}
        Err(e) => return Ok(w.write_all(format!("{:#?}\n", e).as_bytes()).await?),
    }

    // 5. Translate and excute query
    execute_stmt(ast, s, resources, w).await
}

async fn execute_stmt(
    ast: Stmt,
    s: &WrapperState,
    resources: ResourcesGuard<'_, Schema>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    match ast {
        Stmt::CreateTable(create_table) => {
            execute_create_table(create_table, s, resources, w).await
        }
        Stmt::CreateType(create_type) => execute_create_type(create_type, resources, w).await,
        Stmt::Insert(insert) => execute_insert(insert, resources, w, &s).await,
        Stmt::Select(select) => execute_select(select, &s, resources, w).await,
        _ => unimplemented!("Not implemented: {:?}", ast),
    }
}

async fn execute_select(
    select: Select,
    s: &WrapperState,
    resources: ResourcesGuard<'_, Schema>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    let rows: Vec<SimpleQueryMessage> = s
        .client
        .simple_query(translate_select(&select).as_str())
        .await
        .unwrap();

    w.write_all(
        rows.iter().fold(String::new(),|a, b| format!("{}\n{}", a, match b {
            SimpleQueryMessage::Row(r) => String::from((0..r.len()).map(|c| match r.get(c){Some(st) => st, None => ""}).collect::<Vec<&str>>().join(",")),
            SimpleQueryMessage::CommandComplete(r) => {dbg!(r); String::new()},
            _ => unreachable!(),
        })).as_bytes()
    )
    .await?;
    Ok(())
}

async fn execute_create_table(
    create_table: CreateTable,
    s: &WrapperState,
    resources: ResourcesGuard<'_, Schema>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    let columns: Vec<_> = create_table
        .columns
        .into_iter()
        .map(|(column_name, column_type)| {
            let t_id = resources
                .type_map
                .get_id(&column_type)
                .expect("Type does not exist");
            (column_name, t_id)
        })
        .collect();

    let schema = Schema::new(columns);
    s.create_table(create_table.table, schema).await.unwrap();
    w.write_all("created table\n".as_bytes()).await?;
    Ok(())
}

async fn execute_create_type(
    create_type: CreateType,
    mut resources: ResourcesGuard<'_, Schema>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    let types = &mut resources.type_map;
    // TODO: PSQL
    match create_type {
        CreateType::Variant(name, variants) => {
            let variant_types: Vec<_> = variants
                .into_iter()
                .map(|(constructor, subtypes)| {
                    let subtype_ids: Vec<TypeId> = subtypes
                        .iter()
                        .map(|type_name| types.get_id(type_name).unwrap())
                        .collect();

                    (constructor, subtype_ids)
                })
                .collect();

            w.write_all(b"Type ").await?;
            w.write_all(name.as_bytes()).await?;
            w.write_all(b" created \n").await?;
            types.insert(name, Type::Sum(variant_types));
        }
    }
    Ok(())
}

async fn execute_insert(
    insert: Insert,
    mut resources: ResourcesGuard<'_, Schema>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
    s: &WrapperState,
) -> Result<(), Box<dyn Error>> {
    let (table, types) = resources.write_table(&insert.table);
    let row_count = insert.rows.len();

    s.client
        .execute(translate_insert(&insert).as_str(), &[])
        .await
        .unwrap();

    w.write_all(format!("{} row(s) inserted\n", row_count).as_bytes())
        .await?;
    Ok(())
}

fn execute_expr(expr: Expr, _ctx: &Context) -> Value {
    match expr {
        Expr::Value(v) => v,
        _ => unimplemented!("Non-value exprs"),
    }
}