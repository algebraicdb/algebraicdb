use crate::ast::*;
use crate::local::{DbState, DbmsState, ResourcesGuard};
use crate::pattern::CompiledPattern;
use crate::table::{Schema, Table};
use crate::types::{Type, TypeId, Value};
use std::error::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};

struct Context {
    // TODO
}

impl Context {
    pub fn empty() -> Context {
        Context {}
    }
}

pub(crate) async fn execute_query(
    ast: Stmt,
    s: &DbmsState,
    resources: ResourcesGuard<'_, Table>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    match ast {
        Stmt::CreateTable(create_table) => {
            execute_create_table(create_table, s, resources, w).await
        }
        Stmt::CreateType(create_type) => execute_create_type(create_type, resources, w).await,
        Stmt::Insert(insert) => execute_insert(insert, resources, w).await,
        Stmt::Select(select) => execute_select(select, resources, w).await,
        _ => unimplemented!("Not implemented: {:?}", ast),
    }
}

async fn execute_select(
    select: Select,
    resources: ResourcesGuard<'_, Table>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    match select.from {
        Some(SelectFrom::Table(table_name)) => {
            let table = resources.read_table(&table_name);

            let p =
                CompiledPattern::compile(&select.items, table.get_schema(), &resources.type_map);

            for row in table.pattern_iter(&p, &resources.type_map) {
                w.write_all(b"[").await?;
                let mut first = true;
                for (_name, cell) in row {
                    if !first {
                        w.write_all(b", ").await?;
                    }
                    first = false;
                    w.write_all(format!("{}", cell).as_bytes()).await?;
                }
                w.write_all(b"]\n").await?;
            }
        }
        select_from => unimplemented!("Not implemented: {:?}", select_from),
    }

    Ok(())
}

async fn execute_create_table(
    create_table: CreateTable,
    s: &DbmsState,
    resources: ResourcesGuard<'_, Table>,
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
    let table = Table::new(schema, &resources.type_map);

    match s.create_table(create_table.table, table).await {
        Ok(()) => w.write_all(b"Table created\n").await?,
        Err(()) => w.write_all(b"Table already exists\n").await?,
    };
    Ok(())
}

async fn execute_create_type(
    create_type: CreateType,
    mut resources: ResourcesGuard<'_, Table>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    let types = &mut resources.type_map;

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

    w.write_all(b"Current types:\n").await?;
    for name in types.identifiers().keys() {
        w.write_all(b" ").await?;
        w.write_all(name.as_bytes()).await?;
        w.write_all(b"\n").await?;
    }
    Ok(())
}

async fn execute_insert(
    insert: Insert,
    mut resources: ResourcesGuard<'_, Table>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    let (table, types) = resources.write_table(&insert.table);

    let ctx = Context::empty();

    for row in insert.rows.into_iter() {
        let values: Vec<_> = row
            .into_iter()
            .map(|expr| execute_expr(expr, &ctx))
            .collect();
        table.push_row(&values, &types);
    }

    w.write_all(b"Row inserted\n").await?;
    w.write_all(format!("{:#?}\n", table).as_bytes()).await?;
    Ok(())
}

fn execute_expr(expr: Expr, _ctx: &Context) -> Value {
    match expr {
        Expr::Value(v) => v,
        _ => unimplemented!("Non-value exprs"),
    }
}
