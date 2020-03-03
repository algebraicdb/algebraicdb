use crate::ast::*;
use crate::local::{DbState, DbmsState, ResourcesGuard};
use crate::pattern::CompiledPattern;
use crate::pre_typechecker;
use crate::table::{Schema, Table};
use crate::typechecker;
use crate::types::{Type, TypeId, Value};
use std::error::Error;
use std::fmt::Write;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub(crate) async fn execute_query(
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
    execute_stmt(ast, s, resources, w).await
}

async fn execute_stmt(
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

            let p = if let Some(where_clause) = select.where_clause {
                CompiledPattern::compile(
                    &where_clause.items,
                    table.get_schema(),
                    &resources.type_map,
                )
            } else {
                CompiledPattern::compile(&[], table.get_schema(), &resources.type_map)
            };

            // Buffer for string formatting.
            // Avoids allocating a new string every time we want to write a string-formatted value.
            let mut fmt_buf = String::new();

            // Iterator of all bindings in pattern matches, chained with iterator of all columns.
            let rows = table
                .pattern_iter(&p, &resources.type_map)
                .map(|row| row.chain(table.get_row(row.row()).iter(&resources.type_map)));

            for row in rows {
                w.write_all(b"[").await?;
                let mut first = true;

                'selitems: for expr in &select.items {
                    match expr {
                        Expr::Ident(ident) => {
                            for (name, cell) in row.clone() {
                                if name == ident {
                                    if !first {
                                        w.write_all(b", ").await?;
                                    }
                                    first = false;

                                    write!(&mut fmt_buf, "{}", cell)?;
                                    w.write_all(fmt_buf.as_bytes()).await?;
                                    fmt_buf.clear();

                                    continue 'selitems;
                                }
                            }
                            panic!("No such binding: \"{}\"", ident);
                        }
                        e => unimplemented!("{:?}", e),
                    }
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
    Ok(())
}

async fn execute_insert(
    insert: Insert,
    mut resources: ResourcesGuard<'_, Table>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    let (table, types) = resources.write_table(&insert.table);

    let row_count = insert.rows.len();
    for row in insert.rows.into_iter() {
        let values: Vec<_> = row
            .into_iter()
            .map(|expr| execute_expr(expr))
            .collect();
        table.push_row(&values, &types);
    }

    w.write_all(format!("{} row(s) inserted\n", row_count).as_bytes())
        .await?;
    Ok(())
}

fn execute_expr(expr: Expr) -> Value {
    match expr {
        Expr::Value(v) => v,
        _ => unimplemented!("Non-value exprs"),
    }
}
