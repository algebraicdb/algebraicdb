mod iter;

use self::iter::*;
use crate::ast::*;
use crate::error_message::ErrorMessage;
use crate::grammar::StmtParser;
use crate::pre_typechecker;
use crate::state::{DbmsState, Resource};
use crate::table::{Cell, Schema, TableData};
use crate::typechecker;
use crate::types::{Type, TypeId, TypeMap, Value};
use std::error::Error;
use std::fmt::Write;
use std::iter::empty;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Notify;

lazy_static! {
    static ref PARSER: StmtParser = StmtParser::new();
}

pub(crate) async fn execute_transaction(
    input: &str,
    transaction: Vec<Stmt>,
    s: &mut DbmsState,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    // 1. check for disallowed statements
    // NOTE: TODO: type-checking the entire transaction before-hand is made complicated
    // if we allow modifying table schemas or types during the transaction. For now we
    // disallow all those kinds of operations.
    if transaction.len() > 1 {
        for stmt in transaction.iter() {
            match stmt {
                // Statements that are currently allowed in a transaction:
                Stmt::Select(_) => {},
                Stmt::Insert(_) => {},
                Stmt::Delete(_) => {},
                Stmt::Update(_) => {},

                _ => panic!("NOT ALLOWED"),
            }
        }
    }

    // 2. determine resources
    // TODO: determine resources for ALL statements (in order)
    let request = pre_typechecker::get_transaction_resource_request(&transaction);

    // 3. acquire resources
    let response = s.acquire_resources(request).await;
    let resources = match response {
        Ok(resources) => resources,
        Err(name) => {
            return Ok(w
                .write_all(format!("no such table: \"{}\"\n", name).as_bytes())
                .await?)
        }
    };
    let mut type_map = resources.take_type_map().await;
    let mut table_schemas = resources.take_schemas().await;

    // 4. typecheck
    for stmt in transaction.iter() {
        match typechecker::check_stmt(stmt, &type_map, &table_schemas) {
            Ok(()) => {}
            Err(e) => {
                w.write_all(e.display(input).as_bytes()).await?;
                return Ok(());
            }
        }
    }

    // 5. write to wal
    // TODOOOOO

    let mut table_datas = resources.take_data().await;
    
    // 6. Execute query
    for stmt in transaction {
        execute_stmt(stmt, s, &mut type_map, &mut table_schemas, &mut table_datas, w).await?;
    }
    Ok(())
}

pub(crate) async fn execute_query(
    input: &str,
    s: &mut DbmsState,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    // 1. parse
    let result: Result<Stmt, _> = PARSER.parse(&input);
    let ast = match result {
        Ok(ast) => ast,
        Err(e) => {
            w.write_all(e.display(input).as_bytes()).await?;
            return Ok(());
        }
    };

    // 2. determine resources
    let request = pre_typechecker::get_resource_request(&ast);

    // 3. acquire resources
    let response = s.acquire_resources(request).await;
    let resources = match response {
        Ok(resources) => resources,
        Err(name) => {
            return Ok(w
                .write_all(format!("no such table: \"{}\"\n", name).as_bytes())
                .await?)
        }
    };
    let mut type_map = resources.take_type_map().await;
    let mut table_schemas = resources.take_schemas().await;

    // 4. typecheck
    match typechecker::check_stmt(&ast, &type_map, &table_schemas) {
        Ok(()) => {}
        Err(e) => {
            w.write_all(e.display(input).as_bytes()).await?;
            return Ok(());
        }
    }

    // 5. write to WAL
    let notify = log_stmt(&ast, s).await?;
    
    // 6. Fetch data resources
    let mut table_datas = resources.take_data().await;

    // 7. Notify the wal that we're done fetching locks
    if let Some(notify) = notify {
        notify.notify();
    }

    // 8. Execute query
    execute_stmt(ast, s, &mut type_map, &mut table_schemas, &mut table_datas, w).await
}

pub(crate) async fn execute_replay_query(
    ast: Stmt,
    s: &mut DbmsState,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    // 2. determine resources
    let request = pre_typechecker::get_resource_request(&ast);

    // 3. acquire resources
    let response = s.acquire_resources(request).await;
    let resources = match response {
        Ok(resources) => resources,
        Err(name) => {
            return Ok(w
                .write_all(format!("no such table: \"{}\"\n", name).as_bytes())
                .await?)
        }
    };

    let mut type_map = resources.take_type_map().await;
    let mut table_schemas = resources.take_schemas().await;
    let mut table_datas = resources.take_data().await;

    // ?. Execute query
    // TODO: Error checking
    execute_stmt(ast, s, &mut type_map, &mut table_schemas, &mut table_datas, w).await
}

async fn log_stmt(
    ast: &Stmt,
    s: &mut DbmsState,
) -> Result<Option<Arc<Notify>>, Box<dyn Error>> {
    Ok(if let Some(wal) = s.wal() {
        match &ast {
            Stmt::CreateTable(_)
            | Stmt::CreateType(_)
            | Stmt::Delete(_)
            | Stmt::Update(_)
            | Stmt::Drop(_)
            | Stmt::Insert(_) => Some(wal.write(&ast).await?),
            Stmt::Select(_) => None/* We're only reading, so no logging required*/
        }
    } else {
        None
    })
}

async fn execute_stmt(
    ast: Stmt,
    s: &mut DbmsState,
    type_map: &mut Resource<'_, TypeMap>,
    table_schemas: &mut HashMap<&str, Resource<'_, Schema>>,
    table_datas: &mut HashMap<&str, Resource<'_, TableData>>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    match ast {
        Stmt::CreateTable(create_table) => {
            execute_create_table(create_table, s, type_map, w).await
        }
        Stmt::CreateType(create_type) => execute_create_type(create_type, type_map, w).await,
        Stmt::Insert(insert) => execute_insert(insert, type_map, table_schemas, table_datas, w).await,
        Stmt::Select(select) => {
            let table = execute_select(&select, type_map, table_schemas, table_datas);
            print_table(table.iter(type_map), w).await
        }
        Stmt::Drop(drop) => execute_drop_table(drop, s, w).await,
        ast @ Stmt::Delete(_) => unimplemented!("Not implemented: {:?}", ast),
        ast @ Stmt::Update(_) => unimplemented!("Not implemented: {:?}", ast),
    }
}

async fn print_table(
    table: RowIter<'_>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    // Buffer for string formatting.
    // Avoids allocating a new string every time we want to write a string-formatted value.
    let mut fmt_buf = String::new();

    for row in table {
        w.write_all(b"[").await?;
        let mut first = true;

        for (_, cell) in row {
            if !first {
                w.write_all(b", ").await?;
            }
            first = false;

            write!(&mut fmt_buf, "{}", cell)?;
            w.write_all(fmt_buf.as_bytes()).await?;
            fmt_buf.clear();
        }

        w.write_all(b"]\n").await?;
    }
    Ok(())
}

fn full_table_scan<'a>(data: &'a TableData, schema: &'a Schema, type_map: &'a TypeMap) -> RowIter<'a> {
    let mut offset = 0;
    let bindings = schema
        .columns
        .iter()
        .map(|(name, type_id)| {
            let t = type_map.get_by_id(*type_id);
            let size = t.size_of(type_map);
            let cr = CellRef {
                source: &data.data,
                name,
                type_id: *type_id,
                offset,
                size,
                row_size: data.row_size,
            };

            offset += size;

            cr
        })
        .collect();

    RowIter {
        bindings,
        matches: Arc::new([]),
        type_map,
        row: Some(0),
    }
}

pub fn execute_select_from<'a>(
    from: &'a SelectFrom,
    type_map: &'a Resource<'a, TypeMap>,
    table_schemas: &'a HashMap<&'a str, Resource<'a, Schema>>,
    table_datas: &'a HashMap<&'a str, Resource<'a, TableData>>,
) -> Rows<'a> {
    match from {
        SelectFrom::Table(table_name) => {
            let schema = &table_schemas[table_name.as_str()];
            let data = &table_datas[table_name.as_str()];
            full_table_scan(data, schema, type_map).into()
        }
        SelectFrom::Select(select) => execute_select(select, type_map, table_schemas, table_datas).into(),
        SelectFrom::Join(join) => {
            match join.join_type {
                JoinType::Inner => { /* This is the only one supported for now...*/ }
                JoinType::LeftOuter => unimplemented!("Left Outer Join"),
                JoinType::RightOuter => unimplemented!("Right Outer Join"),
                JoinType::FullOuter => unimplemented!("Full Outer Join"),
            }

            let table_a = execute_select_from(&join.table_a, type_map, table_schemas, table_datas);
            let table_b = execute_select_from(&join.table_b, type_map, table_schemas, table_datas);

            let schema_out = table_a.schema().union(&table_b.schema());
            let mut data_out = TableData::new(&schema_out, type_map);

            let default_expr = Expr::Value(Spanned::from(Value::Bool(true)));
            let on_expr = join
                .on_clause
                .as_ref()
                .map(|e| &e.value)
                .unwrap_or(&default_expr);

            let mut row_buf: Vec<u8> = vec![];

            // The join algorithm is currently a basic n² loop.
            // We probably want to implement a more efficient one.

            let table_b = table_b.iter(type_map);
            for row_a in table_a.iter(type_map) {
                for row_b in table_b.clone() {
                    let bindings = row_a.clone().chain(row_b.clone());
                    let matches = match execute_expr(on_expr, bindings) {
                        Value::Bool(b) => b,
                        v => panic!("Tried joining on something other than a bool: {:?}", v),
                    };

                    if matches {
                        let row_a = row_a.clone();

                        for (_, cell) in row_a.chain(row_b) {
                            row_buf.extend_from_slice(cell.data);
                        }
                        data_out.push_row_bytes(&row_buf);
                        row_buf.clear();
                    }
                }
            }

            Rows::from((schema_out, data_out))
        }
    }
}

fn execute_select<'a>(
    select: &'a Select,
    type_map: &'a Resource<'a, TypeMap>,
    table_schemas: &'a HashMap<&'a str, Resource<'a, Schema>>,
    table_datas: &'a HashMap<&'a str, Resource<'a, TableData>>,
) -> Rows<'a> {
    let rows = match &select.from {
        Some(from) => execute_select_from(from, type_map, table_schemas, table_datas),
        None => unimplemented!("Selecting from nothing"),
    };

    let mut scan = rows;

    let where_items = select
        .where_clause
        .as_ref()
        .map(|wc| &wc.items[..])
        .unwrap_or(&[]);
    scan.apply_pattern(where_items, type_map);

    scan.select(&select.items);

    scan
}

async fn execute_create_table(
    create_table: CreateTable,
    s: &DbmsState,
    type_map: &Resource<'_, TypeMap>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    let columns: Vec<_> = create_table
        .columns
        .into_iter()
        .map(|(column_name, column_type)| {
            let t_id = type_map
                .get_id(&column_type)
                .expect("Type does not exist");
            (column_name.to_string(), t_id)
        })
        .collect();

    let schema = Schema::new(columns);
    let data = TableData::new(&schema, type_map);

    match s.create_table(create_table.table.to_string(), schema, data).await {
        Ok(()) => {
            w.write_all(format!("table created: \"{}\"\n", create_table.table).as_bytes())
                .await?
        }
        Err(()) => {
            w.write_all(format!("table already exists: \"{}\"\n", create_table.table).as_bytes())
                .await?
        }
    };
    Ok(())
}

async fn execute_create_type(
    create_type: CreateType,
    types: &mut Resource<'_, TypeMap>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    match create_type {
        CreateType::Variant { name, variants } => {
            let variant_types: Vec<_> = variants
                .into_iter()
                .map(|(constructor, subtypes)| {
                    let subtype_ids: Vec<TypeId> = subtypes
                        .iter()
                        .map(|type_name| types.get_id(type_name).unwrap())
                        .collect();

                    (constructor.to_string(), subtype_ids)
                })
                .collect();

            w.write_all(b"type ").await?;
            w.write_all(name.as_bytes()).await?;
            w.write_all(b" created\n").await?;
            types.insert(name.value, Type::Sum(variant_types));
        }
    }
    Ok(())
}

async fn execute_drop_table(
    drop: Drop,
    s: &DbmsState,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    match s.drop_table(&drop.table).await {
        Ok(()) => {
            w.write_all(format!("table dropped: \"{}\"\n", drop.table).as_bytes())
                .await?
        }
        Err(()) => {
            w.write_all(format!("no such table: \"{}\"\n", drop.table).as_bytes())
                .await?
        }
    }
    Ok(())
}

async fn execute_insert(
    insert: Insert,
    type_map: &Resource<'_, TypeMap>,
    table_schemas: &HashMap<&str, Resource<'_, Schema>>,
    table_datas: &mut HashMap<&str, Resource<'_, TableData>>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    match insert.from {
        // case !query
        InsertFrom::Values(rows) => {
            let schema = &table_schemas[insert.table.as_str()];
            let data = table_datas.get_mut(insert.table.as_str()).unwrap();
            let row_count = rows.len();
            let mut values = vec![];
            for row in rows.iter() {
                row.iter()
                    .map(|e| execute_expr(e, empty()))
                    .for_each(|v| values.push(v));
                data.push_row(&values, schema, &type_map);
                values.clear();
            }

            w.write_all(format!("{} row(s) inserted\n", row_count).as_bytes())
                .await?;
        }

        //case query
        InsertFrom::Select(select) => {
            let mut data = vec![];
            let mut row_count: usize = 0;
            for row in execute_select(&select, type_map, table_schemas, table_datas).iter(type_map) {
                row_count += 1;
                for (_, cell) in row {
                    data.extend_from_slice(&cell.data);
                }
            }

            let table = table_datas.get_mut(insert.table.as_str()).unwrap();
            table.data.extend_from_slice(&data);

            w.write_all(format!("{} row(s) inserted\n", row_count).as_bytes())
                .await?;
        }
    }

    Ok(())
}

fn execute_expr<'a, I>(expr: &Expr, mut bs: I) -> Value<'a>
where
    I: Iterator<Item = (&'a str, Cell<'a, 'a>)> + Clone,
{
    fn cmp<'a, I, F>(e1: &Expr, e2: &Expr, bs: I, f: F) -> Value<'a>
    where
        F: for<'l, 'r> FnOnce(&'l Value, &'r Value) -> bool,
        I: Iterator<Item = (&'a str, Cell<'a, 'a>)> + Clone,
    {
        let v1 = execute_expr(e1, bs.clone());
        let v2 = execute_expr(e2, bs);
        Value::Bool(f(&v1, &v2))
    }

    match expr {
        Expr::Value(v) => v.deep_clone(),
        Expr::Eql(box (e1, e2)) => cmp(e1, e2, bs, |v1, v2| v1 == v2),
        Expr::NEq(box (e1, e2)) => cmp(e1, e2, bs, |v1, v2| v1 != v2),
        Expr::LEq(box (e1, e2)) => cmp(e1, e2, bs, |v1, v2| v1 <= v2),
        Expr::LTh(box (e1, e2)) => cmp(e1, e2, bs, |v1, v2| v1 < v2),
        Expr::GTh(box (e1, e2)) => cmp(e1, e2, bs, |v1, v2| v1 > v2),
        Expr::GEq(box (e1, e2)) => cmp(e1, e2, bs, |v1, v2| v1 >= v2),
        Expr::And(box (e1, e2)) => match execute_expr(e1, bs.clone()) {
            Value::Bool(true) => execute_expr(e2, bs),
            Value::Bool(false) => Value::Bool(false),
            v => unreachable!("Non-boolean expression in Expr::And: {:?}", v),
        },
        Expr::Or(box (e1, e2)) => match execute_expr(e1, bs.clone()) {
            Value::Bool(true) => Value::Bool(true),
            Value::Bool(false) => execute_expr(e2, bs),
            v => unreachable!("Non-boolean expression in Expr::And: {:?}", v),
        },
        Expr::Ident(ident) => {
            let (_, cell) = bs
                .find(|(name, _)| name == ident.as_ref())
                .unwrap_or_else(|| unreachable!("Ident did not exist"));

            let t: &Type = cell.type_map.get_by_id(cell.type_id());

            t.from_bytes(&cell.data, cell.type_map)
                .expect("Deserializing cell failed")
        }
    }
}
