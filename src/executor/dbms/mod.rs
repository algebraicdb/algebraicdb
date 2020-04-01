mod iter;

use crate::ast::*;
use crate::local::{DbState, DbmsState, ResourcesGuard};
use crate::pre_typechecker;
use crate::table::{Schema, Table, Cell};
use crate::typechecker;
use crate::types::{TypeMap, Type, TypeId, Value};
use std::error::Error;
use std::fmt::Write;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use std::sync::Arc;
use std::iter::empty;

use self::iter::*;

pub(crate) async fn execute_query(
    input: &str,
    s: &mut DbmsState,
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

    // 5. Execute query
    execute_stmt(ast, s, resources, w).await
}

async fn execute_stmt(
    ast: Stmt,
    s: &mut DbmsState,
    resources: ResourcesGuard<'_, Table>,
    w: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    match &ast {
        Stmt::CreateTable(_) |
        Stmt::CreateType(_) |
        Stmt::Delete(_) |
        Stmt::Update(_) |
        Stmt::Drop(_) |
        Stmt::Insert(_) => {
            s.wal().write(&ast).await;
        }
        Stmt::Select(_) => {/* We're only reading, so no logging required*/}
    }
    match ast {
        Stmt::CreateTable(create_table) => {
            execute_create_table(create_table, s, resources, w).await
        }
        Stmt::CreateType(create_type) => execute_create_type(create_type, resources, w).await,
        Stmt::Insert(insert) => execute_insert(insert, resources, w).await,
        Stmt::Select(select) => {
            let type_map = &resources.type_map;
            let table = execute_select(&select, &resources);
            print_table(table.iter(type_map), w).await

        },
        _ => unimplemented!("Not implemented: {:?}", ast),
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

fn full_table_scan<'a>(table: &'a Table, type_map: &'a TypeMap) -> RowIter<'a> {
    let mut offset = 0;
    let bindings = table.schema().columns.iter()
        .map(|(name, type_id)| {
            let t = type_map.get_by_id(*type_id);
            let size = t.size_of(type_map);
            let cr = CellRef {
                source: &table.data,
                name,
                type_id: *type_id,
                offset,
                size,
                row_size: table.row_size,
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

fn execute_select_from<'a>(
    from: &'a SelectFrom,
    resources: &'a ResourcesGuard<'a, Table>,
) -> Rows<'a> {
    let type_map = &resources.type_map;
    match from {
        SelectFrom::Table(table_name) => {
            let table = resources.read_table(&table_name);
            full_table_scan(table, type_map).into()
        }
        SelectFrom::Select(select) => {
            execute_select(select, resources).into()
        }
        SelectFrom::Join(join) => {

            match join.join_type {
                JoinType::Inner => {/* This is the only one supported for now...*/},
                JoinType::LeftOuter => unimplemented!("Left Outer Join"),
                JoinType::RightOuter => unimplemented!("Right Outer Join"),
                JoinType::FullOuter => unimplemented!("Full Outer Join"),
            }

            let table_a = execute_select_from(&join.table_a, resources);
            let table_b = execute_select_from(&join.table_b, resources);

            let mut table_out = Table::new(table_a.schema().union(&table_b.schema()), type_map);

            let on_expr = join.on_clause.as_ref().unwrap_or(&Expr::Value(Value::Bool(true)));

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
                        table_out.push_row_bytes(&row_buf);
                        row_buf.clear();
                    }
                }
            }

            Rows::from(table_out)
        }
    }
}

fn execute_select<'a>(
    select: &'a Select,
    resources: &'a ResourcesGuard<'a, Table>,
) -> Rows<'a> {
    let type_map = &resources.type_map;

    let rows = match &select.from {
        Some(from) => execute_select_from(from, resources),
        None => unimplemented!("Selecting from nothing"),
    };

    let mut scan = rows;

    let where_items = select.where_clause.as_ref().map(|wc| &wc.items[..]).unwrap_or(&[]);
    scan.apply_pattern(where_items, type_map);

    scan.select(&select.items);

    scan
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
    match insert.from {
        // case !query
        InsertFrom::Values(rows) => {
            let (table, type_map) = resources.write_table(&insert.table);
            let row_count = rows.len();
            let mut values = vec![];
            for row in rows.into_iter() {
                row.iter().map(|e| execute_expr(e, empty())).for_each(|v| values.push(v));
                table.push_row(&values, &type_map);
                values.clear();
            }

            w.write_all(format!("{} row(s) inserted\n", row_count).as_bytes())
                .await?;
        }

        //case query
        InsertFrom::Select(select) => {
            let mut data = vec![];
            let mut row_count = 0;
            for row in execute_select(&select, &resources).iter(&resources.type_map) {
                row_count += 1;
                for (_, cell) in row {
                    data.extend_from_slice(&cell.data);
                }
            }

            let (table, _) = resources.write_table(&insert.table);
            table.data.extend_from_slice(&data);

            w.write_all(format!("{} row(s) inserted\n", row_count).as_bytes())
                .await?;
        }
    }

    Ok(())
}

fn execute_expr<'a, I>(expr: &Expr, mut bs: I) -> Value
where I: Iterator<Item = (&'a str, Cell<'a, 'a>)> + Clone,
{
    fn cmp<'a, I, F>(e1: &Expr, e2: &Expr, bs: I, f: F) -> Value
    where F: FnOnce(&Value, &Value) -> bool,
          I: Iterator<Item = (&'a str, Cell<'a, 'a>)> + Clone,
    {
        let v1 = execute_expr(e1, bs.clone());
        let v2 = execute_expr(e2, bs);
        Value::Bool(f(&v1, &v2))
    }

    match expr {
        Expr::Value(v) => v.clone(),
        Expr::Equals(e1, e2) => cmp(e1, e2, bs, PartialEq::eq),
        Expr::NotEquals(e1, e2) => cmp(e1, e2, bs, PartialEq::ne),
        Expr::LessEquals(e1, e2) => cmp(e1, e2, bs, |v1, v2| v1 <= v2),
        Expr::LessThan(e1, e2) => cmp(e1, e2, bs, |v1, v2| v1 < v2),
        Expr::GreaterThan(e1, e2) => cmp(e1, e2, bs, |v1, v2| v1 > v2),
        Expr::GreaterEquals(e1, e2) => cmp(e1, e2, bs, |v1, v2| v1 >= v2),
        Expr::And(e1, e2) => {
            match execute_expr(e1, bs.clone()) {
                Value::Bool(true) => execute_expr(e2, bs),
                Value::Bool(false) => Value::Bool(false),
                v => unreachable!("Non-boolean expression in Expr::And: {:?}", v),
            }
        }
        Expr::Or(e1, e2) => {
            match execute_expr(e1, bs.clone()) {
                Value::Bool(true) => Value::Bool(true),
                Value::Bool(false) => execute_expr(e2, bs),
                v => unreachable!("Non-boolean expression in Expr::And: {:?}", v),
            }
        }
        Expr::Ident(ident) => {
            let (_, cell) = bs.find(|(name, _)| name == ident)
                .unwrap_or_else(|| unreachable!("Ident did not exist"));

            let t: &Type = cell.type_map.get_by_id(cell.type_id());

            t.from_bytes(&cell.data, cell.type_map)
                .expect("Deserializing cell failed")
        }
    }
}
