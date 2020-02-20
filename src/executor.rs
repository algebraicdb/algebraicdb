use crate::ast::*;
use crate::global::{send_request, Request, ResourcesGuard, Response};
use crate::pattern::CompiledPattern;
use crate::table::{Schema, Table};
use crate::types::{Type, TypeId, Value};
use std::error::Error;
use std::io::Write;

struct Context {
    // TODO
}

impl Context {
    pub fn empty() -> Context {
        Context {}
    }
}

pub(crate) fn execute_query(
    ast: Stmt,
    resources: ResourcesGuard,
    w: &mut dyn Write,
) -> Result<(), Box<dyn Error>> {
    match ast {
        Stmt::CreateTable(create_table) => execute_create_table(create_table, resources, w),
        Stmt::CreateType(create_type) => execute_create_type(create_type, resources, w),
        Stmt::Insert(insert) => execute_insert(insert, resources, w),
        Stmt::Select(select) => execute_select(select, resources, w),
        _ => unimplemented!("Not implemented: {:?}", ast),
    }
}

fn execute_select(
    select: Select,
    resources: ResourcesGuard,
    w: &mut dyn Write,
) -> Result<(), Box<dyn Error>> {
    match select.from {
        Some(SelectFrom::Table(table_name)) => {
            let table = resources.read_table(&table_name);

            let p =
                CompiledPattern::compile(&select.items, table.get_schema(), &resources.type_map);

            for row in table.pattern_iter(&p, &resources.type_map) {
                write!(w, "[")?;
                let mut first = true;
                for (_name, cell) in row {
                    if !first {
                        write!(w, ", ")?;
                    }
                    first = false;
                    write!(w, "{}", cell)?;
                }
                write!(w, "]\n")?;
            }
        }
        select_from => unimplemented!("Not implemented: {:?}", select_from),
    }

    Ok(())
}

fn execute_create_table(
    create_table: CreateTable,
    resources: ResourcesGuard,
    w: &mut dyn Write,
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

    let request = Request::CreateTable(create_table.table, table);

    match send_request(request) {
        Response::TableCreated => write!(w, "Table created\n")?,
        Response::TableAlreadyExists => write!(w, "Table already exists\n")?,
        _ => unreachable!(),
    };
    Ok(())
}

fn execute_create_type(
    create_type: CreateType,
    mut resources: ResourcesGuard,
    w: &mut dyn Write,
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

            write!(w, "Type {} created.\n", name)?;
            types.insert(name, Type::Sum(variant_types));
        }
    }

    write!(w, "Current types:\n")?;
    for name in types.identifiers().keys() {
        write!(w, "  {}\n", name)?;
    }
    Ok(())
}

fn execute_insert(
    insert: Insert,
    mut resources: ResourcesGuard,
    w: &mut dyn Write,
) -> Result<(), Box<dyn Error>> {
    let (table, types) = resources.write_table(&insert.table);

    let ctx = Context::empty();

    let values: Vec<_> = insert
        .values
        .into_iter()
        .map(|expr| execute_expr(expr, &ctx))
        .collect();

    table.push_row(&values, &types);

    write!(w, "Row inserted\n")?;
    write!(w, "{:#?}\n", table)?;
    Ok(())
}

fn execute_expr(expr: Expr, _ctx: &Context) -> Value {
    match expr {
        Expr::Value(v) => v,
        _ => unimplemented!("Non-value exprs"),
    }
}
