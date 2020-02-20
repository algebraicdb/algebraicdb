use crate::ast::*;
use crate::global::{send_request, Request, ResourcesGuard, Response};
use crate::pattern::CompiledPattern;
use crate::table::{Schema, Table};
use crate::types::{Type, TypeId, Value};

struct Context {
    // TODO
}

impl Context {
    pub fn empty() -> Context {
        Context {}
    }
}

pub(crate) fn execute_query(ast: Stmt, resources: ResourcesGuard) -> String {
    match ast {
        Stmt::CreateTable(create_table) => execute_create_table(create_table, resources),
        Stmt::CreateType(create_type) => execute_create_type(create_type, resources),
        Stmt::Insert(insert) => execute_insert(insert, resources),
        Stmt::Select(select) => execute_select(select, resources),
        _ => unimplemented!("Not implemented: {:?}", ast),
    }
}

// SELECT a FROM table
// SELECT a: a FROM table
fn execute_select(select: Select, resources: ResourcesGuard) -> String {
    match select.from {
        Some(SelectFrom::Table(table_name)) => {
            let table = resources.read_table(&table_name);

            let p =
                CompiledPattern::compile(&select.items, table.get_schema(), &resources.type_map);

            let mut output = String::new();

            for row in table.pattern_iter(&p, &resources.type_map) {
                for (name, cell) in row {
                    output.push_str(&format!("{}: {:?} ", name, cell.data))
                }
                output.push_str("\n");
            }

            output
        }
        select_from => unimplemented!("Not implemented: {:?}", select_from),
    }
}

fn execute_create_table(create_table: CreateTable, resources: ResourcesGuard) -> String {
    let columns: Vec<_> = create_table
        .columns
        .into_iter()
        .map(|(column_name, column_type)| {
            let t_id = resources
                .type_map
                .get_id(&column_type)
                .expect("Type does not exists");
            (column_name, t_id)
        })
        .collect();

    let schema = Schema::new(columns);
    let table = Table::new(schema, &resources.type_map);

    let request = Request::CreateTable(create_table.table, table);

    match send_request(request) {
        Response::TableCreated => "Table created\n",
        Response::TableAlreadyExists => "Table already exists\n",
        _ => unreachable!(),
    }
    .to_string()
}

fn execute_create_type(create_type: CreateType, mut resources: ResourcesGuard) -> String {
    let types = &mut resources.type_map;

    let mut output;

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

            output = format!("Type {} created.\n", name);
            types.insert(name, Type::Sum(variant_types));
        }
    }

    output.push_str("Current types:\n");
    for name in types.identifiers().keys() {
        output.push_str("  ");
        output.push_str(name);
        output.push_str("\n");
    }
    output
}

fn execute_insert(insert: Insert, mut resources: ResourcesGuard) -> String {
    let (table, types) = resources.write_table(&insert.table);

    let ctx = Context::empty();

    let values: Vec<_> = insert
        .values
        .into_iter()
        .map(|expr| execute_expr(expr, &ctx))
        .collect();

    table.push_row(&values, &types);

    //"Row inserted".to_string()
    format!("{:#?}", table)
}

fn execute_expr(expr: Expr, _ctx: &Context) -> Value {
    match expr {
        Expr::Value(v) => v,
        _ => unimplemented!("Non-value exprs"),
    }
}