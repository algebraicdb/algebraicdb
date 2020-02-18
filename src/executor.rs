use crate::ast::*;
use crate::global::{send_request, ResourcesGuard, Request, Response};
use crate::table::{Table, Schema};

pub fn execute_query(ast: Stmt, resources: ResourcesGuard) -> String {
    match ast {
        Stmt::CreateTable(create_table) => execute_create_table(create_table, resources),
        _ => unimplemented!("Not implemented: {:?}", ast),
    }
}

fn execute_create_table(create_table: CreateTable, resources: ResourcesGuard) -> String {
    let columns: Vec<_> = create_table.columns
        .into_iter()
        .map(|(column_name, column_type)| {
            let t_id = resources.types.get_id(&column_type).expect("Type does not exists");
            (column_name, t_id)
        })
        .collect();

    let schema = Schema::new(columns);
    let table = Table::new(schema, &resources.types);

    let request = Request::CreateTable(create_table.table, table);

    match send_request(request) {
        Response::TableCreated => "Table created\n",
        Response::TableAlreadyExists => "Table already exists\n",
        _ => unreachable!(),
    }.to_string()
}