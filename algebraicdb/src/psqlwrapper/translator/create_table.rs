use crate::table::*;
use crate::types::{Type, TypeId, TypeMap};

pub fn translate_create_table(name: &String, schema: &Schema, typemap: &TypeMap) -> String {
    return format!(
        "CREATE TABLE {} ({});",
        name,
        translate_schema(schema, &typemap)
    );
}

fn translate_schema(schema: &Schema, typemap: &TypeMap) -> String {
    schema
        .columns
        .iter()
        .map(|x| col_to_string(x, &typemap))
        .collect::<Vec<String>>()
        .join(",")
}

fn col_to_string(col: &(String, TypeId), typemap: &TypeMap) -> String {
    format!("{} {} NOT NULL", col.0, translate_typeid(&col.1, typemap))
}

fn translate_typeid(tid: &TypeId, typemap: &TypeMap) -> String {
    match typemap.get_by_id(*tid) {
        Type::Bool => String::from("BOOLEAN"),
        Type::Double => String::from("float(53)"),
        Type::Integer => String::from("INT"),
        Type::Sum(_) => String::from("json"),
    }
}
/*
#[cfg(test)]
pub mod test{
    use crate::grammar::StmtParser;
    use crate::ast::*;
    use super::translate_create_table;
    fn test_create_table(){
        let parser = StmtParser::new();

        let inputs = vec![
            r#"CREATE TABLE table (col Integer);"#,
            r#"CREATE TABLE table (col Sum(Thing(Integer)), col2 Integer);"#,
        ];
        let outputs = vec![
            r#"CREATE TABLE table (col Integer);"#,
            r#"CREATE TABLE table (col Sum(Thing(Integer)), col2 Integer);"#,
        ];

        let asts = inputs.iter().map(|x| parser.parse(x).unwrap()).collect::<Vec<Stmt>>();

        let tables = asts.iter().map(|x| match x {
            Stmt::CreateTable(ctable) => ctable
        }).collect::<Vec<CreateTable>>();


        for (table, out) in tables.iter().zip(outputs) {
            assert_eq!(translate_create_table(table, ), out);
        }
    }
}
*/
