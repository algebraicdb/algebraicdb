use crate::table::*;
use crate::ast::*;
use crate::types::{TypeMap, TypeId, Type};


pub fn translate_create_table(create_table: CreateTable, schema: &Schema, typemap: &TypeMap) -> String  {
    return format!("CREATE TABLE {};", translate_table(schema, typemap))
}

fn translate_table(schema: &Schema, typemap: &TypeMap)-> String {
    schema.columns.iter().map(|x| col_to_string(x, typemap)).collect::<Vec<String>>().join(",")
}

fn col_to_string(col: &(String, TypeId), typemap: &TypeMap) -> String {
    format!("{} {} NOT NULL", col.0, translate_typeid(&col.1, typemap))
}

fn translate_typeid(tid: &TypeId, typemap: &TypeMap) -> String {
    match typemap.get_by_id(*tid) {
        Type::Bool => String::from("BOOLEAN"),
        Type::Double => String::from("float64"),
        Type::Integer => String::from("INT"),
        Type::Sum(_) => String::from("json"),
    }
    
}