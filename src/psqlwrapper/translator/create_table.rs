use crate::table::*;
use crate::ast::*;
use crate::types::{TypeMap, TypeId};


pub fn translate_create_table(create_table: CreateTable, schema: &Schema, typemap: &TypeMap) -> String  {
    return format!("CREATE TABLE {}", translate_table(schema, typemap))
}

fn translate_table(schema: &Schema, typemap: &TypeMap)-> String {
    schema.columns.iter().fold(String::from(""), )
}

fn col_to_string(col: Vec<(String, TypeId)>) -> String {
    
}

fn translate_typeid(tid: TypeId, typemap: &TypeMap) -> String {
}