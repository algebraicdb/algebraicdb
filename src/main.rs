#![feature(str_strip)]
#![feature(box_syntax)]

mod table;
mod types;
mod ast;
mod grammar;
mod pattern;
mod primitive;

use std::collections::HashMap;
use crate::types::*;

fn main() {
    let mut types = HashMap::new();
    types.insert(0, Type::Integer);
    types.insert(
        0,
        Type::Sum(vec![("Var1".into(), vec![]), ("Var2".into(), vec![0])]),
    );

    println!("Size of {:#?}: {}", types[&1], types[&1].size_of(&types));
}
