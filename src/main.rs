#![feature(str_strip)]
#![feature(never_type)]
#![feature(box_syntax)]

#[macro_use]
extern crate lazy_static;

mod ast;
mod global;
mod grammar;
mod pattern;
mod pre_typechecker;
mod primitive;
mod table;
mod typechecker;
mod types;

use crate::types::*;
use std::collections::HashMap;

fn main() {
    let mut types = HashMap::new();
    types.insert(0, Type::Integer);
    types.insert(
        0,
        Type::Sum(vec![("Var1".into(), vec![]), ("Var2".into(), vec![0])]),
    );

    println!("Size of {:#?}: {}", types[&1], types[&1].size_of(&types));
}
