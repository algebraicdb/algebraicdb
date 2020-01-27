mod types;

use std::mem::size_of;

use crate::types::*;

/*
Thing = Var1 Int Int | Var2 Float

all_types: HashMap<String, Type>;
*/

fn main() {
    let example: Type = Type::Sum(vec![
        ("Var1".into(), vec![Type::Int]),
        ("Var2".into(), vec![]),
    ]);

    println!("Size of {:#?}: {}", example, example.size_of());
}
