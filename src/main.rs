mod table;
mod types;
mod ast;
mod my_grammar;

use std::collections::HashMap;

use crate::types::*;
//mod types;

/*
Thing = Var1 Int Int | Var2 Float

all_types: HashMap<String, Type>;
*/

fn main() {
    let mut types = HashMap::new();
    types.insert(0, Type::Int);
    types.insert(
        0,
        Type::Sum(vec![("Var1".into(), vec![]), ("Var2".into(), vec![0])]),
    );

    println!("Size of {:#?}: {}", types[&1], types[&1].size_of(&types));
}
