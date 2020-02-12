// Wheres ska evalueras till bools
// 
//
use crate::ast::*;
use crate::types::*;
use crate::table::*;




//                                                                        TODO
pub fn check_stmt(stmt: &Stmt, table: &Table, types: &TypeMap) -> Result<(), &'static str>{
    match stmt {
        Stmt::Select(sel) => {
            check_select(sel, table, types);

        }
        Stmt::Delete(del) => {
            
        }
        Stmt::Update(upd) => {

        }
        Stmt::Insert(ins) =>  {

        }
    }

    Ok(())

}

fn check_select(sel: &Select, table: &Table, types: &TypeMap) -> Result<Vec<Column>, &'static str> {
    match &sel.from {
        Some(SelectFrom::Select(nsel)) => check_select(&nsel, table, types),
        Some(SelectFrom::Table(tab)) => unimplemented!("FEFFE FIX"),
        Some(SelectFrom::Join(jon)) => unimplemented!("TUX FIX"),
        None => Err("Select from nothing not allowed."),
    }
}


