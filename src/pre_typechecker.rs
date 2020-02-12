use crate::ast::*;

#[derive(Clone, Copy)]
pub struct TablePermissions<'a> {
    name: &'a String,
    perm: Permission,
}

#[derive(Clone, Copy)]
pub enum Permission {
    R,
    RW,
}

pub fn get_table_permissions(stmt: &Stmt) -> Vec<TablePermissions> {
    match stmt {
        Stmt::Select(sel) => get_option_select(&sel.from),
        Stmt::Update(upd) => vec![TablePermissions {
            name: &upd.table,
            perm: Permission::RW,
        }],
        Stmt::Insert(ins) => vec![TablePermissions {
            name: &ins.table,
            perm: Permission::RW,
        }],
        Stmt::Delete(del) => vec![TablePermissions {
            name: &del.table,
            perm: Permission::RW,
        }],
        Stmt::CreateType(_) => vec![],
    }
}

fn get_option_select<'a>(sel: &'a Option<SelectFrom>) -> Vec<TablePermissions<'a>> {
    match sel {
        Some(from) => get_select(&from),
        None => vec![],
    }
}

fn get_select<'a>(sel: &'a SelectFrom) -> Vec<TablePermissions<'a>> {
    match &sel {
        SelectFrom::Select(nsel) => get_option_select(&nsel.from),
        SelectFrom::Table(tab) => vec![TablePermissions {
            name: &tab,
            perm: Permission::R,
        }],
        SelectFrom::Join(jon) => {
            let mut vec = get_select(&jon.table_a);
            vec.extend(get_select(&jon.table_b));
            vec
        }
    }
}
