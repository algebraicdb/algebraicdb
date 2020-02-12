use crate::ast::*;

#[derive(Clone, Copy)]
pub struct TablePermissions<'a> {
    name: &'a String,
    perm: Permission,
}

#[derive(Clone, Copy, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {

    use crate::ast::*;
    use crate::grammar::*;
    use crate::pre_typechecker::*;

    #[test]
    fn test_get_permissions() {
        let parser = StmtParser::new();

        let ex1 = parser
            .parse(r#"SELECT hello, ma, boi FROM feffe;"#)
            .unwrap();
        let ex2 = parser
            .parse(r#"SELECT col FROM faffe LEFT JOIN feffe LEFT JOIN foffe ON 3=5;"#)
            .unwrap();

        let ex1r = get_table_permissions(&ex1);
        let ex2r = get_table_permissions(&ex2);

        println!("{}", ex2r[2].name);

        assert!(ex1r[0].name == "feffe" && ex1r[0].perm == Permission::R && ex1r.len() == 1);
        assert!(ex2r[0].name == "faffe" && ex2r[0].perm == Permission::R);
        assert!(ex2r[2].name == "foffe" && ex2r[2].perm == Permission::R);
        assert!(ex2r[1].name == "feffe" && ex2r[1].perm == Permission::R && ex2r.len() == 3)
    }
}
