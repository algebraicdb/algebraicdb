use crate::ast::*;
use crate::global::{TableRequest, RW};

pub fn get_table_permissions(stmt: &Stmt) -> Vec<TableRequest> {
    match stmt {
        Stmt::Select(sel) => get_option_select(&sel.from),
        Stmt::Update(upd) => vec![TableRequest {
            table: upd.table.clone(),
            rw: RW::Write,
        }],
        Stmt::Insert(ins) => vec![TableRequest {
            table: ins.table.clone(),
            rw: RW::Write,
        }],
        Stmt::Delete(del) => vec![TableRequest {
            table: del.table.clone(),
            rw: RW::Write,
        }],
        Stmt::CreateType(_) => vec![],
    }
}

fn get_option_select<'a>(sel: &'a Option<SelectFrom>) -> Vec<TableRequest> {
    match sel {
        Some(from) => get_select(&from),
        None => vec![],
    }
}

fn get_select<'a>(sel: &'a SelectFrom) -> Vec<TableRequest> {
    match &sel {
        SelectFrom::Select(nsel) => get_option_select(&nsel.from),
        SelectFrom::Table(tab) => vec![TableRequest {
            table: tab.clone(),
            rw: RW::Read,
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

        println!("{}", ex2r[2].table);

        assert!(ex1r[0].table == "feffe" && ex1r[0].rw == RW::Read && ex1r.len() == 1);
        assert!(ex2r[0].table == "faffe" && ex2r[0].rw == RW::Read);
        assert!(ex2r[2].table == "foffe" && ex2r[2].rw == RW::Read);
        assert!(ex2r[1].table == "feffe" && ex2r[1].rw == RW::Read && ex2r.len() == 3)
    }
}
