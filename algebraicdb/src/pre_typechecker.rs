use crate::ast::*;
use crate::state::*;

pub fn get_resource_request(stmt: &Stmt) -> Acquire {
    Acquire {
        table_reqs: get_table_resource_requests(stmt),
        type_map_perms: get_type_map_resource_perm(stmt),
    }
}

fn get_type_map_resource_perm(stmt: &Stmt) -> RW {
    match stmt {
        Stmt::CreateType(_) => RW::Write,
        _ => RW::Read,
    }
}

fn get_table_resource_requests(stmt: &Stmt) -> Vec<TableRequest> {
    match stmt {
        Stmt::Select(sel) => get_option_select(&sel.from),
        Stmt::Update(upd) => vec![TableRequest {
            table: upd.table.to_string(),
            rw: RW::Write,
        }],
        Stmt::Insert(ins) => {
            let mut req = match &ins.from {
                InsertFrom::Values(_) => vec![],

                InsertFrom::Select(select) => get_option_select(&select.from),
            };

            req.sort();
            match req.binary_search_by(|r| r.table.as_str().cmp(&ins.table)) {
                Ok(i) => req[i].rw = RW::Write,
                Err(i) => req.insert(
                    i,
                    TableRequest {
                        table: ins.table.to_string(),
                        rw: RW::Write,
                    },
                ),
            }

            req
        }
        Stmt::Delete(del) => vec![TableRequest {
            table: del.table.to_string(),
            rw: RW::Write,
        }],
        Stmt::CreateType(_) => vec![],
        Stmt::CreateTable(_) => vec![],
        Stmt::Drop(drop) => vec![TableRequest {
            table: drop.table.to_string(),
            rw: RW::Write,
        }],
    }
}

fn get_option_select<'a>(sel: &'a Option<SelectFrom>) -> Vec<TableRequest> {
    match sel {
        Some(from) => get_select(&from),
        None => vec![],
    }
}

fn get_select(sel: &SelectFrom) -> Vec<TableRequest> {
    match &sel {
        SelectFrom::Select(nsel) => get_option_select(&nsel.from),
        SelectFrom::Table(tab) => vec![TableRequest {
            table: tab.to_string(),
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

    use super::*;
    use crate::grammar::StmtParser;

    #[test]
    fn test_get_permissions() {
        let parser = StmtParser::new();

        let ex1 = parser
            .parse(r#"SELECT hello, ma, boi FROM feffe;"#)
            .unwrap();
        let ex2 = parser
            .parse(r#"SELECT col FROM faffe LEFT JOIN feffe LEFT JOIN foffe ON 3=5;"#)
            .unwrap();

        let ex1r = get_table_resource_requests(&ex1);
        let ex2r = get_table_resource_requests(&ex2);

        println!("{}", ex2r[2].table);

        assert!(ex1r[0].table == "feffe" && ex1r[0].rw == RW::Read && ex1r.len() == 1);
        assert!(ex2r[0].table == "faffe" && ex2r[0].rw == RW::Read);
        assert!(ex2r[2].table == "foffe" && ex2r[2].rw == RW::Read);
        assert!(ex2r[1].table == "feffe" && ex2r[1].rw == RW::Read && ex2r.len() == 3)
    }
}
