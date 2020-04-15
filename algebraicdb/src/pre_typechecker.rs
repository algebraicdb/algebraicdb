use crate::ast::*;
use crate::state::*;

pub fn get_resource_request(stmt: &Stmt) -> Acquire {
    Acquire {
        table_reqs: get_table_resource_requests(stmt),
        type_map_perms: get_type_map_resource_perm(stmt),
    }
}

pub fn get_transaction_resource_request(transaction: &[Stmt]) -> Acquire {
    Acquire {
        table_reqs: get_transaction_resource_requests(transaction),
        type_map_perms: RW::Read,
    }
}

fn get_type_map_resource_perm(stmt: &Stmt) -> RW {
    match stmt {
        Stmt::CreateType(_) => RW::Write,
        _ => RW::Read,
    }
}

struct Request(Vec<TableRequest>);

impl Request {
    fn push(&mut self, table: &str, rw: RW) {
        match self.0.binary_search_by(|r| r.table.as_str().cmp(table)) {
            Ok(_) if rw == RW::Read => {}
            Ok(i) => self.0[i].rw = RW::Write,
            Err(i) => self.0.insert(
                i,
                TableRequest {
                    table: table.to_string(),
                    rw,
                },
            ),
        }
    }
}

fn get_transaction_resource_requests(transaction: &[Stmt]) -> Vec<TableRequest> {
    let mut request = Request(vec![]);
    for stmt in transaction.iter() {
        get_stmt(&mut request, stmt);
    }
    request.0
}

fn get_table_resource_requests(stmt: &Stmt) -> Vec<TableRequest> {
    let mut request = Request(vec![]);
    get_stmt(&mut request, stmt);
    request.0
}

fn get_stmt(request: &mut Request, stmt: &Stmt) {
    match stmt {
        Stmt::Select(sel) => get_option_select(request, &sel.from),
        Stmt::Update(upd) => request.push(&upd.table, RW::Write),
        Stmt::Insert(ins) => {
            match &ins.from {
                InsertFrom::Values(_) => {}
                InsertFrom::Select(select) => get_option_select(request, &select.from),
            }

            request.push(&ins.table, RW::Write);
        }
        Stmt::Delete(del) => request.push(&del.table, RW::Write),
        Stmt::CreateType(_) => {},
        Stmt::CreateTable(_) => {},
        Stmt::Drop(drop) => request.push(&drop.table, RW::Write),
    }
}

fn get_option_select<'a>(request: &mut Request, sel: &'a Option<SelectFrom>) {
    match sel {
        Some(from) => get_select(request, &from),
        None => {},
    }
}

fn get_select(request: &mut Request, sel: &SelectFrom) {
    match &sel {
        SelectFrom::Select(nsel) => get_option_select(request, &nsel.from),
        SelectFrom::Table(tab) => request.push(tab, RW::Read),
        SelectFrom::Join(jon) => {
            get_select(request, &jon.table_a);
            get_select(request, &jon.table_b);
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
