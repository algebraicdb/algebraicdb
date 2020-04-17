use crate::ast::*;
use crate::state::*;

pub fn get_resource_request(stmt: &Stmt) -> Acquire {
    get_table_resource_requests(stmt).into_acquire(get_type_map_resource_perm(stmt))
}

pub fn get_transaction_resource_request(transaction: &[Stmt]) -> Acquire {
    get_transaction_resource_requests(transaction).into_acquire(RW::Read)
}

fn get_type_map_resource_perm(stmt: &Stmt) -> RW {
    match stmt {
        Stmt::CreateType(_) => RW::Write,
        _ => RW::Read,
    }
}

#[derive(Default)]
struct Request {
    schema_reqs: Vec<TableRequest>,
    data_reqs: Vec<TableRequest>,
}

impl Request {
    fn push_req(&mut self, table: &str, schema_perm: RW, data_perm: RW) {
        self.push_schema_req(table, schema_perm);
        self.push_data_req(table, data_perm);
    }

    fn push_schema_req(&mut self, table: &str, rw: RW) {
        match self.schema_reqs.binary_search_by(|r| r.table.as_str().cmp(table)) {
            Ok(_) if rw == RW::Read => {}
            Ok(i) => self.schema_reqs[i].rw = RW::Write,
            Err(i) => self.schema_reqs.insert(
                i,
                TableRequest {
                    table: table.to_string(),
                    rw,
                },
            ),
        }
    }

    fn push_data_req(&mut self, table: &str, rw: RW) {
        match self.data_reqs.binary_search_by(|r| r.table.as_str().cmp(table)) {
            Ok(_) if rw == RW::Read => {}
            Ok(i) => self.data_reqs[i].rw = RW::Write,
            Err(i) => self.data_reqs.insert(
                i,
                TableRequest {
                    table: table.to_string(),
                    rw,
                },
            ),
        }
    }

    fn into_acquire(self, type_map_perms: RW) -> Acquire {
        Acquire {
            schema_reqs: self.schema_reqs,
            data_reqs: self.data_reqs,
            type_map_perms,
        }
    }
}

fn get_transaction_resource_requests(transaction: &[Stmt]) -> Request {
    let mut request = Request::default();
    for stmt in transaction.iter() {
        get_stmt(&mut request, stmt);
    }
    request
}

fn get_table_resource_requests(stmt: &Stmt) -> Request {
    let mut request = Request::default();
    get_stmt(&mut request, stmt);
    request
}

fn get_stmt(request: &mut Request, stmt: &Stmt) {
    match stmt {
        Stmt::Select(sel) => get_option_select(request, &sel.from),
        Stmt::Update(upd) => request.push_req(&upd.table, RW::Read, RW::Write),
        Stmt::Insert(ins) => {
            match &ins.from {
                InsertFrom::Values(_) => {}
                InsertFrom::Select(select) => get_option_select(request, &select.from),
            }
            request.push_req(&ins.table, RW::Read, RW::Write);
        }
        Stmt::Delete(del) => request.push_req(&del.table, RW::Read, RW::Write),
        Stmt::CreateType(_) => {},
        Stmt::CreateTable(_) => {},
        Stmt::Drop(drop) => request.push_req(&drop.table, RW::Write, RW::Write), // TODO: do we even need this?
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
        SelectFrom::Table(tab) => request.push_req(tab, RW::Read, RW::Read),
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
