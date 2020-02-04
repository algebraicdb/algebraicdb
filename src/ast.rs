
#[derive(Debug)]
pub enum Expr {
    Ident(String),
    Integer(i32),
    Str(String),
}

#[derive(Debug)]
pub struct Select {
    pub exprs: Vec<Expr>,
    pub from: Option<SelectFrom>,
}

#[derive(Debug)]
pub enum SelectFrom {
    Table(String),
    Select(Box<Select>),
}

#[derive(Debug)]
pub enum Stmt {
    Select(Select),
    Insert {
        into: String,
        columns: Vec<String>,
        values: Vec<Expr>,
    },
}

#[test]
fn select() {
    use crate::grammar::StmtParser;

    let examples = vec![ 
        r#"SELECT hello, ma, boi FROM feffe;"#,
        r#"SELECT hello, asdsad FROM adssad;"#,
        r#"INSERT INTO empty;"#,
        r#"INSERT INTO empty () VALUES ();"#,
        r#"INSERT INTO empty VALUES ();"#,
        r#"INSERT INTO feffes_mom (foo, bar, baz) VALUES (1,myself,"hello");"#,
        r#"SELECT bleh FROM (SELECT 3);"#,
    ];

    for ex in examples {
        println!("Trying to parse {}", ex);
        let out = StmtParser::new()
            .parse(ex)
            .expect("Parsing failed");

        println!("parsed: {:#?}", out);
    }

    assert!(false);
}
