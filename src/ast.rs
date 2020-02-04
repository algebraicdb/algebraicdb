
#[derive(Debug)]
pub enum Expr<'a> {
    Ident(&'a str),
    Integer(i32),
    Str(&'a str),
}

#[derive(Debug)]
pub struct Select<'a> {
    pub exprs: Vec<Expr<'a>>,
    pub from: Option<SelectFrom<'a>>,
}

#[derive(Debug)]
pub enum SelectFrom<'a> {
    Table(&'a str),
    Select(Box<Select<'a>>),
}

#[derive(Debug)]
pub enum Stmt<'a> {
    Select(Select<'a>),
    Insert {
        into: &'a str,
        columns: Vec<&'a str>,
        values: Vec<Expr<'a>>,
    },
}

#[test]
fn select() {
    use crate::grammar::StmtParser;

    let valid_examples = vec![
        r#"SELECT hello, ma, boi FROM feffe;"#,
        r#"SELECT hello, asdsad FROM adssad;"#,
        r#"INSERT INTO empty;"#,
        r#"INSERT INTO empty () VALUES ();"#,
        r#"INSERT INTO empty VALUES ();"#,
        r#"INSERT INTO feffes_mom (foo, bar, baz) VALUES (1, myself, "hello");"#,
        r#"SELECT bleh FROM (SELECT 3);"#,
    ];

    let invalid_examples = vec![
        r#"SELECT hello, ma boi FROM feffe;"#,
        r#"SELECT hello FROM "sup dawg";"#,
        r#"INSERT INTO empty"#,
        r#"INSERT INTO empty (2) VALUES ();"#,
    ];

    for ex in valid_examples {
        println!("Trying to parse {}", ex);
        let out = StmtParser::new()
            .parse(ex)
            .expect("Parsing failed");

        println!("parsed: {:#?}", out);
    }

    for ex in invalid_examples {
        println!("Trying to parse invalid input {}", ex);
        let _out = StmtParser::new()
            .parse(ex)
            .expect_err("Parsing succeeded when it should have failed");
    }

    //assert!(false);
}
