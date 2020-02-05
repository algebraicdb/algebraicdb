
#[derive(Debug)]
pub enum Expr<'a> {
    Ident(&'a str),
    Integer(i32),
    Str(&'a str),
    Equals(Box<Expr<'a>>, Box<Expr<'a>>),
    NotEquals(Box<Expr<'a>>, Box<Expr<'a>>),
    LessEquals(Box<Expr<'a>>, Box<Expr<'a>>),
    LessThan(Box<Expr<'a>>, Box<Expr<'a>>),
    GreaterThan(Box<Expr<'a>>, Box<Expr<'a>>),
    GreaterEquals(Box<Expr<'a>>, Box<Expr<'a>>),
}

#[derive(Debug)]
pub struct Ass<'a> {
    pub col: &'a str,
    pub expr: Expr<'a>,
}

#[derive(Debug)]
pub struct Select<'a> {
    pub exprs: Vec<Expr<'a>>,
    pub from: Option<SelectFrom<'a>>,
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug)]
pub enum SelectFrom<'a> {
    Table(&'a str),
    Select(Box<Select<'a>>),
    Join(Box<Join<'a>>),
}

#[derive(Debug)]
pub struct Join<'a>{
    pub table_a: SelectFrom<'a>,
    pub table_b: SelectFrom<'a>,
    pub join_type: JoinType,
    pub on_clause: Option<Expr<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType{
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug)]
pub struct WhereClause<'a>(pub Expr<'a>);

#[derive(Debug)]
pub struct Delete<'a> {
    pub table: &'a str,
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug)]
pub struct Insert<'a> {
    pub table: &'a str,
    pub columns: Vec<&'a str>,
    pub values: Vec<Expr<'a>>,
}

#[derive(Debug)]
pub enum Stmt<'a> {
    Select(Select<'a>),
    Insert(Insert<'a>),
    Delete(Delete<'a>),
    Update(Update<'a>),
}

#[derive(Debug)]
pub struct Update <'a> {
    pub table: &'a str,
    pub ass: Vec<Ass<'a>>,
    pub where_clause: Option<WhereClause<'a>>,
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
        r#"DELETE FROM feffe WHERE goblin;"#,
        r#"SELECT col FROM t1 LEFT JOIN t2;"#,
        r#"SELECT col FROM t1 RIGHT JOIN t2;"#,
        r#"SELECT col FROM t1 RIGHT OUTER JOIN t2;"#,
        r#"SELECT col FROM t1 FULL OUTER JOIN t2;"#,
        r#"UPDATE feffe SET hair_length = "short";"#,
        r#"UPDATE feffe SET hair_length = short WHERE hej=3;"#,
        r#"SELECT col FROM t1 LEFT JOIN t2 LEFT JOIN t3 ON 3=5;"#,
    ];

    let invalid_examples = vec![
        r#"SELECT hello, ma boi FROM feffe;"#,
        r#"SELECT hello FROM "sup dawg";"#,
        r#"INSERT INTO empty"#,
        r#"SELECT * FROM t1 INNER LEFT JOIN t2;"#,
        r#"SELECT * FROM t1 INNER OUTER JOIN t2;"#,
        r#"INSERT INTO empty (2) VALUES ();"#,
        r#"DELETE just;"#,
        r#"DELETE FROM just WHERE ;"#,
        r#"DELETE FROM just some more tables ;"#,
        r#"DELETE FROM now, with, commas ;"#,
        r#"UPDATE SET xxsxsxsxsxsxsxs=2 ;"#,
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

    assert!(false);
}
