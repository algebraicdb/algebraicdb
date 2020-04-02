use crate::pattern::Pattern;
use crate::types::Value;

#[derive(Debug)]
pub enum Expr<'a> {
    Ident(&'a str),
    Value(Value<'a>),
    Equals(Box<Expr<'a>>, Box<Expr<'a>>),
    NotEquals(Box<Expr<'a>>, Box<Expr<'a>>),
    LessEquals(Box<Expr<'a>>, Box<Expr<'a>>),
    LessThan(Box<Expr<'a>>, Box<Expr<'a>>),
    GreaterThan(Box<Expr<'a>>, Box<Expr<'a>>),
    GreaterEquals(Box<Expr<'a>>, Box<Expr<'a>>),
    And(Box<Expr<'a>>, Box<Expr<'a>>),
    Or(Box<Expr<'a>>, Box<Expr<'a>>),
}

#[derive(Debug)]
pub struct Ass<'a> {
    pub col: &'a str,
    pub expr: Expr<'a>,
}

#[derive(Debug)]
pub struct Select<'a> {
    pub items: Vec<Expr<'a>>,
    pub from: Option<SelectFrom<'a>>,
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug)]
pub enum WhereItem<'a> {
    Expr(Expr<'a>),
    Pattern(&'a str, Pattern<'a>),
}

#[derive(Debug)]
pub enum SelectFrom<'a> {
    Table(&'a str),
    Select(Box<Select<'a>>),
    Join(Box<Join<'a>>),
}

#[derive(Debug)]
pub struct Join<'a> {
    pub table_a: SelectFrom<'a>,
    pub table_b: SelectFrom<'a>,
    pub join_type: JoinType,
    pub on_clause: Option<Expr<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug)]
pub struct WhereClause<'a> {
    pub items: Vec<WhereItem<'a>>,
}

#[derive(Debug)]
pub struct Delete<'a> {
    pub table: &'a str,
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug)]
pub struct Drop<'a> {
    pub table: &'a str,
    //  pub drop_clause: Option<DropClause>, // should be cascade or restrict
}

#[derive(Debug)]
pub struct Insert<'a> {
    pub table: &'a str,
    pub columns: Vec<&'a str>,
    pub from: InsertFrom<'a>,
}

#[derive(Debug)]
pub enum InsertFrom<'a> {
    Values(Vec<Vec<Expr<'a>>>),
    Select(Select<'a>),
}

#[derive(Debug)]
pub struct CreateTable<'a> {
    pub table: &'a str,
    pub columns: Vec<(&'a str, &'a str)>,
}

#[derive(Debug)]
pub enum Stmt<'a> {
    Select(Select<'a>),
    Insert(Insert<'a>),
    Delete(Delete<'a>),
    Update(Update<'a>),
    CreateTable(CreateTable<'a>),
    CreateType(CreateType<'a>),
    Drop(Drop<'a>),
}

#[derive(Debug)]
pub struct Update<'a> {
    pub table: &'a str,
    pub ass: Vec<Ass<'a>>,
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug)]
pub enum CreateType<'a> {
    Variant(&'a str, Vec<(&'a str, Vec<&'a str>)>),
}

#[test]
fn ast_grammar() {
    use crate::grammar::StmtParser;

    let valid_examples = vec![
        r#"SELECT hello, ma, boi FROM feffe;"#,
        r#"SELECT hello, asdsad FROM adssad;"#,
        r#"INSERT INTO empty () VALUES ();"#,
        r#"INSERT INTO empty () VALUES (), (), ();"#,
        r#"INSERT INTO empty VALUES ();"#,
        r#"INSERT INTO feffes_mom (foo, bar, baz) VALUES (1, myself, hello);"#,
        r#"SELECT bleh FROM (SELECT 3);"#,
        r#"DELETE FROM feffe WHERE goblin;"#,
        r#"SELECT col FROM t1 LEFT JOIN t2;"#,
        r#"SELECT col FROM t1 RIGHT JOIN t2;"#,
        r#"SELECT col FROM t1 RIGHT OUTER JOIN t2;"#,
        r#"SELECT col FROM t1 FULL OUTER JOIN t2;"#,
        r#"UPDATE feffe SET hair_length = -3.14;"#,
        r#"UPDATE feffe SET hair_length = short WHERE hej=3;"#,
        r#"SELECT col FROM t1 LEFT JOIN t2 LEFT JOIN t3 ON 3=5;"#,
        r#"SELECT col FROM t1 LEFT JOIN (t2 LEFT JOIN t3 ON 3=5);"#,
        r#"SELECT col FROM t1 LEFT JOIN (t2 LEFT JOIN t3) ON 3=5;"#,
        r#"SELECT col FROM (t1 LEFT JOIN t2) LEFT JOIN t3 ON 3=5;"#,
        r#"SELECT col FROM t1 LEFT JOIN t2 ON 3 = 5 LEFT JOIN t3 ON 3 < 4;"#,
        r#"SELECT col FROM t1 LEFT JOIN t2 LEFT JOIN t3;"#,
        r#"UPDATE feffe SET hair_length = short WHERE hej=3 AND true OR false;"#,
        r#"SELECT c FROM t WHERE a AND b OR c AND d;"#,
        r#"SELECT y FROM t WHERE x: 1;"#,
        r#"SELECT y FROM t WHERE x: Val1(1, InnerVal2(true, _), y);"#,
        r#"INSERT INTO table VALUES (Val1(1, 2, T::Val2()), true);"#,
        r#"INSERT INTO table VALUES (T::Val1(1, 2, Val2()), true);"#,
        r#"SELECT FROM t WHERE true, x: Val1(1, InnerVal2(true, _), y);"#,
        r#"CREATE TYPE newCoolType AS VARIANT {};"#,
        r#"CREATE TABLE bananas ();"#,
        r#"CREATE TABLE bananas (col_a Integer, col_b Double);"#,
        r#"CREATE TYPE newCoolType AS VARIANT {
            Var1(),
            Var1(Bool),
            Var1(newCoolType, alsoCoolType),
        };"#,
        r#"DROP TABLE bananas ;"#,
        r#"CREATE TYPE newCoolType AS VARIANT {
            Var1(),
            -- Var1(Bool), yeah, this is a comment line whatcha gonna do bout it
            Var1(newCoolType, alsoCoolType),
        };"#,
    ];

    let invalid_examples = vec![
        r#"SELECT hello, ma boi FROM feffe;"#,
        r#"SELECT hello FROM 3;"#,
        r#"INSERT INTO withoutsemicolon"#,
        r#"INSERT INTO empty;"#,
        r#"SELECT c FROM t1 INNER LEFT JOIN t2;"#,
        r#"SELECT c FROM t1 INNER OUTER JOIN t2;"#,
        r#"INSERT INTO empty (2) VALUES ();"#,
        r#"INSERT INTO empty () VALUES ,,;"#,
        r#"DELETE just;"#,
        r#"DELETE FROM just WHERE ,;"#,
        r#"DELETE FROM just some more tables ;"#,
        r#"CREATE TABLE bananas;"#,
        r#"CREATE TABLE bananas (without_type);"#,
        r#"DELETE FROM now, with, commas ;"#,
        r#"UPDATE SET xxsxsxsxsxsxsxs=2 ;"#,
        r#"DROP ;"#,
        r#"INSERT INTO empty 
        -- (a)
        -- VALUES (2)
        ;"#,
    ];

    for ex in valid_examples {
        println!("Trying to parse {}", ex);
        let out = StmtParser::new().parse(ex).expect("Parsing failed");

        println!("parsed: {:#?}", out);
    }

    for ex in invalid_examples {
        println!("Trying to parse invalid input {}", ex);
        let _out = StmtParser::new()
            .parse(ex)
            .expect_err("Parsing succeeded when it should have failed");
    }
}
