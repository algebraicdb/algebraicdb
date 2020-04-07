mod pattern;
mod span;

pub use pattern::*;
pub use span::*;

use crate::types::Value;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Expr<'a> {
    #[serde(borrow)]
    Ident(Spanned<&'a str>),
    Value(Spanned<Value<'a>>),
    Eql(Box<(Spanned<Expr<'a>>, Spanned<Expr<'a>>)>),
    NEq(Box<(Spanned<Expr<'a>>, Spanned<Expr<'a>>)>),
    LEq(Box<(Spanned<Expr<'a>>, Spanned<Expr<'a>>)>),
    LTh(Box<(Spanned<Expr<'a>>, Spanned<Expr<'a>>)>),
    GTh(Box<(Spanned<Expr<'a>>, Spanned<Expr<'a>>)>),
    GEq(Box<(Spanned<Expr<'a>>, Spanned<Expr<'a>>)>),
    And(Box<(Spanned<Expr<'a>>, Spanned<Expr<'a>>)>),
    Or(Box<(Spanned<Expr<'a>>, Spanned<Expr<'a>>)>),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Ass<'a> {
    pub col: Spanned<&'a str>,

    #[serde(borrow)]
    pub expr: Spanned<Expr<'a>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Select<'a> {
    #[serde(borrow)]
    pub items: Vec<Spanned<Expr<'a>>>,

    #[serde(borrow)]
    pub from: Option<SelectFrom<'a>>,

    #[serde(borrow)]
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum WhereItem<'a> {
    #[serde(borrow)]
    Expr(Spanned<Expr<'a>>),
    Pattern(Spanned<&'a str>, Spanned<Pattern<'a>>),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum SelectFrom<'a> {
    Table(&'a str),
    Select(Box<Select<'a>>),
    Join(Box<Join<'a>>),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Join<'a> {
    #[serde(borrow)]
    pub table_a: SelectFrom<'a>,

    #[serde(borrow)]
    pub table_b: SelectFrom<'a>,
    pub join_type: JoinType,

    #[serde(borrow)]
    pub on_clause: Option<Spanned<Expr<'a>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WhereClause<'a> {
    #[serde(borrow)]
    pub items: Vec<WhereItem<'a>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Delete<'a> {
    pub table: &'a str,

    #[serde(borrow)]
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Drop<'a> {
    pub table: &'a str,
    //  pub drop_clause: Option<DropClause>, // should be cascade or restrict
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Insert<'a> {
    pub table: &'a str,
    pub columns: Spanned<Vec<Spanned<&'a str>>>,

    #[serde(borrow)]
    pub from: InsertFrom<'a>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum InsertFrom<'a> {
    #[serde(borrow)]
    Values(Vec<Spanned<Vec<Spanned<Expr<'a>>>>>),
    Select(Spanned<Select<'a>>),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateTable<'a> {
    pub table: &'a str,
    pub columns: Vec<(Spanned<&'a str>, Spanned<&'a str>)>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Stmt<'a> {
    #[serde(borrow)]
    Select(Select<'a>),
    Insert(Insert<'a>),
    Delete(Delete<'a>),
    Update(Update<'a>),
    CreateTable(CreateTable<'a>),
    CreateType(CreateType<'a>),
    Drop(Drop<'a>),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Update<'a> {
    #[serde(borrow)]
    pub table: &'a str,

    #[serde(borrow)]
    pub ass: Vec<Ass<'a>>,

    #[serde(borrow)]
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CreateType<'a> {
    Variant {
        #[serde(borrow)]
        name: Spanned<&'a str>,

        #[serde(borrow)]
        variants: Vec<(Spanned<&'a str>, Vec<Spanned<&'a str>>)>,
    },
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
