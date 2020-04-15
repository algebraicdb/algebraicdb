mod pattern;
mod span;

pub use pattern::*;
pub use span::*;

use crate::types::Value;
use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Instr {
    BeginTransaction(),
    EndTransaction(),
    Stmt(Stmt),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Stmt {
    Select(Select),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    CreateTable(CreateTable),
    CreateType(CreateType),
    Drop(Drop),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Expr {
    Ident(Spanned<String>),
    Value(Spanned<Value<'static>>),
    Eql(Box<(Spanned<Expr>, Spanned<Expr>)>),
    NEq(Box<(Spanned<Expr>, Spanned<Expr>)>),
    LEq(Box<(Spanned<Expr>, Spanned<Expr>)>),
    LTh(Box<(Spanned<Expr>, Spanned<Expr>)>),
    GTh(Box<(Spanned<Expr>, Spanned<Expr>)>),
    GEq(Box<(Spanned<Expr>, Spanned<Expr>)>),
    And(Box<(Spanned<Expr>, Spanned<Expr>)>),
    Or(Box<(Spanned<Expr>, Spanned<Expr>)>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Ass {
    pub col: Spanned<String>,

    pub expr: Spanned<Expr>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Select {
    pub items: Vec<Spanned<Expr>>,

    pub from: Option<SelectFrom>,

    pub where_clause: Option<WhereClause>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum WhereItem {
    Expr(Spanned<Expr>),
    Pattern(Spanned<String>, Spanned<Pattern>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum SelectFrom {
    Table(String),
    Select(Box<Select>),
    Join(Box<Join>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Join {
    pub table_a: SelectFrom,
    pub table_b: SelectFrom,

    pub join_type: JoinType,

    pub on_clause: Option<Spanned<Expr>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WhereClause {
    pub items: Vec<WhereItem>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Delete {
    pub table: String,

    pub where_clause: Option<WhereClause>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Drop {
    pub table: String,
    //  pub drop_clause: Option<DropClause>, // should be cascade or restrict
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Insert {
    pub table: String,
    pub columns: Spanned<Vec<Spanned<String>>>,
    pub from: InsertFrom,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum InsertFrom {
    Values(Vec<Spanned<Vec<Spanned<Expr>>>>),
    Select(Spanned<Select>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateTable {
    pub table: String,
    pub columns: Vec<(Spanned<String>, Spanned<String>)>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Update {
    pub table: String,

    pub ass: Vec<Ass>,

    pub where_clause: Option<WhereClause>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum CreateType {
    Variant {
        name: Spanned<String>,
        variants: Vec<(Spanned<String>, Vec<Spanned<String>>)>,
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
