use crate::pattern::Pattern;
use crate::types::Value;

#[derive(Debug)]
pub enum Expr {
    Ident(String),
    Value(Value),
    Equals(Box<Expr>, Box<Expr>),
    NotEquals(Box<Expr>, Box<Expr>),
    LessEquals(Box<Expr>, Box<Expr>),
    LessThan(Box<Expr>, Box<Expr>),
    GreaterThan(Box<Expr>, Box<Expr>),
    GreaterEquals(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

#[derive(Debug)]
pub struct Ass {
    pub col: String,
    pub expr: Expr,
}

#[derive(Debug)]
pub struct Select {
    pub items: Vec<Expr>,
    pub from: Option<SelectFrom>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug)]
pub enum WhereItem {
    Expr(Expr),
    Pattern(String, Pattern),
}

#[derive(Debug)]
pub enum SelectFrom {
    Table(String),
    Select(Box<Select>),
    Join(Box<Join>),
}

#[derive(Debug)]
pub struct Join {
    pub table_a: SelectFrom,
    pub table_b: SelectFrom,
    pub join_type: JoinType,
    pub on_clause: Option<Expr>,
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug)]
pub struct WhereClause {
    pub items: Vec<WhereItem>,
}

#[derive(Debug)]
pub struct Delete {
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug)]
pub struct Drop {
    pub table: String,
    //  pub drop_clause: Option<DropClause>, // should be cascade or restrict
}

#[derive(Debug)]
pub struct Insert {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Expr>>,
}

#[derive(Debug)]
pub struct CreateTable {
    pub table: String,
    pub columns: Vec<(String, String)>,
}

#[derive(Debug)]
pub enum Stmt {
    Select(Select),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    CreateTable(CreateTable),
    CreateType(CreateType),
    Drop(Drop),
}

#[derive(Debug)]
pub struct Update {
    pub table: String,
    pub ass: Vec<Ass>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug)]
pub enum CreateType {
    Variant(String, Vec<(String, Vec<String>)>),
}

#[test]
fn ast_grammar() {
    use crate::grammar::StmtParser;

    let valid_examples = vec![
        r#"SELECT hello, ma, boi FROM feffe;"#,
        r#"SELECT hello, asdsad FROM adssad;"#,
        r#"INSERT INTO empty;"#,
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
        r#"SELECT x: 1, y FROM t;"#,
        r#"SELECT x: Val1(1, InnerVal2(true, _), y) FROM t;"#,
        r#"INSERT INTO table VALUES (Val1(1, 2, T::Val2()), true);"#,
        r#"INSERT INTO table VALUES (T::Val1(1, 2, Val2()), true);"#,
        r#"SELECT x: Val1(1, InnerVal2(true, _), y) FROM t WHERE true;"#,
        r#"CREATE TYPE newCoolType AS VARIANT {};"#,
        r#"CREATE TABLE bananas ();"#,
        r#"CREATE TABLE bananas (col_a Integer, col_b Double);"#,
        r#"CREATE TYPE newCoolType AS VARIANT {
            Var1(),
            Var1(Bool),
            Var1(newCoolType, alsoCoolType),
        };"#,
        r#"DROP TABLE bananas ;"#,
    ];

    let invalid_examples = vec![
        r#"SELECT hello, ma boi FROM feffe;"#,
        r#"SELECT hello FROM 3;"#,
        r#"INSERT INTO empty"#,
        r#"SELECT c FROM t1 INNER LEFT JOIN t2;"#,
        r#"SELECT c FROM t1 INNER OUTER JOIN t2;"#,
        r#"INSERT INTO empty (2) VALUES ();"#,
        r#"INSERT INTO empty () VALUES ,,;"#,
        r#"DELETE just;"#,
        r#"DELETE FROM just WHERE ;"#,
        r#"DELETE FROM just some more tables ;"#,
        r#"CREATE TABLE bananas;"#,
        r#"CREATE TABLE bananas (without_type);"#,
        r#"DELETE FROM now, with, commas ;"#,
        r#"UPDATE SET xxsxsxsxsxsxsxs=2 ;"#,
        r#"DROP ;"#,
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
