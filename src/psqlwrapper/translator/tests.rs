use crate::ast::*;

use super::*;
use crate::grammar::StmtParser;
#[test]
pub fn test_translate_pattern(){
    let parser = StmtParser::new();
    let input = vec![
        r#"INSERT INTO table (col1,col2) VALUES (5,8);"#,
        r#"INSERT INTO table (col1) VALUES (Val1(5, 2));"#,
    ];
    let output = vec![
        r#"INSERT INTO table (col1,col2) VALUES (5,8);"#,
        r#"INSERT INTO table (col1) VALUES ('{"Val1":[5,2]}');"#,
    ];
    let asts: Vec<Stmt> = input.iter().map(|x| parser.parse(x).unwrap()).collect::<Vec<Stmt>>();
    
    
    for (stmt, out) in asts.iter().zip(output) {
        assert_eq!(translate_insert(match stmt {Stmt::Insert(i) => i, _ => panic!()}), out);
    }
    
}

#[test]
pub fn test_translate_select(){
    let parser = StmtParser::new();
    let input = vec![
        r#"SELECT b FROM a;"#,
//            r#"INSERT INTO table (col1) VALUES (Val1(5, 2));"#,
    ];
    let output = vec![
        r#"SELECT b FROM a ;"#,
//           r#"INSERT INTO table (col1) VALUES ('{"Val1":[5,2]}');"#,
    ];
    let asts = input.iter().map(|x| parser.parse(x).unwrap()).collect::<Vec<Stmt>>();
    

    for (stmt, out) in asts.iter().zip(output) {
        assert_eq!(translate_select(match stmt {Stmt::Select(sel) => sel, _ => panic!()}), out);
    }
}


