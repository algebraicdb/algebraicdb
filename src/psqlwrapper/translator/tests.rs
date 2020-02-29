use crate::ast::*;


#[cfg(test)]
pub mod test {
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
        let asts = input.iter().map(|x| parser.parse(x).unwrap()).collect::<Vec<Stmt>>();
        

        for (ast, out) in asts.iter().zip(output) {
            assert_eq!(translate_insert(ast), out);
        }
        
    }
}

