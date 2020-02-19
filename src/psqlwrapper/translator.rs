use crate::ast::*;
pub use insert::*;




pub fn translate (stmt: &Stmt) -> String {
    match stmt {
        Stmt::Insert(ins) => insert::translate(ins),
        _ => unimplemented!(),
    }
}





mod insert {
    use crate::ast::*;
    //use crate::pattern::*;
    use crate::types::*;
    pub fn translate(ins: &Insert) -> String {
        format!("INSERT INTO {} ({}) VALUES ({});",
        ins.table, 
        ins.columns.join(","), 
        ins.values.iter().map(|x| translate_exp(x)).collect::<Vec<String>>().join(","))
    }

    fn translate_exp(exp: &Expr) -> String {
        match exp {
            Expr::Value(val) => translate_value(val),
            _ => panic!("Can't insert non-values") 
        }
    }

    fn translate_value(val: &Value) -> String {
        match val {
            Value::Integer(i) => format!("{}", i),
            Value::Bool(b) => format!("{}", b),
            Value::Double(d) =>format!("{}", d),
            Value::Sum(ns, var, vals) => {
                format!("{}{}:[{}:]", match ns {
                    Some(s) => format!("{}::", s),
                    None => String::new(),
                },
                var,
                vals.iter().map(|x| translate_value(x)).collect::<Vec<String>>().join(",")
            )
            },
        }
    }
}




#[cfg(test)]
pub mod test {
    use super::*;
    use crate::grammar::StmtParser;
    #[test]
    pub fn test_translate_pattern(){
        let parser = StmtParser::new();
        let input = vec![
            r#"INSERT INTO table (col1, col2) VALUES (5, 8);"#,
            r#"INSERT INTO table (col1) VALUES (Val1(5, 2)) "#,
        ];
        let output = vec![
            r#"INSERT INTO table (col1, col2) VALUES (5, 8);"#,
            r#"
                INSERT INTO table (col1) VALUES 
                ('
                    {
                        "Val1": [
                            5,
                            8
                        ]
                    }
                ');
            "#
        ];
        let asts = input.iter().map(|x| parser.parse(x).unwrap()).collect::<Vec<Stmt>>();
        assert_eq!(translate(&asts[0]), output[0]);
    }
}

