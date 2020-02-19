use crate::ast::*;
pub use insert::*;

pub fn translate (stmt: Stmt) -> String {
    match stmt {
        Stmt::Insert(ins) => insert::translate(ins),
        _ => unimplemented!(),
    }
}





mod insert {
    use crate::ast::*;
    use crate::pattern::*;
    pub fn translate(ins: Insert) -> String {
        String::from(":)")
    }

    fn translate_pattern(pat: Pattern) -> String {
        match pat {
            Pattern::Int(i) => format!("{}", i),
            Pattern::Bool(b) => format!("{}", b),
            Pattern::Double(d) =>format!("{}", d),
            Pattern::Variant(con, vals) => {
                con.into()
                .append(":[")
                .append(vals.iter().map(|x| translate_pattern(x))
                    .join(","))
                .append("]")
            },
            Pattern::Binding(con) => unimplemented!(),
        }
    }
    #[cfg(test)]
    mod test {
        use super::*;
        use crate::grammar::parse;
        fn test_translate_pattern(){
            input = vec![
                r#"INSERT INTO table (col1, col2) VALUES (5, 8);"#,
                r#"INSERT INTO table (col1) VALUES (Val1(5, 2)) "#,
            ];
            output = vec![
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


        }

    }
}


