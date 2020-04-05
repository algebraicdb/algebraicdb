use serde::{Deserialize, Serialize};
use crate::ast::Spanned;

#[derive(Debug, Serialize, Deserialize)]
pub enum Pattern<'a> {
    /// Char literal
    Char(char),

    /// Integer literal
    Int(i32),

    /// Boolean literal
    Bool(bool),

    /// Floating-point literal
    Double(f64),

    /// Actual pattern matching
    Variant {
        namespace: Option<Spanned<&'a str>>,
        name: Spanned<&'a str>,
        sub_patterns: Vec<Spanned<Pattern<'a>>>,
    },

    /// _
    Ignore,

    /// Binding a value to a new identifier
    Binding(&'a str),
}

#[test]
fn pattern_grammar() {
    use crate::grammar::PatternParser;

    let valid_examples = vec![
        r#"Val1()"#,
        r#"T::Val1()"#,
        r#"42"#,
        r#"123.321"#,
        r#"true"#,
        r#"false"#,
        r#"Val1(1, InnerVal2(true, _), y)"#,
    ];

    let invalid_examples = vec![];

    for ex in valid_examples {
        println!("Trying to parse {}", ex);
        let out = PatternParser::new().parse(ex).expect("Parsing failed");

        println!("parsed: {:#?}", out);
    }

    for ex in invalid_examples {
        println!("Trying to parse invalid input {}", ex);
        let _out = PatternParser::new()
            .parse(ex)
            .expect_err("Parsing succeeded when it should have failed");
    }
}
