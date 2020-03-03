use crate::ast::WhereItem;
use crate::table::Schema;
use crate::types::{EnumTag, Type, TypeId, TypeMap};
use bincode::serialize;
use smallvec::SmallVec;

#[derive(Debug)]
pub enum Pattern {
    /// Integer literal
    Int(i32),

    /// Boolean literal
    Bool(bool),

    /// Floating-point literal
    Double(f64),

    /// Actual pattern matching
    Variant {
        namespace: Option<String>,
        name: String,
        sub_patterns: Vec<Pattern>,
    },

    /// _
    Ignore,

    /// Binding a value to a new identifier
    Binding(String),
}

#[derive(Debug)]
pub struct CompiledPattern {
    // TODO: this could probably be refactored into an Vec<u8> with a bitmask.
    pub matches: Vec<(usize, SmallVec<[u8; 16]>)>,
    pub bindings: Vec<(usize, TypeId, String)>,
}

impl CompiledPattern {
    pub fn compile(pattern: &[WhereItem], schema: &Schema, types: &TypeMap) -> Self {
        fn build_pattern(
            pattern: &Pattern,
            mut byte_index: usize,
            types: &TypeMap,
            type_id: TypeId,
            matches: &mut Vec<(usize, SmallVec<[u8; 16]>)>,
            bindings: &mut Vec<(usize, TypeId, String)>,
        ) {
            match pattern {
                Pattern::Int(v) => {
                    matches.push((byte_index, SmallVec::from_vec(serialize(v).unwrap())))
                }
                Pattern::Bool(v) => {
                    matches.push((byte_index, SmallVec::from_vec(serialize(v).unwrap())))
                }
                Pattern::Double(v) => {
                    matches.push((byte_index, SmallVec::from_vec(serialize(v).unwrap())))
                }
                Pattern::Ignore => {}
                Pattern::Binding(ident) => bindings.push((byte_index, type_id, ident.into())),
                Pattern::Variant {
                    namespace: _namespace,
                    name,
                    sub_patterns,
                } => {
                    if let Type::Sum(variants) = &types[&type_id] {
                        let (i, (_, sub_types)) = variants
                            .iter()
                            .enumerate()
                            .find(|(_, (variant, _))| variant == name)
                            .unwrap();
                        matches.push((byte_index, SmallVec::from_vec(serialize(&i).unwrap())));

                        byte_index += std::mem::size_of::<EnumTag>();
                        for (type_id, pattern) in sub_types.iter().zip(sub_patterns.iter()) {
                            let t = &types[type_id];
                            build_pattern(pattern, byte_index, types, *type_id, matches, bindings);
                            byte_index += t.size_of(types);
                        }
                    } else {
                        panic!("not a sum-type")
                    }
                }
            }
        }

        let mut bindings = vec![];
        let mut matches = vec![];

        for select_item in pattern {
            match select_item {
                WhereItem::Expr(_) => {} // Ignore expressions for now
                WhereItem::Pattern(name, pattern) => {
                    let mut byte_index = 0;
                    for (column, t_id) in &schema.columns {
                        if column == name {
                            build_pattern(
                                pattern,
                                byte_index,
                                types,
                                *t_id,
                                &mut matches,
                                &mut bindings,
                            );
                            break;
                        }

                        let t = &types[t_id];
                        byte_index += t.size_of(types);
                    }
                }
            }
        }

        CompiledPattern { bindings, matches }
    }
}

#[test]
fn pattern_grammar() {
    use crate::grammar::PatternParser;

    let valid_examples = vec![
        r#"Val1()"#,
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
