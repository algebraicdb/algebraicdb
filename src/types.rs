use bincode::{deserialize, serialize};
use std::cmp;
use std::mem::size_of;

pub type EnumTag = usize;

#[derive(Debug)]
pub enum Type {
    Int,
    Sum(Vec<(String, Vec<Type>)>),
}

#[derive(Debug)]
pub enum Value {
    Int(i32),
    Sum(Vec<(String, Vec<Value>)>),
}
impl Value {
    pub fn rev_parse(&self) -> Vec<u8> {
        match self {
            Value::Int(val) => {}
            Value::Sum(variants) => {
                // parse enum tag
                let tag_size = size_of::<EnumTag>();
                let tag: EnumTag = deserialize(&bytes[..tag_size])?;
                eprintln!("sum variant {} (", tag);

                // parse subtypes
                let (name, members) = &variants[tag];
                for t in members {
                    let t_size = t.size_of();
                    t.parse(&bytes[tag_size..t_size])?;
                }
                eprintln!(")");
            }
        }
    }
}

impl Type {
    pub fn size_of(&self) -> usize {
        match self {
            Type::Int => size_of::<i32>(),
            Type::Sum(variants) => {
                size_of::<EnumTag>()
                    + variants
                        .iter()
                        .map(|(_, ts)| ts.iter().map(|t| t.size_of()).sum())
                        .fold(0, cmp::max)
            }
        }
    }

    /*fn parse(&self, bytes: &[u8]) -> bincode::Result<()> {
        assert_eq!(self.size_of(), bytes.len());

        match self {
            Type::Int => {
                let value = deserialize::<i32>(bytes)?;
                eprintln!("int: {}", value);
            }
            Type::Sum(variants) => {
                // parse enum tag
                let tag_size = size_of::<EnumTag>();
                let tag: EnumTag = deserialize(&bytes[..tag_size])?;
                eprintln!("sum variant {} (", tag);

                // parse subtypes
                let (name, members) = &variants[tag];
                for t in members {
                    let t_size = t.size_of();
                    t.parse(&bytes[tag_size..t_size])?;
                }
                eprintln!(")");
            }
        }

        Ok(())
    }*/
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let t: Type = Type::Sum(vec![
            ("Var1".into(), vec![]),
            ("Var2".into(), vec![Type::Int]),
        ]);

        t.parse(&[1, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0])
            .expect("Failed to parse");
        assert!(false);
    }
}
