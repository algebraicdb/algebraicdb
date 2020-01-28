use bincode::{deserialize, serialize};
use std::cmp;
use std::mem::size_of;

type EnumTag = u32;
#[derive(Debug)]
pub enum Type {
    Int,
    Sum(Vec<(String, Vec<Type>)>),
}

impl Type {
    pub fn size_of(&self) -> usize {
        match self {
            Type::Int => size_of::<i32>(),
            Type::Sum(variants) => {
                size_of::<u32>()
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
                let value = deserialize::<i32>(bytes);
                eprintln!("int: {}", value);
            }
            Type::Sum(variants) => {
                let tag = deserialize::<EnumTag>(bytes[..]);
                eprintln!("int: {}", value);
            }
        }

        Ok(())
    }*/
}
