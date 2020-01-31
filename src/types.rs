use bincode::{deserialize, serialize};
use std::cmp;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::mem::size_of;

pub type EnumTag = usize;
pub type TypeId = usize;

#[derive(Debug)]
pub enum Type {
    Int,
    Sum(Vec<(String, Vec<TypeId>)>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Value {
    Int(i32),
    Sum(String, Vec<Value>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Value::Int(int) => write!(f, "{}", int),
            Value::Sum(variant, values) => {
                write!(f, "{}", variant)?;
                for value in values {
                    write!(f, " {}", value)?;
                }
                Ok(())
            }
        }
    }
}

impl Value {
    pub fn to_bytes(&self, types: &HashMap<TypeId, Type>, t: &Type) -> Vec<u8> {
        let size = t.size_of(types);
        match self {
            Value::Int(val) => serialize(val).unwrap(),
            Value::Sum(variant, values) => {
                if let Type::Sum(variants) = t {
                    let (tag, variant_types) = variants
                        .iter()
                        .enumerate()
                        .find(|(_, (id, _))| id == variant)
                        .map(|(tag, (_, variant_types))| (tag, variant_types))
                        .unwrap();

                    let mut bytes = serialize(&tag).unwrap();

                    for (v, t_id) in values.iter().zip(variant_types.iter()) {
                        bytes.extend_from_slice(&v.to_bytes(types, &types[t_id])[..]);
                    }

                    // pad with 0:s for variants smaller than the largest variant
                    while bytes.len() < size {
                        bytes.push(0);
                    }

                    bytes
                } else {
                    panic!("Not a sum-type");
                }
            }
        }
    }
}

impl Type {
    pub fn size_of(&self, types: &HashMap<TypeId, Type>) -> usize {
        match self {
            Type::Int => size_of::<i32>(),
            Type::Sum(variants) => {
                size_of::<EnumTag>()
                    + variants
                        .iter()
                        .map(|(_, ts)| ts.iter().map(|t_id| types[t_id].size_of(types)).sum())
                        .fold(0, cmp::max)
            }
        }
    }

    fn from_bytes(
        &self,
        mut bytes: &[u8],
        types: &HashMap<TypeId, Type>,
    ) -> bincode::Result<Value> {
        assert_eq!(self.size_of(types), bytes.len());

        match self {
            Type::Int => {
                let value = deserialize::<i32>(bytes)?;
                //eprintln!("int: {}", value);
                Ok(Value::Int(value))
            }
            Type::Sum(variants) => {
                // parse enum tag
                let tag_size = size_of::<EnumTag>();
                let tag: EnumTag = deserialize(&bytes[..tag_size])?;
                bytes = &bytes[tag_size..];
                //eprintln!("sum variant {} (", tag);

                // parse subtypes
                let (name, members) = &variants[tag];
                let values = members
                    .iter()
                    .map(|t_id| {
                        let t = &types[t_id];
                        let t_size = t.size_of(types);
                        let v = t.from_bytes(&bytes[..t_size], types);
                        bytes = &bytes[t_size..];
                        v
                    })
                    .collect::<Result<_, _>>()?;

                //eprintln!(")");

                Ok(Value::Sum(name.clone(), values))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let mut types = HashMap::new();
        types.insert(0, Type::Int);
        types.insert(
            1,
            Type::Sum(vec![("Nil".into(), vec![]), ("Int".into(), vec![0])]),
        );
        types.insert(
            2,
            Type::Sum(vec![
                ("MaybeInt".into(), vec![1]),
                ("DoubleInt".into(), vec![0, 0]),
            ]),
        );

        let example_values = vec![
            (1, Value::Sum("Nil".into(), vec![])),
            (1, Value::Sum("Int".into(), vec![Value::Int(42)])),
            (
                2,
                Value::Sum("MaybeInt".into(), vec![Value::Sum("Nil".into(), vec![])]),
            ),
            (
                2,
                Value::Sum(
                    "MaybeInt".into(),
                    vec![Value::Sum("Int".into(), vec![Value::Int(1337)])],
                ),
            ),
            (
                2,
                Value::Sum("DoubleInt".into(), vec![Value::Int(11), Value::Int(22)]),
            ),
        ];

        for (type_id, value) in example_values {
            println!("Serializing value as bytes");
            println!("  Value: {}", value);

            let bytes: Vec<u8> = value.to_bytes(&types, &types[&type_id]);
            println!("  bytes: {:?}", bytes);

            let value_again = types[&type_id]
                .from_bytes(&bytes[..], &types)
                .expect("failed to parse");

            assert_eq!(value, value_again);
            println!("  Value: {}", value_again);
            println!();
        }

        //assert!(false); // for printing stdout
    }
}
