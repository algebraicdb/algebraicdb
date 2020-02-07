use bincode::{deserialize, serialize_into};
use std::cmp;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::mem::size_of;

pub type EnumTag = usize;
pub type TypeId = usize;

pub type TypeMap = HashMap<TypeId, Type>;

#[derive(Debug)]
pub enum Type {
    Integer,
    Double,
    Bool,
    Sum(Vec<(String, Vec<TypeId>)>),
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Integer(i32),
    Double(f64),
    Bool(bool),
    Sum(String, Vec<Value>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Value::Integer(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Sum(variant, values) => {
                write!(f, "({}", variant)?;
                for value in values {
                    write!(f, " {}", value)?;
                }
                write!(f, ")")
            }
        }
    }
}

impl Value {
    pub fn to_bytes<W: Write>(&self, writer: &mut W, types: &TypeMap, t: &Type) {
        let size = t.size_of(types);
        match self {
            Value::Integer(val) => serialize_into(writer, val).unwrap(),
            Value::Double(val) => serialize_into(writer, val).unwrap(),
            Value::Bool(val) => serialize_into(writer, val).unwrap(),
            Value::Sum(variant, values) => {
                if let Type::Sum(variants) = t {
                    let (tag, variant_types) = variants
                        .iter()
                        .enumerate()
                        .find(|(_, (id, _))| id == variant)
                        .map(|(tag, (_, variant_types))| (tag, variant_types))
                        .unwrap();

                    serialize_into(&mut *writer, &tag).unwrap();
                    let mut bytes_written = size_of::<EnumTag>();

                    for (v, t_id) in values.iter().zip(variant_types.iter()) {
                        let t = &types[t_id];
                        bytes_written += t.size_of(types);
                        v.to_bytes(writer, types, t);
                    }

                    // pad with 0:s for variants smaller than the largest variant
                    for _ in bytes_written..size {
                        writer.write_all(&[0]).unwrap();
                    }
                } else {
                    panic!("Not a sum-type");
                }
            }
        }
    }
}

impl Type {
    pub fn size_of(&self, types: &TypeMap) -> usize {
        match self {
            Type::Integer => size_of::<i32>(),
            Type::Bool => size_of::<bool>(),
            Type::Double => size_of::<f64>(),
            Type::Sum(variants) => {
                size_of::<EnumTag>()
                    + variants
                        .iter()
                        .map(|(_, ts)| ts.iter().map(|t_id| types[t_id].size_of(types)).sum())
                        .fold(0, cmp::max)
            }
        }
    }

    pub fn from_bytes(&self, mut bytes: &[u8], types: &TypeMap) -> bincode::Result<Value> {
        assert_eq!(self.size_of(types), bytes.len());

        match self {
            Type::Integer => deserialize(bytes).map(|v| Value::Integer(v)),
            Type::Bool => deserialize(bytes).map(|v| Value::Bool(v)),
            Type::Double => deserialize(bytes).map(|v| Value::Double(v)),
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

    pub fn random_value(&self, types: &TypeMap) -> Value {
        match self {
            Type::Integer => Value::Integer(rand::random::<i32>()),
            Type::Bool => Value::Bool(rand::random::<bool>()),
            Type::Double => Value::Double(rand::random::<f64>()),
            Type::Sum(variants) => {
                let i = rand::random::<usize>() % variants.len();
                let (variant, members) = &variants[i];
                let values = members
                    .iter()
                    .map(|t_id| types[t_id].random_value(types))
                    .collect();
                Value::Sum(variant.clone(), values)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes_and_back_again() {
        let mut types: TypeMap = HashMap::new();
        types.insert(0, Type::Integer);
        types.insert(1, Type::Bool);
        types.insert(2, Type::Double);
        types.insert(
            3,
            Type::Sum(vec![("Nil".into(), vec![]), ("Int".into(), vec![0])]),
        );
        types.insert(
            4,
            Type::Sum(vec![
                ("MaybeInt".into(), vec![3]),
                ("DoubleInt".into(), vec![0, 0]),
            ]),
        );
        types.insert(
            5,
            Type::Sum(vec![
                ("OtherThing".into(), vec![4]),
                ("Boolean".into(), vec![1]),
            ]),
        );

        let example_values: Vec<_> = (0..5000)
            .map(|i| {
                let t_id = i % types.len();
                (t_id, types[&t_id].random_value(&types))
            })
            .collect();

        for (type_id, value) in example_values {
            println!("  Value Before: {}", value);

            let mut bytes: Vec<u8> = vec![];
            value.to_bytes(&mut bytes, &types, &types[&type_id]);
            println!("  Bytes: {:?}", bytes);

            let value_again = types[&type_id]
                .from_bytes(&bytes[..], &types)
                .expect("failed to parse");

            assert_eq!(value, value_again);
            println!("  Value After:  {}", value_again);
            println!();
        }

        //assert!(false); // for printing stdout
    }
}
