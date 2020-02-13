use bincode::{deserialize, serialize_into};
use std::cmp;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::mem::size_of;
use std::ops::Index;

pub type EnumTag = usize;
pub type TypeId = usize;

pub struct TypeMap {
    types: HashMap<TypeId, Type>,
    identifiers: HashMap<String, TypeId>,
    next_id: TypeId,
}

impl Index<&TypeId> for TypeMap {
    type Output = Type;

    fn index(&self, index: &TypeId) -> &Self::Output {
        &self.types[index]
    }
}

impl TypeMap {
    pub fn new() -> Self {
        TypeMap {
            types: HashMap::new(),
            identifiers: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn insert<I: Into<String>>(&mut self, name: I, t: Type) -> TypeId {
        let id = self.next_id;
        self.next_id += 1;

        self.types.insert(id, t);
        self.identifiers.insert(name.into(), id);

        id
    }

    pub fn get_id(&self, name: &str) -> Option<TypeId> {
        self.identifiers.get(name).map(|id| *id)
    }

    pub fn get(&self, name: &str) -> Option<&Type> {
        self.get_id(name).and_then(|id| self.types.get(&id))
    }

    pub fn types(&self) -> &HashMap<TypeId, Type> {
        &self.types
    }
}

#[derive(Debug, Clone)]
pub enum Type {
    Integer,
    Double,
    Bool,
    Sum(Vec<(String, Vec<TypeId>)>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Integer(i32),
    Double(f64),
    Bool(bool),
    Sum(String, String, Vec<Value>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Value::Integer(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Sum(type_name, variant, values) => {
                write!(f, "{}::{}(", type_name, variant)?;
                if values.len() > 0 {
                    for value in values.iter().take(values.len() - 1) {
                        write!(f, "{}, ", value)?;
                    }
                    for value in values.last() {
                        write!(f, "{}", value)?;
                    }
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
            Value::Sum(_type_name, variant, values) => {
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

    pub fn type_of(&self, types: &TypeMap) -> Option<TypeId> {
        match self {
            // TODO: maybe we should have a list of "keywords" somewhere we can use
            Value::Integer(_) => types.get_id("Integer"),
            Value::Double(_) => types.get_id("Double"),
            Value::Bool(_) => types.get_id("Bool"),
            Value::Sum(type_name, _, _) => types.get_id(type_name),
        }
    }
}

impl Type {
    pub fn size_of(&self, types: &TypeMap) -> usize {
        match self {
            Type::Integer => size_of::<i32>(),
            Type::Double => size_of::<f64>(),
            Type::Bool => size_of::<bool>(),
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

                // TODO: Type name
                Ok(Value::Sum("TODO".into(), name.clone(), values))
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
                eprintln!("{:?}", members);
                let values = members
                    .iter()
                    .map(|t_id| types[t_id].random_value(types))
                    .collect();
                // TODO: Type name
                Value::Sum("TODO".into(), variant.clone(), values)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::tests::create_type_map;

    #[test]
    fn test_to_bytes_and_back_again() {
        let (_ids, types) = create_type_map();

        let example_values: Vec<_> = (0..5000)
            .map(|i| {
                let map = types.types();
                let t_id: TypeId = *map.keys().nth(i % map.len()).unwrap();
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
    }
}
