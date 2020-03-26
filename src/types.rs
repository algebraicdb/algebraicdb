use bincode::{deserialize, serialize_into};
use std::char;
use std::cmp;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::mem::size_of;
use std::ops::Index;
use std::cmp::Ordering;

pub type EnumTag = usize;
pub type TypeId = usize;

pub struct TypeMap {
    types: HashMap<TypeId, Type>,
    identifiers: HashMap<String, TypeId>,
    constructors: HashMap<String, Vec<TypeId>>,
    next_id: TypeId,
    bool_id: TypeId,
    integer_id: TypeId,
    double_id: TypeId,
    char_id: TypeId,
}

pub enum BaseType {
    Bool,
    Integer,
    Double,
    Char,
}

impl Index<&TypeId> for TypeMap {
    type Output = Type;

    fn index(&self, index: &TypeId) -> &Self::Output {
        &self.types[index]
    }
}

impl TypeMap {
    pub fn new() -> Self {
        let mut map = TypeMap {
            types: HashMap::new(),
            identifiers: HashMap::new(),
            constructors: HashMap::new(),
            next_id: 1,
            integer_id: 0,
            double_id: 0,
            bool_id: 0,
            char_id: 0,
        };

        map.integer_id = map.insert("Integer", Type::Integer);
        map.double_id = map.insert("Double", Type::Double);
        map.bool_id = map.insert("Bool", Type::Bool);
        map.char_id = map.insert("Char", Type::Char);
        map
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

    pub fn get_base_id(&self, base_type: BaseType) -> TypeId {
        match base_type {
            BaseType::Bool => self.bool_id,
            BaseType::Integer => self.integer_id,
            BaseType::Double => self.double_id,
            BaseType::Char => self.char_id,
        }
    }

    pub fn get(&self, name: &str) -> Option<&Type> {
        self.get_id(name).map(|id| self.get_by_id(id))
    }

    pub fn get_by_id(&self, id: TypeId) -> &Type {
        self.types
            .get(&id)
            .unwrap_or_else(|| panic!("No type with id: {}", id))
    }

    pub fn get_name(&self, type_id: TypeId) -> Option<&str> {
        // TODO: Make this not O(n)
        for (name, id) in self.identifiers.iter() {
            if id == &type_id {
                return Some(name);
            }
        }
        None
    }

    pub fn types(&self) -> &HashMap<TypeId, Type> {
        &self.types
    }

    pub fn identifiers(&self) -> &HashMap<String, TypeId> {
        &self.identifiers
    }

    pub fn constructors_of(&self, name: &str) -> Option<&Vec<TypeId>> {
        self.constructors.get(name)
    }
}

#[derive(Debug, Clone)]
pub enum Type {
    Integer,
    Double,
    Bool,
    Char,
    Sum(Vec<(String, Vec<TypeId>)>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Char(char),
    Integer(i32),
    Double(f64),
    Bool(bool),
    Sum(Option<String>, String, Vec<Value>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Value::Char(v) => write!(f, "{}", v),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Sum(namespace, variant, values) => {
                if let Some(namespace) = namespace {
                    write!(f, "{}::", namespace)?;
                }
                write!(f, "{}(", variant)?;
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
            Value::Char(val) => serialize_into(writer, &u32::from(*val)).unwrap(),
            Value::Integer(val) => serialize_into(writer, val).unwrap(),
            Value::Double(val) => serialize_into(writer, val).unwrap(),
            Value::Bool(val) => serialize_into(writer, val).unwrap(),
            Value::Sum(type_name, variant, values) => {
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
                    panic!(
                        "Not a sum-type: {:?}::{}({:?})\nIs actually: {:?}",
                        type_name, variant, values, t
                    );
                }
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Char(v1), Value::Char(v2)) => Some(v1.cmp(v2)),
            (Value::Integer(v1), Value::Integer(v2)) => Some(v1.cmp(v2)),
            (Value::Double(v1), Value::Double(v2)) => Some(v1.partial_cmp(v2).unwrap_or(Ordering::Greater)),
            (Value::Bool(v1), Value::Bool(v2)) => Some(v1.cmp(v2)),
            (Value::Sum(_, _, _), Value::Sum(_, _, _)) => unimplemented!("Ord for sum-types"),
            (_, _) => None
        }
    }
}

impl Type {
    pub fn size_of(&self, types: &TypeMap) -> usize {
        match self {
            Type::Char => size_of::<char>(),
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
            Type::Char => deserialize(bytes).map(|v| Value::Char(char::from_u32(v).unwrap())),
            Type::Integer => deserialize(bytes).map(|v| Value::Integer(v)),
            Type::Bool => deserialize(bytes).map(|v| Value::Bool(v)),
            Type::Double => deserialize(bytes).map(|v| Value::Double(v)),
            Type::Sum(variants) => {
                // parse enum tag
                let tag_size = size_of::<EnumTag>();
                let tag: EnumTag = deserialize(&bytes[..tag_size])?;
                bytes = &bytes[tag_size..];

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

                // TODO: Type name
                Ok(Value::Sum(None, name.clone(), values))
            }
        }
    }

    pub fn random_value(&self, types: &TypeMap) -> Value {
        match self {
            Type::Char => Value::Char(rand::random::<char>()),
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
                // TODO: Type name
                Value::Sum(None, variant.clone(), values)
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
