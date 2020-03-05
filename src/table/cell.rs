use crate::types::{EnumTag, Type, TypeId, TypeMap};
use bincode::deserialize;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fmt::{self, Display, Formatter};

pub struct Cell<'tb, 'ts> {
    type_id: TypeId,
    types: &'ts TypeMap,
    pub data: &'tb [u8],
}

impl<'tb, 'ts> Cell<'tb, 'ts> {
    pub fn new(type_id: TypeId, data: &'tb [u8], types: &'ts TypeMap) -> Self {
        Cell {
            type_id,
            types,
            data,
        }
    }
}

impl<'tb, 'ts> Display for Cell<'tb, 'ts> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let t = &self.types[&self.type_id];
        let t_size = t.size_of(self.types);
        match t {
            Type::Integer | Type::Double | Type::Char | Type::Bool => t
                .from_bytes(&self.data[..t_size], self.types)
                .unwrap()
                .fmt(f),
            Type::Sum(variants) => {
                // Converting this to a Value would do heap-allocations.
                // So we need to manually traverse the "tree"

                let tag_size = std::mem::size_of::<EnumTag>();
                let tag: EnumTag = deserialize(&self.data[..tag_size]).unwrap();

                let (name, sub_types) = &variants[tag];

                write!(f, "{}(", name)?;

                let mut first = true;
                let mut cursor: usize = tag_size;
                for t_id in sub_types {
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    let t = &self.types[t_id];
                    let t_size = t.size_of(self.types);
                    let end = cursor + t_size;
                    let cell = Cell::new(*t_id, &self.data[cursor..end], self.types);
                    write!(f, "{}", cell)?;
                    cursor += t_size;
                }

                write!(f, ")")
            }
        }
    }
}

impl PartialEq for Cell<'_, '_> {
    fn eq(&self, other: &Self) -> bool {
        debug_assert_eq!(self.type_id, other.type_id);
        self.data == other.data
    }
}

impl PartialOrd for Cell<'_, '_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        debug_assert_eq!(self.type_id, other.type_id);

        match &self.types[&self.type_id] {
            Type::Char => deserialize::<char>(self.data)
                .unwrap()
                .partial_cmp(&deserialize::<char>(other.data).unwrap()),
            Type::Integer => deserialize::<i32>(self.data)
                .unwrap()
                .partial_cmp(&deserialize(other.data).unwrap()),
            Type::Bool => deserialize::<bool>(self.data)
                .unwrap()
                .partial_cmp(&deserialize(other.data).unwrap()),
            Type::Double => deserialize::<f64>(self.data)
                .unwrap()
                .partial_cmp(&deserialize(other.data).unwrap()),
            Type::Sum(variants) => {
                let mut data1 = self.data;
                let mut data2 = other.data;

                let tag_size = std::mem::size_of::<EnumTag>();
                let tag1: EnumTag = deserialize(&data1[..tag_size]).unwrap();
                let tag2: EnumTag = deserialize(&data2[..tag_size]).unwrap();

                data1 = &data1[tag_size..];
                data2 = &data2[tag_size..];

                match tag1.cmp(&tag2) {
                    Ordering::Equal => {
                        let (_name, members) = &variants[tag1];
                        for &type_id in members {
                            let t = &self.types[&type_id];
                            let t_size = t.size_of(self.types);
                            let member_cell1 = Cell {
                                types: self.types,
                                type_id,
                                data: &data1[..t_size],
                            };
                            let member_cell2 = Cell {
                                types: self.types,
                                type_id,
                                data: &data2[..t_size],
                            };

                            match member_cell1.partial_cmp(&member_cell2) {
                                Some(Ordering::Equal) => continue,
                                not_equal => return not_equal,
                            }
                        }
                        Some(Ordering::Equal)
                    }
                    not_equal => Some(not_equal),
                }
            }
        }
    }
}
