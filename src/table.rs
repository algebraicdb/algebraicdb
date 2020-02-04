use crate::types::{TypeId, TypeMap, Value};

pub type Schema = Vec<TypeId>;

pub struct Table {
    schema: Schema,
    data: Vec<u8>,
    row_size: usize,
}

impl Table {
    pub fn new(schema: Schema, types: &TypeMap) -> Self {
        Self {
            data: vec![],
            row_size: schema
                .iter()
                .map(|t_id| types.get(t_id).unwrap())
                .map(|t| t.size_of(types))
                .sum(),
            schema,
        }
    }

    pub fn get_row(&self, row: usize, types: &TypeMap) -> Vec<Value> {
        let mut output = vec![];
        let mut data = &self.data[self.row_start(row)..];
        for t_id in self.schema.iter() {
            let t = types.get(t_id).unwrap();
            let t_size = t.size_of(types);
            output.push(t.from_bytes(&data[..t_size], types).unwrap());
            data = &data[t_size..];
        }

        output
    }

    pub fn row_count(&self) -> usize {
        assert_eq!(self.data.len() % self.row_size, 0);

        self.data.len() / self.row_size
    }

    fn row_start(&self, row: usize) -> usize {
        row * self.row_size
    }

    pub fn push_row(&mut self, cells: &[Value], types: &TypeMap) {
        assert_eq!(self.data.len() % self.row_size, 0);

        for (t, c) in self.schema.iter().zip(cells.iter()) {
            c.to_bytes(&mut self.data, types, types.get(t).unwrap())
        }

        assert_eq!(self.data.len() % self.row_size, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Type, TypeMap, Value};
    use std::collections::HashMap;

    #[test]
    fn test_table() {
        let mut types: TypeMap = HashMap::new();
        types.insert(0, Type::Int);
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

        let schema = vec![0, 1, 5];
        let mut table = Table::new(schema.clone(), &types);

        assert_eq!(table.row_count(), 0);

        let rows: Vec<Vec<Value>> = (0..5000)
            .map(|_| {
                schema
                    .iter()
                    .map(|t_id| types[t_id].random_value(&types))
                    .collect()
            })
            .collect();
        for (i, row) in rows.iter().enumerate() {
            eprintln!("inserting {}: {:?}", i, row);
            table.push_row(&row[..], &types);
            assert_eq!(table.row_count(), i + 1);
            assert_eq!(&table.get_row(i, &types), row);
        }

        for (i, row) in rows.iter().enumerate() {
            assert_eq!(&table.get_row(i, &types), row);
        }
    }
}
