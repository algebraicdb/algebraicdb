use crate::types::{EnumTag, Type, TypeId, TypeMap, Value};
use bincode::deserialize;
use std::cmp::{Ord, Ordering, PartialOrd};

pub type Schema = Vec<Column>;


#[derive(Debug, Clone)]
pub struct Column{
    col_type: TypeId,
    name: String,
}


// Table defines 
pub struct Table{
    schema: Schema,
    data: Vec<u8>,
    row_size: usize,
}

pub struct Row<'a> {
    schema: &'a Schema,
    data: &'a [u8],
}

pub struct Cell<'tb, 'ts> {
    type_id: TypeId,
    types: &'ts TypeMap,
    data: &'tb [u8],
}

pub struct RowIter<'a> {
    table: &'a Table,
    row: usize,
}

impl Table{
    pub fn new(schema: Schema, types: &TypeMap) -> Self {
        Self {
            data: vec![],
            row_size: schema
                .iter()
                .map(|col| types.get(&col.col_type).unwrap())
                .map(|t| t.size_of(types))
                .sum(),
            schema,
        }
    }

    pub fn iter<'a>(&'a self) -> RowIter<'a> {
        RowIter {
            table: self,
            row: 0,
        }
    }

    pub fn get_row<'a>(&'a self, row: usize) -> Row<'a> {
        let start = self.row_start(row);
        let end = start + self.row_size;

        Row {
            schema: &self.schema,
            data: &self.data[start..end],
        }
    }

    // i found it ayy lmao fuck you you little bitch ill have you know i graduated at the top of
    pub fn get_row_value(&self, row: usize, types: &TypeMap) -> Vec<Value> {
        let mut output = vec![];
        let mut data = &self.data[self.row_start(row)..];
        for col in self.schema.iter() {
            let t = types.get(&col.col_type).unwrap();
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

        for (col, c) in self.schema.iter().zip(cells.iter()) {
            c.to_bytes(&mut self.data, types, types.get(&col.col_type).unwrap())
        }

        assert_eq!(self.data.len() % self.row_size, 0);
    }
}

impl<'a> Iterator for RowIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.table.row_count() {
            return None;
        }

        let row = self.table.get_row(self.row);
        self.row += 1;
        Some(row)
    }
}

impl<'tb, 'ts> Row<'tb> {
    pub fn get_cell(&'tb self, types: &'ts TypeMap, col: usize) -> Cell<'tb, 'ts> {
        let mut start = 0;
        for col in &self.schema[..col] {
            let t = &types[&col.col_type];
            let t_size = t.size_of(types);
            start += t_size;
        }

        let end = start + types[&self.schema[col].col_type].size_of(types);

        Cell {
            type_id: self.schema[col].col_type,
            data: &self.data[start..end],
            types,
        }
    }

    pub fn cell_count(&self) -> usize {
        self.schema.len()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Type, TypeMap, Value};
    use std::collections::HashMap;

    fn create_type_map() -> TypeMap {
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
        types
    }

    #[test]
    fn test_table() {
        let types = create_type_map();
        let schema: Vec<Column> = vec![Column {col_type: 0, name: String::from("feffe")}, Column {col_type: 1, name: String::from("är en")}, Column {col_type: 5, name: String::from("Pepungo")}];
        let mut table = Table::new(schema.clone(), &types);

        assert_eq!(table.row_count(), 0);

        const ROWS_COUNT: usize = 5000;
        let rows: Vec<Vec<Value>> = (0..ROWS_COUNT)
            .map(|_| {
                schema
                    .iter()
                    .map(|col| types[&col.col_type].random_value(&types))
                    .collect()
            })
            .collect();
        for (i, row) in rows.iter().enumerate() {
            eprintln!("inserting {}: {:?}", i, row);
            table.push_row(&row[..], &types);
            assert_eq!(table.row_count(), i + 1);
            assert_eq!(&table.get_row_value(i, &types), row);
        }

        for (i, row) in rows.iter().enumerate() {
            assert_eq!(&table.get_row_value(i, &types), row);
        }

        assert_eq!(table.iter().count(), ROWS_COUNT);

        for (i, row) in table.iter().enumerate() {
            for j in 0..row.cell_count() {
                let cell = row.get_cell(&types, j);
                let t = &types[&schema[j].col_type];
                let v = t.from_bytes(cell.data, &types).unwrap();
                assert_eq!(v, rows[i][j]);
            }
        }
    }

    #[test]
    fn test_ord_ints() {
        let types = create_type_map();
        let schema = vec![Column {col_type: 0, name: String::from("pupunga")}];
        let mut table = Table::new(schema.clone(), &types);

        table.push_row(&[Value::Integer(1)], &types);
        table.push_row(&[Value::Integer(2)], &types);
        table.push_row(&[Value::Integer(3)], &types);
        table.push_row(&[Value::Integer(9001)], &types);

        for i in 0..table.row_count() {
            let row_i = table.get_row(i);
            let cell_i = row_i.get_cell(&types, 0);
            assert!(cell_i == cell_i);
            for j in i + 1..table.row_count() {
                let row_j = table.get_row(j);
                let cell_j = row_j.get_cell(&types, 0);
                assert!(cell_i < cell_j);
                assert!(cell_j > cell_i);
            }
        }
    }

    #[test]
    fn test_ord_variants() {
        let types = create_type_map();
        let schema = vec![Column {col_type: 5, name: String::from("pupunga")}];
        let mut table = Table::new(schema.clone(), &types);

        table.push_row(
            &[Value::Sum(
                "OtherThing".into(),
                vec![Value::Sum(
                    "MaybeInt".into(),
                    vec![Value::Sum("Nil".into(), vec![])],
                )],
            )],
            &types,
        );
        table.push_row(
            &[Value::Sum("Boolean".into(), vec![Value::Bool(false)])],
            &types,
        );
        table.push_row(
            &[Value::Sum("Boolean".into(), vec![Value::Bool(true)])],
            &types,
        );

        for i in 0..table.row_count() {
            let row_i = table.get_row(i);
            let cell_i = row_i.get_cell(&types, 0);
            assert!(cell_i == cell_i);
            for j in i + 1..table.row_count() {
                let row_j = table.get_row(j);
                let cell_j = row_j.get_cell(&types, 0);
                assert!(cell_i < cell_j);
                assert!(cell_j > cell_i);
            }
        }
    }
}
