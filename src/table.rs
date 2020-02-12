use crate::pattern::CompiledPattern;
use crate::types::{EnumTag, Type, TypeId, TypeMap, Value};
use bincode::deserialize;
use std::cmp::{Ord, Ordering, PartialOrd};

pub type Schema = Vec<(String, TypeId)>;

#[derive(Debug, Clone)]
pub struct Column {
    col_type: TypeId,
    name: String,
}

// Table defines
pub struct Table {
    schema: Schema,
    data: Vec<u8>,
    row_size: usize,
}

pub struct Row<'a> {
    schema: &'a Schema,
    data: &'a [u8],
}

pub struct Cell<'ts, 'tb> {
    type_id: TypeId,
    types: &'ts TypeMap,
    data: &'tb [u8],
}

pub struct RowIter<'a> {
    table: &'a Table,
    row: usize,
}

pub struct CellPatternIter<'p, 'ts, 'tb> {
    pattern: &'p CompiledPattern,
    types: &'ts TypeMap,
    data: &'tb [u8],
    cursor: usize,
}

pub struct RowPatternIter<'p, 'ts, 'tb> {
    pattern: &'p CompiledPattern,
    types: &'ts TypeMap,
    table: &'tb Table,
    row: usize,
}

impl Table {
    pub fn new(schema: Schema, types: &TypeMap) -> Self {
        Self {
            data: vec![],
            row_size: schema
                .iter()
                .map(|(_, t_id)| types.get(t_id).unwrap())
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

    pub fn pattern_iter<'p, 'ts, 'tb>(
        &'tb self,
        pattern: &'p CompiledPattern,
        types: &'ts TypeMap,
    ) -> RowPatternIter<'p, 'ts, 'tb> {
        RowPatternIter {
            pattern,
            table: self,
            row: 0,
            types,
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

    pub fn get_row_value(&self, row: usize, types: &TypeMap) -> Vec<Value> {
        let mut output = vec![];
        let mut data = &self.data[self.row_start(row)..];
        for (_, t_id) in self.schema.iter() {
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

        for (t, c) in self.schema.iter().map(|(_, t)| t).zip(cells.iter()) {
            c.to_bytes(&mut self.data, types, types.get(t).unwrap())
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

impl<'p, 'ts, 'tb> Iterator for RowPatternIter<'p, 'ts, 'tb> {
    type Item = CellPatternIter<'p, 'ts, 'tb>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            if self.row >= self.table.row_count() {
                return None;
            }

            let row = self.table.get_row(self.row);
            self.row += 1;
            for (i, value) in &self.pattern.matches {
                for j in 0..value.len() {
                    if row.data[i + j] != value[j] {
                        continue 'outer;
                    }
                }
            }

            return Some(CellPatternIter {
                cursor: 0,
                pattern: self.pattern,
                types: self.types,
                data: row.data,
            });
        }
    }
}

impl<'p, 'ts, 'tb> Iterator for CellPatternIter<'p, 'ts, 'tb> {
    type Item = (&'p str, Cell<'ts, 'tb>);

    fn next(&mut self) -> Option<Self::Item> {
        self.pattern
            .bindings
            .get(self.cursor)
            .map(|(index, type_id, ident)| {
                self.cursor += 1;

                let t = &self.types[type_id];
                let type_size = t.size_of(self.types);

                let cell = Cell {
                    type_id: *type_id,
                    types: self.types,
                    data: &self.data[*index..type_size],
                };
                (ident.as_str(), cell)
            })
    }
}

impl<'tb, 'ts> Row<'tb> {
    pub fn get_cell(&'tb self, types: &'ts TypeMap, col: usize) -> Cell<'ts, 'tb> {
        let mut start = 0;
        for (_, t_id) in &self.schema[..col] {
            let t = &types[t_id];
            let t_size = t.size_of(types);
            start += t_size;
        }

        let end = start + types[&self.schema[col].1].size_of(types);

        Cell {
            type_id: self.schema[col].1,
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
        let schema = vec![("i".into(), 0), ("b".into(), 1), ("s".into(), 5)];
        let mut table = Table::new(schema.clone(), &types);

        assert_eq!(table.row_count(), 0);

        const ROWS_COUNT: usize = 5000;
        let rows: Vec<Vec<Value>> = (0..ROWS_COUNT)
            .map(|_| {
                schema
                    .iter()
                    .map(|(_, t_id)| types[t_id].random_value(&types))
                    .collect()
            })
            .collect();
        for (i, row) in rows.iter().enumerate() {
            println!("inserting {}: {:?}", i, row);
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
                let t = &types[&schema[j].1];
                let v = t.from_bytes(cell.data, &types).unwrap();
                assert_eq!(v, rows[i][j]);
            }
        }
    }

    #[test]
    fn test_ord_ints() {
        let types = create_type_map();
        let schema = vec![("i".into(), 0)];
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
        let schema = vec![("s".into(), 5)];
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

    #[test]
    fn test_pattern_iter() {
        use crate::ast::*;
        use crate::grammar::StmtParser;

        let types = create_type_map();
        let schema = vec![("x".into(), 3), ("y".into(), 3)];

        let mut table = Table::new(schema.clone(), &types);

        let int_val1 = Value::Sum("Int".into(), vec![Value::Integer(1)]);
        let int_val2 = Value::Sum("Int".into(), vec![Value::Integer(2)]);
        let int_val3 = Value::Sum("Int".into(), vec![Value::Integer(3)]);
        let int_none = Value::Sum("Nil".into(), vec![]);

        table.push_row(&[int_val1.clone(), int_val2.clone()], &types);
        table.push_row(&[int_val3.clone(), int_val2.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_val2.clone(), int_val3.clone()], &types);
        table.push_row(&[int_val2.clone(), int_val3.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_val2.clone(), int_val2.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_val2.clone(), int_val2.clone()], &types);
        table.push_row(&[int_val1.clone(), int_none.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);

        // helper function for extracting a pattern match ast from sql input
        let parse_pattern = |input: &str| -> CompiledPattern {
            let stmt = StmtParser::new().parse(input).unwrap();
            match stmt {
                Stmt::Select(Select { items, .. }) => {
                    CompiledPattern::compile(&items, &schema, &types)
                }
                _ => panic!("Not a select statement"),
            }
        };

        // function for parsing sql, extracting a pattern match, and trying to run it.
        let test_pattern = |input: &str, f: Box<dyn FnOnce(RowPatternIter<'_, '_, '_>)>| {
            let pattern = parse_pattern(input);
            println!("testing pattern \"{}\": {:#?}", input, pattern);
            let iter = table.pattern_iter(&pattern, &types);
            f(iter)
        };

        // iterate over all matching rows
        test_pattern("SELECT x: Int(1);", box |i| assert_eq!(i.count(), 2));
        test_pattern("SELECT x: Int(2);", box |i| assert_eq!(i.count(), 4));
        test_pattern("SELECT x: Int(3);", box |i| assert_eq!(i.count(), 1));
        test_pattern("SELECT x: Int(42);", box |i| assert_eq!(i.count(), 0));
        test_pattern("SELECT x: Nil();", box |i| assert_eq!(i.count(), 7));

        // iterate over all bound cells of all matching rows.
        // two rows should match, multiplied by three bindings x1, ý2, x3.
        test_pattern(
            "SELECT x: Int(2), x: x1, y: y2, x: x3, x, y: Int(2);",
            box |i| assert_eq!(i.flatten().count(), 6),
        );
    }
}
