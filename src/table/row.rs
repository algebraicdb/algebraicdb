pub use super::{Cell, Schema};
use crate::types::TypeMap;

#[derive(Clone, Copy)]
pub struct Row<'tb> {
    schema: &'tb Schema,
    pub data: &'tb [u8],
}

#[derive(Clone, Copy)]
pub struct CellIter<'tb, 'ts> {
    schema: &'tb Schema,
    pub data: &'tb [u8],
    type_map: &'ts TypeMap,
    cursor: usize,
    col: usize,
}

impl<'tb, 'ts> Iterator for CellIter<'tb, 'ts> {
    type Item = (&'tb str, Cell<'tb, 'ts>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.col >= self.schema.len() {
            return None;
        }

        let start = self.cursor;
        let (name, t_id) = &self.schema.columns[self.col];
        let t = &self.type_map[t_id];
        let t_size = t.size_of(self.type_map);

        let end = start + t_size;

        let cell = Cell::new(*t_id, &self.data[start..end], self.type_map);

        self.cursor = end;
        self.col += 1;

        Some((name, cell))
    }
}

impl<'tb> Row<'tb> {
    pub fn new(schema: &'tb Schema, data: &'tb [u8]) -> Self {
        Row { schema, data }
    }

    pub fn iter<'ts>(&self, type_map: &'ts TypeMap) -> CellIter<'tb, 'ts> {
        CellIter {
            data: &self.data,
            schema: &self.schema,
            type_map,
            cursor: 0,
            col: 0,
        }
    }

    pub fn get_cell<'ts>(&'tb self, type_map: &'ts TypeMap, col: usize) -> Cell<'tb, 'ts> {
        let mut start = 0;
        for (_, t_id) in &self.schema.columns[..col] {
            let t = &type_map[t_id];
            let t_size = t.size_of(type_map);
            start += t_size;
        }

        let end = start + type_map[&self.schema.columns[col].1].size_of(type_map);

        Cell::new(self.schema.columns[col].1, &self.data[start..end], type_map)
    }

    pub fn cell_count(&self) -> usize {
        self.schema.len()
    }
}
